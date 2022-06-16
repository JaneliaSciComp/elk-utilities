import argparse
from datetime import datetime, date, timedelta
import re
import sys
import time
import colorlog
import requests
from elasticsearch import Elasticsearch

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
POLICY = {}
SERVER = {}

# -----------------------------------------------------------------------------
def call_responder(server, endpoint):
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize database """
    global CONFIG, SERVER, POLICY
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']
    data = call_responder('config', 'config/retention_policies')
    POLICY = data['config']


def get_index_date(index):
    # index is expected to end with YYYY.MM.DD or YYYY.MM
    field = index.split('-')
    indexdate = field[-1]
    if len(indexdate) == 7:
        # YY-MM : add the last day of that month
        yrmo = indexdate.split('.')
        ldom = last_day_of_month(date(int(yrmo[0]), int(yrmo[1]), 1))
        indexdate += '.' + str(ldom.day)
    return datetime.strptime(indexdate, "%Y.%m.%d").date()


def last_day_of_month(indate):
    if indate.month == 12:
        return indate.replace(day=31)
    return indate.replace(month=indate.month+1, day=1) - timedelta(days=1)


def process_indices():
    counter = {'found': 0, 'dfound': 0, 'deleted': 0, 'ddeleted': 0}
    policies = dict()
    today = date.today()
    try:
        esearch = Elasticsearch(SERVER[ARG.ES_SERVER]['address'])
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    #print("Cluster status:", esearch.cluster.health()['status'])
    for index in esearch.indices.get(ARG.INDEX):
        use_policy = ''
        if index[0] == '.':
            continue
        maxdays = 0
        for policy in POLICY:
            if re.search(POLICY[policy]['term'], index):
                maxdays = POLICY[policy]['days']
                LOGGER.debug("%s met %s policy (%d days)", index, policy, maxdays)
                use_policy = policy
                break
        if not maxdays:
            continue
        stats = esearch.indices.stats(index)
        if index not in stats['indices'] or 'docs' not in stats['indices'][index]['primaries']:
            continue
        docs = stats['indices'][index]['primaries']['docs']['count']
        counter['found'] += 1
        counter['dfound'] += docs
        idateobj = get_index_date(index)
        delta = (today - idateobj).days
        LOGGER.info("%s (%s docs): %d day(s)", index, "{:,}".format(docs), delta)
        if delta > maxdays:
            counter['deleted'] += 1
            counter['ddeleted'] += docs
            if use_policy not in policies:
                policies[use_policy] = 0
            policies[use_policy] += 1
            if ARG.DELETE:
                esearch.indices.delete(index=index, ignore=[400, 404]) #pylint: disable=unexpected-keyword-arg
                LOGGER.error("Deleted %s (%s docs) [%s]", index, "{:,}".format(docs), use_policy)
                wait_time = 1 if docs < 200000 else int(docs / 200000)
                time.sleep(wait_time)
            else:
                LOGGER.warning("Would have deleted %s (%s docs) [%s]", index, \
                    "{:,}".format(docs), use_policy)
    print("Indices found: %d (%s docs)" % (counter['found'], "{:,}".format(counter['dfound'])))
    print("Indices %sdeleted: %d (%s docs)" % ('' if (ARG.DELETE) else \
        'that would have been ', counter['deleted'], "{:,}".format(counter['ddeleted'])))
    if len(policies):
        print("Policies in use:")
        for policy in sorted(policies):
            print("  %s (%d days): %d" % (policy, POLICY[policy]['days'], policies[policy]))


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Delete aged Elastic indices')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    PARSER.add_argument('--server', dest='ES_SERVER', action='store',
                        default='flyem-elastic',
                        help='Index to check [*]')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='*',
                        help='Index to check [*]')
    PARSER.add_argument('--delete', action='store_true',
                        dest='DELETE', default=False,
                        help='Actually delete indices')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    process_indices()
    sys.exit(0)
