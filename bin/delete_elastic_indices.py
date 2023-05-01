import argparse
from datetime import datetime, date, timedelta
import re
import sys
import time
import colorlog
import requests
from elasticsearch import Elasticsearch
from tqdm import tqdm

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
POLICY = {}
SERVER = {}
COUNTER = dict.fromkeys(['found', 'dfound', 'dsize', 'deleted', 'ddeleted', 'size'], 0)
NAMES = {}

# -----------------------------------------------------------------------------

def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}P{suffix}"


def terminate_program(msg=None):
    ''' Display stats and terminate program
        Keyword arguments:
          msg: optional message
        Returns:
          None
    '''
    print(f"Indices found: {COUNTER['found']} ({COUNTER['dfound']:,} docs, "
          + f"{humansize(COUNTER['size'])})")
    if ARG.DELETE:
        print(f"Indices deleted: {COUNTER['deleted']} ({COUNTER['ddeleted']:,} docs, " \
              + f"{humansize(COUNTER['dsize'])})")
    else:
        print(f"Indices that would have been deleted: {COUNTER['deleted']} "
              + f"({COUNTER['ddeleted']:,} docs, {humansize(COUNTER['dsize'])})")
    print("Documents per index:")
    for index in NAMES:
        print(f"  {index:36} {NAMES[index]:>13,}")
    if msg:
        LOGGER.error(msg)
    if OUTPUT:
        OUTPUT.close()
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint):
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    global CONFIG, SERVER, POLICY
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']
    data = call_responder('config', 'config/retention_policies')
    POLICY = data['config']


def get_index_date(index):
    # index is expected to end with YYYY.MM.DD or YYYY.MM
    if "aws" in index:
        field = index.split('-', 1)
    else:
        field = index.split('-')
    indexdate = field[-1].replace("-", ".")
    if len(indexdate) == 7:
        # YYYY-MM : add the last day of that month
        yrmo = indexdate.split('.')
        ldom = last_day_of_month(date(int(yrmo[0]), int(yrmo[1]), 1))
        indexdate += '.' + str(ldom.day)
    return datetime.strptime(indexdate, "%Y.%m.%d").date()


def last_day_of_month(indate):
    if indate.month == 12:
        return indate.replace(day=31)
    return indate.replace(month=indate.month+1, day=1) - timedelta(days=1)


def get_index_docs(esearch, index):
    try:
        stats = esearch.indices.stats(index)
    except Exception as err:
        terminate_program(err)
    if index not in stats['indices'] or 'docs' not in stats['indices'][index]['primaries']:
        return None, None
    return stats['indices'][index]['primaries']['docs']['count'], \
           stats['indices'][index]['primaries']['store']['size_in_bytes']


def handle_deletion(use_policy, policies, esearch, index, docs, size):
    if use_policy not in policies:
        policies[use_policy] = 0
    policies[use_policy] += 1
    if ARG.DELETE:
        try:
            esearch.indices.delete(index=index, ignore=[400, 404]) #pylint: disable=unexpected-keyword-arg
        except Exception as err:
            terminate_program(err)
        LOGGER.error("Deleted %s (%s docs, %s) [%s]", index, "{:,}".format(docs),
                     humansize(size), use_policy)
        wait_time = 1 if docs < 200000 else int(docs / 200000)
        time.sleep(wait_time)
    else:
        OUTPUT.write(f"curl -XDELETE flyem-elk.int.janelia.org:9200/{index}\n")
        LOGGER.warning("Would have deleted %s (%s docs, %s) [%s]", index, \
                       "{:,}".format(docs), humansize(size), use_policy)
    COUNTER['deleted'] += 1
    COUNTER['ddeleted'] += docs
    COUNTER['dsize'] += size


def process_indices():
    policies = {}
    today = date.today()
    try:
        esearch = Elasticsearch(SERVER[ARG.ES_SERVER]['address'], timeout=10)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        terminate_program(message)
    print(f"Cluster status: {esearch.cluster.health()['status']}")
    try:
        indices = esearch.indices.get(ARG.INDEX)
    except Exception as err:
        terminate_program(err)
    for index in tqdm(indices):
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
        if not maxdays and ARG.DEBUG:
            LOGGER.error("No policy found for %s", index)
        docs, size = get_index_docs(esearch, index)
        if not docs:
            continue
        if maxdays:
            COUNTER['found'] += 1
            COUNTER['dfound'] += docs
            COUNTER['size'] += size
            idateobj = get_index_date(index)
            delta = (today - idateobj).days
            LOGGER.info("%s (%s docs): %d day(s)", index, "{:,}".format(docs), delta)
            if delta > maxdays:
                handle_deletion(use_policy, policies, esearch, index, docs, size)
        if index.startswith("aws_"):
            dateless = index.split("-")[0]
        else:
            datestamp = index.split("-")[-1]
            dateless = index.replace("-" + datestamp, "")
        if dateless not in NAMES:
            NAMES[dateless] = docs
        else:
            NAMES[dateless] += docs
    if policies:
        print("Policies in use:")
        for policy in sorted(policies):
            print(f"  {policy} ({POLICY[policy]['days']} days): {policies[policy]}")


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
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    OUTPUT = open("delete_elastic.sh", "w", encoding="ascii")
    process_indices()
    terminate_program()
