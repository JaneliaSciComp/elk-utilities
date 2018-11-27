import argparse
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch
import json
import re
import sys
import colorlog
import requests

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}

# -----------------------------------------------------------------------------
def call_responder(server, endpoint):
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        return req.json()
    else:
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)


def initialize_program():
    """ Initialize database """
    global CONFIG
    data = call_responder('config', 'config/servers')
    CONFIG = data['config']


def last_day_of_month(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month+1, day=1) - timedelta(days=1)


def process_indices():
    found = dfound = 0
    deleted = ddeleted = 0
    today = date.today()
    try:
        es = Elasticsearch(CONFIG['elk-elastic']['address'])
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    health = es.cluster.health()
    print("Cluster status:", health['status'])
    for index in es.indices.get('*'):
        if index[0] == '.' or not re.search('-20\d{2}\.\d{2}', index):
            continue
        stats = es.indices.stats(index)
        docs = stats['indices'][index]['primaries']['docs']['count']
        found += 1
        dfound += docs
        field = index.split('-')
        indexdate = field[-1]
        maxdays = 7
        if len(indexdate) == 7:
            # YY-MM : add the last day of that month
            ym = indexdate.split('.')
            ldom = last_day_of_month(date(int(ym[0]), int(ym[1]), 1))
            indexdate += '.' + str(ldom.day)
            maxdays = 60
        idateobj = datetime.strptime(indexdate, "%Y.%m.%d").date()
        delta = (today - idateobj).days
        logger.info("%s (%s docs): %d day(s)", index, "{:,}".format(docs), delta)
        if delta > maxdays:
            deleted += 1
            ddeleted += docs
            if ARG.DELETE:
                es.indices.delete(index=index, ignore=[400, 404])
                logger.error("Deleted %s (%s docs)", index, "{:,}".format(docs))
            else:
                logger.warning("Would have deleted %s (%s docs)", index, "{:,}".format(docs))
    print("Indices found: %d (%s docs)" % (found, "{:,}".format(dfound)))
    print("Indices %sdeleted: %d (%s docs)" % ('' if (ARG.DELETE) else 'that would have been ', deleted, "{:,}".format(ddeleted)))


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
    PARSER.add_argument('--delete', action='store_true',
                        dest='DELETE', default=False,
                        help='Actually delete indices')
    ARG = PARSER.parse_args()

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    process_indices()
    sys.exit(0)
