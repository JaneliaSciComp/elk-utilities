import argparse
from datetime import datetime, date
import re
import sys
import colorlog
import requests
from elasticsearch import Elasticsearch

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
SERVER = {}

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
    logger.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize database """
    global CONFIG, SERVER
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']


def process_indices():
    counter = {'found': 0, 'docs': 0}
    today = date.today()
    try:
        esearch = Elasticsearch(SERVER['elk-elastic']['address'])
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    response = call_responder('elk-elastic', ARG.INDEX)
    for idx in sorted(response):
        counter['found'] += 1
        if 'aliases' in response[idx] and response[idx]['aliases']:
             print('%s (alises: %s)' % (idx, ", ".join(list(response[idx]['aliases'].keys()))))
        else:
            print(idx)
        if (ARG.FULL):
            for prop in response[idx]['mappings']['doc']['properties']:
                print('  %s' % (prop))
        settings = response[idx]['settings']['index']
        created = int(settings['creation_date']) / 1000
        timestamp = datetime.fromtimestamp(created).strftime('%Y-%m-%d %H:%M:%S')
        print("  Created: %s" % (timestamp))
        if ARG.FULL:
            print("  %s replica%s across %s shard%s" % (settings['number_of_replicas'],
                  's' if int(settings['number_of_replicas']) > 1 else '',
                  settings['number_of_shards'], 's' if int(settings['number_of_shards']) > 1 else ''))
        stats = esearch.indices.stats(idx)
        docs = stats['indices'][idx]['primaries']['docs']['count']
        print("  Documents: %s" % ("{:,}".format(docs)))
        counter['docs'] += docs
    print("Indices found: %d (%s docs)" % (counter['found'], "{:,}".format(counter['docs'])))


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Show Elastic indices')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    PARSER.add_argument('--full', action='store_true',
                        dest='FULL', default=False,
                        help='Show full report (includes fields')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='*',
                        help='Index to check [*]')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        default='',
                        help='ES erver to query [flyem-elk.int.janelia.org:9200]')
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

    if ARG.SERVER:
        CONFIG['flyem-elasticsearch']['url'] = 'http://flyem-elk.int.janelia.org:9200/'
        SERVER['flyem-elk']['address'] = 'http://flyem-elk.int.janelia.org:9200'
    else:
        initialize_program()
    process_indices()
    sys.exit(0)
