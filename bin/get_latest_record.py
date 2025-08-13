import argparse
import json
import sys
import os
import requests
from elasticsearch import Elasticsearch
import jrc_common.jrc_common as JRC

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
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
    global CONFIG, SERVER
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']


def process_index(index):
    try:
        esearch = Elasticsearch(SERVER['metrics-elastic']['address'],
                                basic_auth=('elastic', os.environ.get('ELK_PASS')))
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    # Show health
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    if index not in esearch.indices.get(index='*'):
        index += '*'
    # Show transactions in last minute
    result = esearch.search(index=index, body={"query": {"range": \
        {"@timestamp": {"gte": "now-1m"}}}})
    print("Transactions in the last minute: %d"  % (result['hits']['total']['value']))
    # Show last record in index
    result = esearch.search(index=index, body={"size": 1, "sort": {"@timestamp": "desc"}})
    print("Index: " + result['hits']['hits'][0]['_index'])
    print(json.dumps(result['hits']['hits'][0]['_source'], indent=4, default=str))


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Count documents in Elastic indices')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='emdata*_dvid_activity-*', help='Index to fetch latest record from')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        default='',
                        help='ES erver to query [metrics-elk.int.janelia.org:9200]')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)

    if ARG.SERVER:
        SERVER['metrics-elastic'] = {'address': 'http://' + ARG.SERVER + ':9200'}
        CONFIG['metrics-elastic'] = {'url': SERVER['metrics-elastic']['address'] + '/'}
    else:
        initialize_program()
    process_index(ARG.INDEX)
    sys.exit(0)
