import argparse
import sys
import colorlog
import pprint
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
        esearch = Elasticsearch(SERVER['elk-elastic']['address'])
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    if index not in esearch.indices.get('*'):
        index += '*'
    result = esearch.search(index=index, body={"size": 1, "sort": {"@timestamp": "desc"}})
    print("Index: " + result['hits']['hits'][0]['_index'])
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(result['hits']['hits'][0]['_source'])


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Count documents in Elastic indices')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='nptest', help='Index to fetch latest record from')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
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
    process_index(ARG.INDEX)
    sys.exit(0)
