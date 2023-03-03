import argparse
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


def process_indices():
    counter = {'found': 0, 'dfound': 0}
    try:
        esearch = Elasticsearch(SERVER['elk-elastic']['address'])
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        sys.exit(-1)
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    for index in esearch.indices.get('*'):
        stats = esearch.indices.stats(index)
        docs = stats['indices'][index]['primaries']['docs']['count']
        counter['found'] += 1
        counter['dfound'] += docs
        LOGGER.info("%s (%s docs)", index, "{:,}".format(docs))
    print("Indices found: %d (%s docs)" % (counter['found'], "{:,}".format(counter['dfound'])))


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Count documents in Elastic indices')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        default='',
                        help='ES erver to query [flyem-elk.int.janelia.org:9200]')
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

    if ARG.SERVER:
        SERVER['elk-elastic'] = {'address': 'http://' + ARG.SERVER + ':9200'}
        CONFIG['elk-elastic'] = {'url': SERVER['elk-elastic']['address'] + '/'}
    else:
        initialize_program()
    process_indices()
    sys.exit(0)
