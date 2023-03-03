import argparse
from datetime import datetime
import sys
from colorama import init, Fore, Back, Style
import colorlog
import requests
from tqdm import tqdm
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
    init(autoreset=True)
    """ Initialize database """
    global CONFIG, SERVER
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']


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
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'P', suffix)


def process_indices():
    counter = {'found': 0, 'docs': 0, 'size': 0}
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
    index_name = dict()
    indices = sorted(response.keys())
    if not ARG.VERBOSE:
        indices = tqdm(indices)
    for idx in indices:
        counter['found'] += 1
        if 'aliases' in response[idx] and response[idx]['aliases']:
            aliases = Fore.WHITE + Style.BRIGHT + Back.RED \
                      + ", ".join(list(response[idx]['aliases'].keys())) \
                      + Style.RESET_ALL
            LOGGER.info('%s (alises: %s)' % (idx, aliases))
        else:
            LOGGER.info(idx)
        base = idx.split("-")[0]
        index_name[base] = {"earliest": None, "docs": 0, "size": 0}
        if ARG.FULL:
            for prop in response[idx]['mappings']['doc']['properties']:
                print('  %s' % (prop))
        settings = response[idx]['settings']['index']
        created = int(settings['creation_date']) / 1000
        timestamp = datetime.fromtimestamp(created).strftime('%Y-%m-%d %H:%M:%S')
        if not index_name[base]["earliest"]:
            index_name[base]["earliest"] = timestamp
        if ARG.VERBOSE:
            print("  Created: %s" % (timestamp))
        if ARG.FULL:
            print("  %s replica%s across %s shard%s" % (settings['number_of_replicas'], \
                's' if int(settings['number_of_replicas']) > 1 else '', \
                settings['number_of_shards'], 's' if int(settings['number_of_shards']) > 1 else ''))
        stats = esearch.indices.stats(idx)
        docs = stats['indices'][idx]['primaries']['docs']['count']
        index_name[base]["docs"] += docs
        size = stats['indices'][idx]['primaries']['store']['size_in_bytes']
        index_name[base]["size"] += size
        if ARG.VERBOSE:
            print(f"  Documents: {docs:,} ({humansize(size)})")
        counter['docs'] += docs
        counter['size'] += size
    print(f"Indices found: {counter['found']} ({counter['docs']:,} docs, {humansize(counter['size'])})")
    for idx in index_name:
        print(idx)
        print(f"  Earliest:  {index_name[idx]['earliest']}")
        print(f"  Socuments: {index_name[idx]['docs']}")
        print(f"  Size:      {humansize(index_name[idx]['size'])}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Show Elastic indices')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='*',
                        help='Index to check [*]')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        default='',
                        help='ES erver to query [flyem-elk.int.janelia.org:9200]')
    PARSER.add_argument('--full', action='store_true',
                        dest='FULL', default=False,
                        help='Show full report (includes fields')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
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
