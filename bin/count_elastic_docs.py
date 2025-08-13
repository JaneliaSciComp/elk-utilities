''' count_elastic_docs.py
    Count the number of Elastic docs in all indices
'''

import argparse
import os
import sys
from elasticsearch import Elasticsearch
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG = LOGGER = None
# -----------------------------------------------------------------------------

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def process_indices():
    ''' Process indices
        Keyword arguments:
          None
        Returns:
          None
        '''
    found = dfound = 0
    if "ELK_PASS" not in os.environ:
        terminate_program("Missing password - set in ELK_PASS environment variable")
    if ARG.SERVER:
        url = ARG.SERVER
    else:
        try:
            server = JRC.simplenamespace_to_dict(JRC.get_config("servers"))
        except Exception as err:
            terminate_program(err)
        url = server['metrics-elastic']['address']
    try:
        esearch = Elasticsearch(url, basic_auth=('elastic', os.environ.get('ELK_PASS')))
    except Exception as err:
        terminate_program(err)
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    for idx in esearch.indices.get(index='*'):
        stats = esearch.indices.stats(index=idx)
        docs = stats['indices'][idx]['primaries']['docs']['count']
        found += 1
        dfound += docs
        LOGGER.info(f"{idx} ({docs:,} docs)")
    print(f"Indices found: {found:,} ({dfound:,} docs)")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Count documents in Elastic indices')
    PARSER.add_argument('--server', dest='SERVER', action='store',
                        default='', help='ES erver to query')
    PARSER.add_argument('--verbose', action='store_true',
                        dest='VERBOSE', default=False,
                        help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true',
                        dest='DEBUG', default=False,
                        help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    process_indices()
    terminate_program()
