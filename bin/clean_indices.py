''' clean_indices.py
    Decrease the amount of storage space by removing the "message" field for
    specified indices.
'''
import argparse
import json
from operator import attrgetter
import socket
import sys
import requests
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught

# -----------------------------------------------------------------------------

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
          None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint, payload=''):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
          payload: payload for POST requests
        Returns:
          JSON response
    '''
    url = attrgetter(f"{server}.url")(REST) + endpoint
    try:
        if payload:
            headers = {"Content-Type": "application/json",
                       "Accept": "application/json",
                       "host": socket.gethostname()}
            req = requests.post(url, headers=headers, json=payload, timeout=5)
        else:
            req = requests.get(url, timeout=10)
        if req.status_code == 200:
            return req.json()
    except requests.exceptions.ReadTimeout:
        return
    except requests.exceptions.RequestException as err:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        print(template.format(type(err).__name__, err.args))
        terminate_program(err)
    except Exception as err:
        template = "An exception exception of type {0} occurred. Arguments:\n{1!r}"
        print(template.format(type(err).__name__, err.args))
        terminate_program(err)
    LOGGER.critical(url)
    LOGGER.critical(req.text)
    terminate_program(f"Status: {str(req.status_code)}")


def process_indices():
    """ Process indices to remove "message" fields
        Keyword arguments:
          None
        Returns:
          None
    """
    indices = call_responder(ARG.ES_SERVER, f"_cat/indices/{ARG.INDEX}*?format=JSON")
    if not indices:
        terminate_program(f"No indices found for {ARG.INDEX}")
    print(f"Indices for {ARG.INDEX}: {len(indices)}")
    docs = updated = to_update = 0
    commands = []
    for idx in sorted(indices, key=lambda x: x['index']):
        LOGGER.info("%s: %s docs (%s)", idx['index'], f"{int(idx['docs.count']):,}",
                    idx['store.size'])
        docs += int(idx['docs.count'])
        payload = {"script" : "ctx._source.remove(\"message\")",
                   "query" : {"exists": { "field": "message" }}
                  }
        search_payload = {"query": {"term": {"client": "screen_review"}}}
        search_payload = {"query" : {"exists": { "field": "message" }}}
        resp = call_responder(ARG.ES_SERVER, f"{idx['index']}/_search?pretty", search_payload)
        LOGGER.debug(json.dumps(resp['hits'], indent=2))
        if resp['hits']['total']:
            if isinstance(resp['hits']['total'], int):
                to_update += resp['hits']['total']
            else:
                to_update += len(resp['hits'])
            commands.append([idx['index'], payload])
    print(f"Found {docs:,} documents in total")
    print(f"Found {to_update:,} documents to be updated")
    if ARG.WRITE:
        for cmd in commands:
            resp = call_responder(ARG.ES_SERVER, f"{cmd[0]}/" \
                                  + "_update_by_query?conflicts=proceed", cmd[1])
            if resp:
                updated += resp['updated']
            else:
                LOGGER.warning("Cleanup of %s has returned a timeout error, but is still running",
                               cmd[0])
    print(f"Updated {updated:,} documents")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Elatic indices")
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='aws_', help='Index basename')
    PARSER.add_argument('--server', dest='ES_SERVER', action='store',
                        default='elk-elastic',
                        help='Index to check [*]')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually clean indices')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        REST = JRC.get_config("rest_services")
    except Exception as gerr: # pylint: disable=broad-exception-caught
        terminate_program(gerr)
    process_indices()
