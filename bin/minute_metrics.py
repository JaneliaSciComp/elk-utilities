import argparse
import sys
import colorlog
import json
import requests
import time
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
SERVER = {}
WRITE_TOPIC = 'dvid_activity_minute_metrics'

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
    # Show health
    health = esearch.cluster.health()
    print("Cluster status:", health['status'])
    if index not in esearch.indices.get('*'):
        index += '*'
    # Get last minute metrics
    payload = {
  "aggs": {
    "2": {
      "terms": {
        "field": "user.keyword",
        "size": 100,
        "order": {
          "_count": "desc"
        }
      },
      "aggs": {
        "3": {
          "significant_terms": {
            "field": "client.keyword",
            "size": 100
          },
          "aggs": {
            "4": {
              "significant_terms": {
                "field": "method.keyword",
                "size": 5
              },
              "aggs": {
                "5": {
                  "significant_terms": {
                    "field": "server.keyword",
                    "size": 10
                  },
                  "aggs": {
                    "6": {
                      "significant_terms": {
                        "field": "port.keyword",
                        "size": 100
                      },
                      "aggs": {
                        "7": {
                          "max": {
                            "field": "duration"
                          }
                        },
                        "8": {
                          "min": {
                            "field": "duration"
                          }
                        },
                        "9": {
                          "avg": {
                            "field": "duration"
                          }
                        },
                        "10": {
                          "sum": {
                            "field": "bytes_in"
                          }
                        },
                        "11": {
                          "sum": {
                            "field": "bytes_out"
                          }
                        },
                        "12": {
                          "percentiles": {
                            "field": "duration",
                            "percents": [
                              99
                            ],
                            "keyed": False
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  },
  "size": 0,
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "@timestamp",
      "format": "date_time"
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte": "now-1m",
            }
          }
        }
      ],
      "filter": [],
      "should": [],
      "must_not": []
    }
  }
}
    epoch_seconds = time.time()
    producer = KafkaProducer(bootstrap_servers=SERVER['Kafka']['broker_list'])
    result = esearch.search(index=index, body=payload)
    for user in result['aggregations']['2']['buckets']:
        for app in user['3']['buckets']:
            for method in app['4']['buckets']:
                for server in method['5']['buckets']:
                    count = server['6']['doc_count']
                    for port in server['6']['buckets']:
                        duration_99 = port['12']['values'][0]['value']
                        payload = {'time': epoch_seconds,
                                   'user': user['key'], 'app': app['key'],
                                   'method': method['key'],
                                   'server': ':'.join([server['key'],
                                                       port['key']]),
                                   'count': count,
                                   'min_duration': '%.2f' % port['8']['value'],
                                   'max_duration': '%.2f' % port['7']['value'],
                                   'avg_duration': '%.2f' % port['9']['value'],
                                   'percentile_99_duration': '%.2f' % duration_99,
                                   'bytes_in': '%d' % port['10']['value'],
                                   'bytes_out': '%d' % port['11']['value']}
                        logger.info(payload)
                        future = producer.send(WRITE_TOPIC,
                                               json.dumps(payload))
                        try:
                            record_metadata = future.get(timeout=10)
                        except KafkaError:
                            print("Failed writing to " + WRITE_TOPIC)
                            sys.exit(-1)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Count documents in Elastic indices')
    PARSER.add_argument('--index', dest='INDEX', action='store',
                        default='emdata*_dvid_activity-*',
                        help='Index to fetch latest record from')
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
