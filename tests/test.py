#!/usr/bin/env python3.8
from dynamodb_stream_router.router import StreamRouter, StreamRecord

from time import time


router = StreamRouter()


# flake8: noqa
records = [{
    'eventID': 'cc7afaec1a119f8e7accf2fd46a83fa5',
  		'eventName': 'MODIFY',
  		'eventVersion': '1.1',
  		'eventSource': 'aws:dynamodb',
  		'awsRegion': 'us-east-1',
  		'dynamodb': {
                    'ApproximateCreationDateTime': 1627664656.0,
                 			'Keys': {
                                            'sk': {
                                                'S': 'TENANT~Mathew Refactor'
                                            },
                                            'pk': {
                                                'S': 'SYSTEM'
                                            }
                                        },
                    'NewImage': {
                                            'system': {
                                                'BOOL': True
                                            },
                                            'created': {
                                                'M': {
                                                    'at': {
                                                        'S': '2021-06-18T18:32:45.220064'
                                                    },
                                                    'by': {
                                                        'S': 'mmoon@quinovas.com'
                                                    }
                                                }
                                            },
                                            'name': {
                                                'S': 'Mathew Refactor'
                                            },
                                            'sk': {
                                                'S': 'TENANT~Mathew Refactor'
                                            },
                                            'lastModified': {
                                                'M': {
                                                    'at': {
                                                        'S': '2021-06-18T18:32:45.220064'
                                                    },
                                                    'by': {
                                                        'S': 'mmoon@quinovas.com'
                                                    }
                                                }
                                            },
                                            'pk': {
                                                'S': 'SYSTEM'
                                            },
                                            'region': {
                                                'S': 'us-east-1'
                                            }
                                        },
                    'OldImage': {
                                            'system': {
                                                'BOOL': True
                                            },
                                            'test': {
                                                'S': 'bar'
                                            },
                                            'created': {
                                                'M': {
                                                    'at': {
                                                        'S': '2021-06-18T18:32:45.220064'
                                                    },
                                                    'by': {
                                                        'S': 'mmoon@quinovas.com'
                                                    }
                                                }
                                            },
                                            'name': {
                                                'S': 'Mathew Refactor'
                                            },
                                            'sk': {
                                                'S': 'TENANT~Mathew Refactor'
                                            },
                                            'lastModified': {
                                                'M': {
                                                    'at': {
                                                        'S': '2021-06-18T18:32:45.220064'
                                                    },
                                                    'by': {
                                                        'S': 'mmoon@quinovas.com'
                                                    }
                                                }
                                            },
                                            'pk': {
                                                'S': 'SYSTEM'
                                            },
                                            'region': {
                                                'S': 'us-east-1'
                                            }
                                        },
                    'SequenceNumber': '741245700000000103331120494',
                 			'SizeBytes': 435,
                 			'StreamViewType': 'NEW_AND_OLD_IMAGES'
		},
    'eventSourceARN': 'arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582'
}]


record = StreamRecord(records[0])

@router.update(condition_expression="NOT (attribute_exists($NEW.systemd) | attribute_exists($OLD.systemd))")
def delete_tenant(record):
    print(record._asdict())


def handler():
    start = time()
    res = router.resolve_all([StreamRecord(x)._asdict() for x in records])
    print(time() - start)
    print([
        x.value for x in res
    ])


if __name__ == "__main__":
    handler()
