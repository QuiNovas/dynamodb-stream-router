#!/usr/bin/env python3.8
from dynamodb_stream_router.router import StreamRouter, StreamRecord, ExceptionHandler
from json_deserializer import loads
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


records = {
    "Records": [
        {
            "messageId": "ad29ce8e-2b7f-4223-964b-57588e7ec6f2",
            "receiptHandle": "AQEB6Rkx24Pwqro340ZYXTLgK/TOo+qRgCK6tBJyHLCUgKNOMejF5UHzmQe4uLLdu5Bz+UYo5GyxvHhvCfVLWOKai3nrHbn2TvGtn4OYkJ3GV5VmIy/aP5hAW9JRhTx3tEHJMuTgDPVylsQrXclOfoHyUrP4Uz3Tsx5gHiRColUz9EL1a15eGI4o3Z9ayG2Dj1lLH4WI/ENfUqnG6JdXNAWg5zlgkD0NXksYMTZtI7BVsZ3ZYSJ7PUQnvcR+Uo4sFxyV5eIdpMrkhQ1P6fcV/a1fASH5SDitji5LSRp3Ew+/Z9Ts9XFt+dzG+FXiPqwZFh0g",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"5fb8e903411842f1cb7fe28bb9933567\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775037700000000056045687957\", 812, {\"eventID\": \"5fb8e903411842f1cb7fe28bb9933567\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775037700000000056045687957\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "5",
                "SentTimestamp": "1628275216433",
                "SequenceNumber": "18863582529116399616",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "e4ff91ce-cf11-4bf9-80e9-078758cd6ac7",
                "ApproximateFirstReceiveTimestamp": "1628275216433"
            },
            "messageAttributes": {},
            "md5OfBody": "02885aba1e2b408c9653a7e074567ed5",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        },
        {
            "messageId": "8d890041-aa67-41e6-bc75-8457d329555f",
            "receiptHandle": "AQEBeQBBXKlybKvcUzYppXlIi3x2Q5VTeMdQ5uyJxu7CDqi8KrTJG7CYeWvkJpbtZJNlkc5idouV6vr26MrfTMByrWxBkN+JkKDU5D68LQm/Fb2waDgLTbZq71B/9jMrlqDIv8qy6uIj4+fT9HolbTyBsL0OILNU9bsx45GYwWpZOh8k4DfDYVe/RHc8o4oGnjtnVZJVnt5kj6o0EvNpVgYeqnUjJpaNG4hmNZCj9bntb5CcH62YANrRTbwhBT47/kTuDOdnWN/xfNu563hZYXd/dUNpz1we5PcNQjE39+xiguuDRk0mJF6IFcyCwkdeBCVZ",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"214c91448f5da230097b4a3263bdd2b3\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775037800000000056046198354\", 812, {\"eventID\": \"214c91448f5da230097b4a3263bdd2b3\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775037800000000056046198354\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "2",
                "SentTimestamp": "1628275987870",
                "SequenceNumber": "18863582726604274176",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "9298ba79-cd76-41c1-b7b3-2e589de2aa04",
                "ApproximateFirstReceiveTimestamp": "1628276116463"
            },
            "messageAttributes": {},
            "md5OfBody": "442222beceb4c31fe7496a51409931fb",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        },
        {
            "messageId": "72d45dd0-7c53-40e6-9ee0-d157c05b1f0e",
            "receiptHandle": "AQEBdJOBqoDGUxRPOr0c19Re8Hc/7WAwCwESgK5vnGnoiQYdXf1dfOoqkXPKs3FUFn5U7eI6UNszHoubNrEcrqhV+ln3fThwVbMoGICPeF3QyN4CDWXOepNoa0m2RV7XUIOyczwBSWCja/7LBsnl54ahxV4fbaoVhpTtrYBCpvUe+YB8cHsmMkqi83SwWlci4vaUZQrQ8TCKf/X+IOHijxru1LPoJY6B9YtDSLvddEfVVme0xd7SfdmCvQ2cDyhK5rs/cB/3JnZEVQWHgegBgB1JA/S/z5xbvrw1sKYRsGB+N6N5KQWGKZ1x3x2+D+swYPPj",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"f2fdbcdb083c44375c757d9e70671a3e\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775037900000000056046246591\", 812, {\"eventID\": \"f2fdbcdb083c44375c757d9e70671a3e\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775037900000000056046246591\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "2",
                "SentTimestamp": "1628276061466",
                "SequenceNumber": "18863582745444847872",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "237d3ce3-01be-45b8-bf81-1abd6eb58075",
                "ApproximateFirstReceiveTimestamp": "1628276116463"
            },
            "messageAttributes": {},
            "md5OfBody": "8666e9b67129c0c5a4709a3ade7f25c0",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        },
        {
            "messageId": "737d8af6-8d83-4b7d-b334-f1b37217f937",
            "receiptHandle": "AQEBBCBCBLS10Jj68oyE/njWVuUtBZqI05xQYNzcvOL2H5e6cr5oBO8ksnvhvXQLeoirTshfW8ImgpE1qW2pRZOJAuCH69t14G0y7z9r6bZFtGFtNxKxjoFNKE+nFjJG2JfkcRF+GlfaU5G5VMQZY//ZyZ0ZVDogSVpyS1gGkfhazyRSkMnZXiud9N9bg5WZbLUUcjHoozwYLuZ/OLndE2Kv+NChPiDe6eDj4OmnU+4CU9PVR7OgwN1ShxpoqgKRDFWGaL0MX5asHSHWMFIA6o5umm9noD2x0xrf7LoES6bOHwPIN8BiQ5LvWpfUmrKJKnOD",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"d09fb2077372599f3ca4fd40294183bb\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775038000000000056046266521\", 812, {\"eventID\": \"d09fb2077372599f3ca4fd40294183bb\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775038000000000056046266521\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "2",
                "SentTimestamp": "1628276093081",
                "SequenceNumber": "18863582753538288640",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "3003108a-73e5-494f-a16e-671c8ffe0799",
                "ApproximateFirstReceiveTimestamp": "1628276116463"
            },
            "messageAttributes": {},
            "md5OfBody": "ba9ca875546d76dc74a8dceb43aaee2a",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        },
        {
            "messageId": "f8501ada-7e76-465c-b965-1a439d8b16da",
            "receiptHandle": "AQEBVgy7/c1b2CeI7tNP5s0WDtkrMm7JjMGhoJaCSKYR6pMeQ9B6i76L95/zqlv4TNLN+s7wiMkl8CZyHaui6g1d2gDgsRBH7UOW/Bq/QLjc3TXS+fOyrm+D9UzK3YCQW91JqPmiA76uCLQjUu9DN3tYUBb2ZxqYwpj8TuORK2tbFRm3Agv+gt1+NZeJ2JgopH87nrnZd9E/Whh3rA5J8LbxVLOeGyZ2HDH4gbEfNz0qqvEz1myUP7B36Zmz/9tPy8Dv0bJX7RBFuXoymUFqbpItRxI0Xe6mJZM1ZsBHury9iOKYU0r+FmddUSBsqRTBn/Nf",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"ad4f15750807034af7483ef6e99370fe\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775038100000000056046376253\", 812, {\"eventID\": \"ad4f15750807034af7483ef6e99370fe\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775038100000000056046376253\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1628276259786",
                "SequenceNumber": "18863582796214767616",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "8004126f-1c47-4b1a-8d5e-8e590c00ef27",
                "ApproximateFirstReceiveTimestamp": "1628276416479"
            },
            "messageAttributes": {},
            "md5OfBody": "47296bf36fff84f7c0266d576f22a3d5",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        },
        {
            "messageId": "59306d86-94dc-4925-ba1a-514eaa54ee74",
            "receiptHandle": "AQEBf/LKSXXqQUnD759H6akpoJvU9gOewQiOZUFQo3TSi2lR0S76u7Ips48pLk0TN7XOCD092jH25ZX12pV8Qg8I3VM6im/zkvWX1Mpao+dTV8OetqFXxFweb3IHoDVM7FAp1Binsaw5pvnwm+0ETbMMPni8sQNpgFwglioHUScUyLIrRAqySLvT90SVpAJGlmShOfU2j346p+ukut/iWya8lkzJUIgSH8QBvxtTX24ADxKMSKSLy6llIeliDbVfA7xEjlpoDDXFOTXamNy2WjUu+LcnZU0Cj2hoEHIt0oODAvXGYLSLKUiCaYGcWoDlp30g",
            "body": r"[\"MODIFY\", \"NEW_AND_OLD_IMAGES\", \"us-east-1\", \"7f70ed1a007a637b3f9710adb495b9b6\", \"aws:dynamodb\", \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"1.1\", {}, {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"775038200000000056046450998\", 812, {\"eventID\": \"7f70ed1a007a637b3f9710adb495b9b6\", \"eventName\": \"MODIFY\", \"eventVersion\": \"1.1\", \"eventSource\": \"aws:dynamodb\", \"awsRegion\": \"us-east-1\", \"eventSourceARN\": \"arn:aws:dynamodb:us-east-1:066817783078:table/echo-dev-graph/stream/2021-02-25T00:29:50.582\", \"NewImage\": {\"system\": true, \"removed\": true, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"OldImage\": {\"system\": true, \"removed\": false, \"dbStreamQueue\": \"https://queue.amazonaws.com/066817783078/db-streamddd88ceb7e7042168a4090e584178df9.fifo\", \"created\": {\"at\": \"2021-08-05T20:03:17.539894\", \"by\": \"mmoon@quinovas.com\"}, \"dbStreamEventSourceMappingId\": \"bddc6d75-5ea6-496c-a506-e49c19991a17\", \"name\": \"Mathew - test account 2\", \"sk\": \"TENANT~Mathew - test account 2\", \"lastModified\": {\"at\": \"2021-08-05T20:03:17.579324\", \"by\": \"rollback\"}, \"pk\": \"SYSTEM\", \"region\": \"us-east-1\", \"type\": \"Tenant\"}, \"StreamViewType\": \"NEW_AND_OLD_IMAGES\", \"SequenceNumber\": \"775038200000000056046450998\", \"SizeBytes\": 812}]",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1628276374915",
                "SequenceNumber": "18863582825687791616",
                "MessageGroupId": "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073",
                "SenderId": "AROAQ7DVGOUTN7BTD7NEG:echo-dev-graph-table-dynamodb-trigger",
                "MessageDeduplicationId": "20a8effa-786b-4632-940e-29965b17f4ef",
                "ApproximateFirstReceiveTimestamp": "1628276416479"
            },
            "messageAttributes": {},
            "md5OfBody": "ab47cd48172860db8aa2da0808089ab8",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-1:066817783078:echo-dev_db-stream_DEFAULT_TENANT.fifo",
            "awsRegion": "us-east-1"
        }
    ]
}

records = [
    loads(x) for x in records
]

def error_handler(record, e):
    print(e)

router.exception_handler = ExceptionHandler(handler=error_handler, exceptions=(Exception))

#record = StreamRecord(records])

@router.modify(condition_expression="NOT (attribute_exists($NEW.systemd) | attribute_exists($OLD.systemd))")
def delete_tenant(record):
    print("called")
    #raise Exception("Exception from route handler")
    #print(record._asdict())


def handler():
    start = time()
    res = router.resolve_all([StreamRecord(x)._asdict() for x in records])
    for x in res:
        if x.error:
            raise x.error
    print(time() - start)
    print(res)


if __name__ == "__main__":
    handler()
