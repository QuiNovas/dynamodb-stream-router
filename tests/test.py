#!/usr/bin/env python3.8
from dynamodb_stream_router.router import StreamRouter, StreamRecord, parse_image

from dynamodb_stream_router.conditions import (
    HasChanged,
    Old,
    New,
    IsType
)
from time import time


router = StreamRouter()

items = [
    {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "eventName": "UPDATE",
        "dynamodb": {
            "OldImage": {
                "type": {
                    "M": {
                        "foo": {
                            "M": {
                                "bar": {
                                    "L": [
                                        {"S": "baz"}
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "NewImage": {
                "type": {"S": "Edge"}
            }
        }
    }
]
# exp = HasChanged(["source", "target"]) & Old("target").get("foo").eq("bar")
# exp = HasChanged(["source", "target"]) & Old("target").get("foo").is_type(str)
# exp = HasChanged(["source", "target"]) & Old("target")["bar"][0].is_type(str) & New("target").get("bazz").as_bool().invert()

exp = IsType(Old("type")["foo"], "M") & Old("type").foo.bar[0].eq("baz")
print(str(exp))
record = StreamRecord(items[0])
test = exp(record)  # Returns a bool by testing exp against a single record
print(test)


@router.update(condition_expression=exp)
def edge(item):
    return item


@router.update(condition_expression=exp)
def edge2(item):
    return item


def handler():
    start = time()
    res = router.resolve_all(items)
    print(time() - start)
    print([
        x.value for x in res
    ])


if __name__ == "__main__":
    handler()
