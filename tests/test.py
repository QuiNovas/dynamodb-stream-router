#!/usr/bin/env python3.8
from dynamodb_stream_router.router import StreamRouter, Record
from dynamodb_stream_router.conditions import (
    HasChanged,
    Old,
    New,
    IsType
)
from time import time


router = StreamRouter(threaded=False)

items = [
    {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "eventName": "UPDATE",
        "dynamodb": {
            "OldImage": {
                "type": {"B": "Edge"}
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
exp = IsType(Old("type"), "B")

test = exp(items[0])  # Returns a bool by testing exp against a single record


@router.update(condition_expression=exp)
def edge(item):
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
