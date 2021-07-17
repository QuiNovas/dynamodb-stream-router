#!/usr/bin/env python3.8
from dynamodb_stream_router.router import StreamRouter, Record
from dynamodb_stream_router.conditions import (
    HasChanged,
    Old,
    New
)
from time import time


router = StreamRouter(threaded=False)

exp = HasChanged(
    ["source", "target"]) & Old("target").get("foo").eq("bar")
exp = HasChanged(["source", "target"]) & Old("target").get("foo").is_type(str)


@router.update(
    condition_expression=HasChanged(["source", "target"]) & Old("target")["bar"][0].is_type(str) & New("target").get("bazz").as_bool().invert()
)
def edge(item):
    return "CALLED ROUTE!!!!!!!!!"


items = [
    {
        ""
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "eventName": "UPDATE",
        "OldImage": {"type": "Edge", "source": "Node1", "target": {"bar": ["foo"]}},
        "NewImage": {"type": "Edge", "source": "Node1", "target": {"baz": "Node3"}}
    }
]


def handler():
    start = time()
    res = router.resolve_all(items)
    print(time() - start)
    print([
        x.value for x in res
    ])


if __name__ == "__main__":
    handler()
