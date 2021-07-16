#!/usr/bin/env python3.8
from dynamodb_stream_router import StreamRouter
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
print(str(exp))
@router.update(
    condition_expression=HasChanged(["source", "target"]) & Old("target")["bar"][0].is_type(list)
)

def edge(item):
    return "CALLED ROUTE!!!!!!!!!"


items = [
    {
        "operation": "UPDATE",
        "old": {"type": "Edge", "source": "Node1", "target": {"bar": ["foo"]}},
        "new": {"type": "Edge", "source": "Node1", "target": "Node3"}
    }
]

def handler():
    start = time()
    res = router.resolve_all(items)
    print(time() - start)
    print([
        x.value for x in res
    ])

handler()
