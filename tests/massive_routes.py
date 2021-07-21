#!/usr/bin/env python3.8
from copy import deepcopy
from dynamodb_stream_router.router import StreamRouter, StreamRecord, parse_image
from dynamodb_stream_router.conditions import (
    HasChanged,
    Old,
    New,
    IsType
)
from time import time
from uuid import uuid4


router = StreamRouter(threaded=True)

item = {
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

items = [deepcopy(item) for _ in range(25)]
# exp = HasChanged(["types", "source", "target"]) & New().type.eq("Edge")
# exp = HasChanged(["source", "target"]) & Old("target").get("foo").is_type(str)
# exp = HasChanged(["source", "target"]) & Old("target")["bar"][0].is_type(str) & New("target").get("bazz").as_bool().invert()

exp = IsType(Old("type")["foo"], "M") & Old("type").foo.bar[0].eq("baz") & HasChanged(["foo", "bar", "baz", "asd", "hjasdf"]) & Old().type.eq("hello") | HasChanged(["type"])
record = items[0]
r = StreamRecord(record)
print(exp(r))

func_str = """
@router.update(condition_expression=exp)
def func_name(item):
    return 1


"""

for _ in range(1000):
    exec(func_str.replace("func_name", "a" + str(uuid4()).replace("-", "_")))


def handler():
    start = time()
    res = router.resolve_all(items)
    print(time() - start)
    # print([
    #     x.value for x in res
    # ])
    return res


if __name__ == "__main__":
    handler()
    handler()
