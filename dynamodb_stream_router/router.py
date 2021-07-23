#!/usr/bin/env python3.8
from concurrent.futures import ThreadPoolExecutor
import typeguard
from os import environ
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    NamedTuple,
    Union,
    Any,
)
from .conditions.parser import Expression


if not environ.get("TYPECHECKED"):
    typeguard.typechecked = lambda: True


class Image(Enum):
    OldImage = auto()
    NewImage = auto()


class Operations(Enum):
    REMOVE = auto()
    INSERT = auto()
    UPDATE = auto()


class StreamViewType(Enum):
    NEW_AND_OLD_IMAGES = auto()


class Route(NamedTuple):

    #: The Callable that will be triggered if this route is a match for a record
    callable: Callable
    #: The operations that this route is registered for (UPDATE | INSERT | DELETE)
    operations: List[Operations]
    #: Optional dynamodb_stream_router.conditions.Expression or string that can be cast to Expression to decide whether this route should be called on a record
    condition_expression: Union[Callable, str] = None


__STREAM_RECORD_TYPES = (
    ("eventName", Operations),
    ("StreamViewType", StreamViewType),
    ("awsRegion", str),
    ("eventID", str),
    ("eventSource", str),
    ("eventSourceARN", str),
    ("eventVersion", str),
    ("Keys", dict),
    ("NewImage", dict),
    ("OldImage", dict),
    ("SequenceNumber", str),
    ("SizeBytes", int)

)


class StreamRecord(
    NamedTuple(
        "StreamRecord",
        __STREAM_RECORD_TYPES
    )
):
    def __new__(cls, record):
        if "dynamodb" in record:
            for k in [
                "NewImage",
                "OldImage",
                "StreamViewType",
                "SequenceNumber",
                "SizeBytes",
            ]:

                if k in record["dynamodb"]:
                    if k in ("OldImage", "NewImage"):
                        record[k] = parse_image(record["dynamodb"][k])
                    else:
                        record[k] = record["dynamodb"][k]

            del record["dynamodb"]

        defaults = {
            "awsRegion": "",
            "eventID": "",
            "eventSource": "aws:dynamodb",
            "eventSourceARN": "",
            "eventVersion": "",
            "Keys": {},
            "OldImage": {},
            "NewImage": {},
            "SequenceNumber": "",
            "SizeBytes": 0
        }

        record = {
            **defaults,
            **record
        }

        try:
            record["eventName"] = Operations[record.get("eventName")].name
        except KeyError:
            raise TypeError(f"Unknown eventName {record['eventName']}'")

        return super().__new__(cls, **record)


class Result(NamedTuple):
    #: The complete ``Route`` object that generated this result
    route: Route
    #: The original stream record passed to the Route's callable
    record: dict
    #: The return value of the callable for the Route that was called
    value: Any


class RouteSet(NamedTuple):
    REMOVE: List[Route]
    INSERT: List[Route]
    UPDATE: List[Route]


class StreamRouter:
    __instance = None
    __threads = 0
    __threaded = False
    __executor = None

    def __new__(cls, *args, threads: int = None, threaded: bool = False, **kwargs):
        if not threaded and threads is not None:
            raise AttributeError(
                "Argument 'threads' doesn't make sense if 'threaded=False'"
            )

        if threads == 0:
            raise AttributeError("Number of threads must be > 0")

        if cls.__instance is None:
            cls.__threads = threads or 0
            cls.__threaded = threaded or bool(threads)
            if threads:
                cls.__executor = ThreadPoolExecutor(max_workers=threads)
            elif threaded:
                cls.__executor = ThreadPoolExecutor()

            cls.__instance = super().__new__(cls, *args, **kwargs)

        return cls.__instance

    def __init__(self, *args, **kwargs):
        """
        Provides routing of Dynamodb Stream records to Callables based on record content and/or truthy functions
        that may inspect the record

        :Keyword Arguments:
            * *threaded:* (``bool`): If True then each record will be processed in a separate thread using ThreadPoolExecutor
        """

        #: A list of dynamodb_stream_router.Route that are registered to the router
        self.routes: RouteSet = RouteSet(**{"REMOVE": [], "INSERT": [], "UPDATE": []})
        self.format_record = True
        self._condition_parser = Expression()

    def update(self, **kwargs) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "UPDATE" operation, taking the same arguments
        """
        return self.route("UPDATE", **kwargs)

    def remove(self, **kwargs) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "REMOVE" operation, taking the same arguments
        """
        return self.route("REMOVE", **kwargs)

    def insert(self, **kwargs) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "INSERT" operation, taking the same arguments
        """
        return self.route("INSERT", **kwargs)

    def route(
        self,
        operations: Union[str, List[str]],
        condition_expression: Union[Callable, str] = None,
    ) -> Callable:

        """
        Used as a decorator to register a route. Accepts keyword arguments that determine under what conditions the route will
        be called. If no condition_expression is provided then the route will be called for any operations that are
        passed.

        :Keyword Arguments:
            * *operations:* (``Union[str, List[str]``): A Dynamodb operation or list of operations. Can be one or
              more of 'REMOVE | INSERT | UPDATE'
            * *condition_expression:* (``Union[Callable, str]``): An expression that returns a boolean indicating if
              the route should be called for a particular record. If type is ``str`` then the expression will be parsed using
              ``dynamodb_stream_router.conditions.Expression.parse`` to generate the callable

        :returns:
            ``Callable``

        """
        known_operations = [x.name for x in Operations]

        if not isinstance(operations, list):
            operations = [operations]

        for op in operations:
            if op not in known_operations:
                raise TypeError(
                    "Supported operations are 'REMOVE', 'INSERT', and 'UPDATE'"
                )

        def inner(func: Callable) -> Callable:
            route = Route(
                operations=operations,
                callable=func,
                condition_expression=condition_expression,
            )

            for x in route.operations:
                self.routes._asdict()[x].append(route)

            return func

        return inner

    @property
    def threaded(self) -> bool:
        return self.__threaded

    @threaded.setter
    def threaded(self, val: bool):
        if val == self.__threaded:
            return
        if True and not self.__threads:
            self.__executor = ThreadPoolExecutor()
        else:
            self.__executor = ThreadPoolExecutor(max_threads=self.__threads)

    @property
    def threads(self) -> int:
        return self.__threads or 0

    @threads.setter
    def threads(self, val: int):
        if val == self.__threads:
            return

        if val == 0:
            self.__threaded = False

        else:
            self.__threaded = True
            self.__threads = val
            self.__executor = ThreadPoolExecutor(max_threads=val)

    @property
    def threaded(self) -> bool:
        """
        If True, then each record will be handled in its own thread using ThreadPoolExecutor

        :getter: Returns a boolean indicating if threading will be used
        :type: bool
        """
        return bool(self.__threads)

    def resolve_all(self, records: List[dict]) -> List[Result]:
        """
        Iterates through each record in a batch and calls any matching resolvers on them, returning a
        list containing ``Result`` objects for any routes that were called on the records

        :Arguments:
            * *records:* (``List[dict]``)

        :returns:
            ``List[dynamodb_stream_router.Result]``
        """
        self.records = [StreamRecord(x) for x in records]

        if self.threads:
            res = self.__executor.map(self.resolve_record, self.records)
        else:
            res = map(self.resolve_record, self.records)

        results = []
        for x in res:
            results += x

        return results

    def resolve_record(self, record: StreamRecord) -> List[Result]:
        """
        Resolves a single record, returning a list containing ``Result`` objects for any
        routes that were called on the record

        :Arguments:
            * *record:* (``dict``): A single stream record

        :returns:
            ``List[dynamodb_stream_router.Result]``
        """

        routes_to_call = []
        op = record.eventName

        for route in self.routes._asdict()[op]:
            if route.condition_expression is None:
                routes_to_call.append(route)

            else:
                if isinstance(route.condition_expression, str):
                    try:
                        test = self._condition_parser.evaluate(
                            route.condition_expression, record=record
                        )
                    except Exception as e:
                        raise ValueError(
                            f"Could not parse {route.condition_expression}") from e
                else:
                    try:
                        test = route.condition_expression(record)
                    except Exception as e:
                        raise ValueError(f"Could not parse expression using {route.condition_expression}") from e

                if test:
                    routes_to_call.append(route)

        record_args = [record for _ in routes_to_call]

        return map(self.__execute_route_callable, routes_to_call, record_args)

    def __execute_route_callable(self, route, record):
        return Result(route=route, record=record, value=route.callable(record))

    @staticmethod
    def test_conditional_func(record: dict, funcs: List[Callable]) -> bool:
        """
        Accepts list of Callables that will be called with ``record`` passed as an argument. Callables
        should return a bool indicating whether or not a route should be called. If any Callable in the
        list returns True then True will be returned by the method, otherwise False

        :Arguments:
            * *record:* (``dict``): A single stream record
            * *funcs:* (``dict``): A list of truthy Callables

        :returns:
            ``bool``
        """
        for func in funcs:
            if func(record):
                return True

        return False


def parse_image(image: dict):
    if isinstance(image, dict):
        is_single = True
        items = [image]
    else:
        items = image
        is_single = False

    def parseList(dynamoList):
        i = 0
        for d in dynamoList:
            dynamoType = list(d.keys())[0]
            dynamoList[i] = typeMap[dynamoType](d[dynamoType])
            i += 1
        return dynamoList

    def parseMap(dynamoMap):
        for d in dynamoMap:
            dynamoType = list(dynamoMap[d].keys())[0]
            dynamoMap[d] = typeMap[dynamoType](dynamoMap[d][dynamoType])
        return dynamoMap

    typeMap = {
        "S": lambda x: x,
        "N": lambda x: x,
        "L": parseList,
        "B": lambda x: bytes(x.encode()),
        "BS": parseList,
        "BOOL": lambda x: x,
        "NS": parseList,
        "NULL": lambda x: None,
        "SS": parseList,
        "M": parseMap,
    }

    i = 0
    for item in items:
        newItem = {}
        for attributeName in item.keys():
            dynamoType = next(iter(item[attributeName]))
            val = typeMap[dynamoType](item[attributeName][dynamoType])
            newItem[attributeName] = val

        items[i] = newItem
        i += 1

    if is_single:
        items = items[0]

    return items
