#!/usr/bin/env python3.8
from concurrent.futures import ThreadPoolExecutor
import typeguard
from os import environ
from enum import (
    Enum,
    auto
)
from typing import (
    Callable,
    List,
    NamedTuple,
    Union,
    Any
)
from .conditions import Expression

if not environ.get("TYPECHECKED"):
    typeguard.typechecked = lambda: True


class Image(Enum):
    old = auto()
    new = auto()


class Operations(Enum):
    REMOVE = auto()
    INSERT = auto()
    UPDATE = auto()


class Route(NamedTuple):
    #: The Callable that will be triggered if this route is a match for a record
    callable: Callable
    #: The operations that this route is registered for (UPDATE | INSERT | DELETE)
    operations: List[Operations]
    #: Optional dynamodb_stream_router.Expression to decide whether this route should be called on a record
    condition_expression: Expression = None
    #: Optional list of truth Callables to decide whether this route should be called on a record
    filter: Union[Callable, List[Callable]] = []


class Result(NamedTuple):
    #: The complete ``Route`` object that generated this result
    route: Route
    #: The original stream record passed to the Route's callable
    record: dict
    #: The return value of the callable for the Route that was called
    value: Any


class StreamRouter:

    __instance = None
    __threaded = False
    __executor = None

    def __new__(
        cls,
        *args,
        threaded: bool = False,
        **kwargs
    ):
        if cls.__instance is None:
            cls.__threaded = threaded
            if cls.__threaded:
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
        self.routes: List[Route] = []

    def update(
        self,
        **kwargs
    ) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "UPDATE" operation, taking the same arguments
        """
        return self.route("UPDATE", **kwargs)

    def remove(
        self,
        **kwargs
    ) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "REMOVE" operation, taking the same arguments
        """
        return self.route("REMOVE", **kwargs)

    def insert(
        self,
        **kwargs
    ) -> Callable:
        """
        Wrapper for StreamRouter.route. Creates a route for "INSERT" operation, taking the same arguments
        """
        return self.route("INSERT", **kwargs)

    def route(
        self,
        operations: Union[str, List[str]],
        condition_expression: Expression = None,
        filter: Union[Callable, List[Callable]] = []
    ) -> Callable:

        """
        Used as a decorator to register a route. Accepts keyword arguments that determine under what conditions the route will
        be called. If no condition_expression or filter is provided then the route will be called for any operations that are
        passed. If both condition_expression and filter are passed the results of the both will be OR'd together to decide whether
        or not the route is called for a particular record

        :Keyword Arguments:
            * *operations:* (``Union[str, List[str]``): A Dynamodb operation or list of operations. Can be one or
              more of 'REMOVE | INSERT | UPDATE'
            * *condition_expression:* (``dynamodb_stream_router.conditions.Expression``): An expression that returns a boolean indicating if
              the route should be called for a particular record
            * *filter:* (``Union[Callable, List[Callable]``): A Callable, or list of Callables that accept a stream record as their
              sole argument and return a boolean, indication if the route should be called for the record

        :returns:
            ``Callable``

        """
        known_operations = [x.name for x in Operations]

        if not isinstance(operations, list):
            operations = [operations]
        if not isinstance(filter, list):
            filter = [filter]

        for op in operations:
            if op not in known_operations:
                raise TypeError("Supported operations are 'REMOVE', 'INSERT', and 'UPDATE'")

        def inner(func: Callable) -> Callable:
            route = Route(
                operations=operations,
                callable=func,
                filter=filter,
                condition_expression=condition_expression
            )
            self.routes.append(route)
            return func

        return inner

    @property
    def threaded(self) -> bool:
        """
        If True, then each record will be handled in its own thread using ThreadPoolExecutor

        :getter: Returns a boolean indicating if threading will be used
        :type: bool
        """
        return self.__threaded

    @threaded.setter
    def threaded(self, val: bool):
        """If True, then each record will be handled in its own thread using ThreadPoolExecutor"""

        self.__threaded = val

    def resolve_all(self, records: List[dict]) -> List[Result]:
        """
        Iterates through each record in a batch and calls any matching resolvers on them, returning a
        list containing ``Result`` objects for any routes that were called on the records

        :Arguments:
            * *records:* (``List[dict]``)

        :returns:
            ``List[dynamodb_stream_router.Result]``
        """
        self.records = records

        for i, record in enumerate(self.records):
            self.records[i]["routes"] = [
                x for x in self.routes
                if record["operation"] in x.operations
            ]

        if self.threaded:
            res = self.__executor.map(self.resolve_record, self.records)
        else:
            res = map(self.resolve_record, self.records)

        results = []
        for x in res:
            results += x

        return results

    def resolve_record(self, record: dict) -> List[Result]:
        """
        Resolves a single record, returning a list containing ``Result`` objects for any
        routes that were called on the record

        :Arguments:
            * *record:* (``dict``): A single stream record

        :returns:
            ``List[dynamodb_stream_router.Result]``
        """
        routes = record["routes"]
        routes_to_call = []
        for route in routes:
            if (
                not (route.condition_expression or route.filter)
                or (
                    route.condition_expression is not None
                    and route.condition_expression(record)
                )
                or self.test_conditional_func(record, route.filter)
            ):
                routes_to_call.append(route)

        return map(self.__execute_route_callable, routes_to_call, [record for _ in routes_to_call])

    def __execute_route_callable(self, route, record):
        return Result(
            route=route,
            record=record,
            value=route.callable(record)
        )

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
