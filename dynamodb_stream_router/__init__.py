#!/usr/bin/env python3.8
from concurrent.futures import ThreadPoolExecutor
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


class Image(Enum):
    old = auto()
    new = auto()


class Operations(Enum):
    REMOVE = auto()
    INSERT = auto()
    UPDATE = auto()


class Route(NamedTuple):
    callable: Callable
    operations: List[Operations]
    condition_expression: Expression = None
    filter: Union[Callable, List[Callable]] = []


class Result(NamedTuple):
    route: Route
    item: dict
    value: Any


class StreamRouter:

    __instance = None
    __threaded = False
    __executor = None

    def __new__(cls, *args, threaded: bool = False, **kwargs):
        if cls.__instance is None:
            cls.__threaded = threaded
            if cls.__threaded:
                cls.__executor = ThreadPoolExecutor()
            cls.__instance = super().__new__(cls, *args, **kwargs)

        return cls.__instance

    def __init__(self, *args, **kwargs):
        self.routes = []

    def route(
        self,
        operations: Union[str, List[str]],
        condition_expression: Expression = None,
        filter: Union[Callable, List[Callable]] = []
    ):
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
        return self.__threaded

    @threaded.setter
    def threaded(self, val: bool):
        self.__threaded = val

    def resolve_all(self, items: list):
        self.items = items

        for i, item in enumerate(self.items):
            self.items[i]["routes"] = [
                x for x in self.routes
                if item["operation"] in x.operations
            ]

        if self.threaded:
            res = self.__executor.map(self.resolve_item, self.items)
        else:
            res = map(self.resolve_item, self.items)

        results = []
        for x in res:
            results += x

        return results

    def resolve_item(self, item: dict) -> list:
        routes = item["routes"]
        routes_to_call = []
        for route in routes:
            if (
                not (route.condition_expression or route.filter)
                or (
                    route.condition_expression is not None
                    and route.condition_expression(item)
                )
                or self.test_conditional_func(item, route.filter)
            ):
                routes_to_call.append(route)

        return map(self.__execute_route_callable, routes_to_call, [item for _ in routes_to_call])

    def __execute_route_callable(self, route, item):
        return Result(
            route=route,
            item=item,
            value=route.callable(item)
        )

    @staticmethod
    def test_conditional_func(item: dict, funcs: List[Callable]) -> bool:
        for func in funcs:
            if func(item):
                return True

        return False
