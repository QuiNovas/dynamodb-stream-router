#!/usr/bin/env python3.8
from decimal import Decimal


class ExpressionBase:
    def __init__(self):
        self.exp = ""

    def __and__(self, other):
        return Expression(f"{self.exp} and {other.exp}")

    def __or__(self, other):
        return Expression(f"{self.exp} or {other.exp}")

    def __ror__(self, other):
        return Expression(f"{other.exp} or {self.exp}")

    def __rand__(self, other):
        return Expression(f"{other.exp} and {self.exp}")

    def __call__(self):
        return Expression(f"({self.exp})")

    @staticmethod
    def quote_str(val):
        return f'"{val}"' if isinstance(val, str) else val


class Expression(ExpressionBase):
    __item = None

    def __init__(self, exp):
        self.exp = exp

    def __str__(self):
        return self.exp

    def __call__(self, item=None):
        if item:
            self.item = item

        if self.item is None:
            raise Exception(
                "Cannot evaluate without attribute 'item' being set")

        return eval(str(self))

    @property
    def item(self):
        return self.__item

    @item.setter
    def item(self, item: dict):
        if not isinstance(item, dict):
            raise TypeError("Expression.item must be a dict")
        item["new"] = item.get("new") or {}
        item["old"] = item.get("old") or {}
        self.__item = item

    def has_changed(self, key):
        if isinstance(key, list):
            exps = [
                self.has_changed(x) for x in key
            ]
            op = " or "
            self.exp = op.join(exps)

        else:
            key = self.quote_str(key)
            return f"""self.item["old"].get({key}) != self.item["new"].get({key})"""

    def evaluate(self, item=None):
        return self(item=item)


class Group(ExpressionBase):
    def __init__(self, exp):
        self.exp = f"({exp})"


class HasChanged(Expression):
    def __init__(self, keys):
        super().__init__("")
        self = Expression(self.has_changed(keys))


class Key(ExpressionBase):

    __known_types = (
        list,
        dict,
        int,
        float,
        Decimal,
        str,
        bool
    )

    __known_types_str = ", ".join([
        str(x) for x in __known_types
    ])

    def __init__(self, image=None, key=None, full_path=None):
        super().__init__()
        if not (
            full_path
            or (image and key)
        ):
            raise AttributeError("Key() expects either full_path, or image and key")

        path_base = "self.item"
        self.base = path_base
        if full_path:
            self.path = full_path
        else:
            self.path = f"""{path_base}[{self.quote_str(image)}][{self.quote_str(key)}]"""
        self.exp = None

    def __getitem__(self, index):
        return Key(full_path=f"{self.path}[{self.quote_str(index)}]")

    def get(self, name: str):
        return Key(full_path=f"{self.path}.get({self.quote_str(name)})")

    def eq(self, val):
        return Expression(f"{self.path} == {self.quote_str(val)}")

    def ne(self, val):
        return Expression(f"{self.path} != {self.quote_str(val)}")

    def contains(self, val):
        return Expression(f"{self.quote_str(val)} in {self.path}")

    def not_contains(self, val):
        return Expression(f"{self.quote_str(val)} not in {self.path}")

    def is_in(self, val):
        return Expression(f"{self.path} in {self.quote_str(val)}")

    def not_in(self, val):
        return Expression(f"{self.path} not in {self.quote_str(val)}")

    def lt(self, val):
        return Expression(f"{self.path} < {self.quote_str(val)}")

    def lte(self, val):
        return Expression(f"{self.path} <= {self.quote_str(val)}")

    def gt(self, val):
        return Expression(f"{self.path} > {self.quote_str(val)}")

    def gte(self, val):
        return Expression(f"{self.path} >= {self.quote_str(val)}")

    def between(self, start, end):
        return Expression(f"{self.quote_str(start)} < {self.path} > {self.quote_str(end)}")

    def begins_with(self, val):
        return Expression(f"{self.path}.startswith({self.quote_str(val)})")

    def ends_with(self, val):
        return Expression(f"{self.path}.endswith({self.quote_str(val)})")

    def exists(self, key):
        return Expression(f"{self.quote_str(key)} in {self.base}")

    def is_null(self):
        return Expression(f"{self.path} is None")

    def is_not_null(self):
        return Expression(f"{self.path} is not None")

    def as_bool(self):
        return Expression(f"bool({self.path})")

    def is_type(self, type_name):

        if type_name not in self.__known_types:
            raise TypeError(f"Key.is_type() only supports the following types: {self.__known_types_str}")

        type_name = type_name.__name__

        return Expression(f"isinstance({self.path}, {type_name})")

    def is_not_type(self, type_name):
        if type_name not in self.__known_types:
            raise TypeError(
                f"Key.is_type() only supports the following types: {self.__known_types_str}")

        type_name = type_name.__name__

        return Expression(f"not isinstance({self.path}, {type_name})")


class Old(Key):
    def __init__(self, key):
        super().__init__("old", key)


class New(Key):
    def __init__(self, key):
        super().__init__("new", key)
