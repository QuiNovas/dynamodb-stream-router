#!/usr/bin/env python3.8
from __future__ import annotations
from typing import (
    Any,
    List,
    Union,
)
from .router import StreamRecord


class Expression:
    """
    A chain of Key, Old, New, HasChanged objects that are formed to create a final expression that can be evaluated against a
    stream record. It doesn't really make sense to create an object of this type directly. It is a byproduct of creating an
    expression of statements using the above mentioned classes. It's main purpose is to facilitate the evaluation of those statements.

    An example expression:

    .. highlight:: python
    .. code-block:: python

        from dynamodb_stream_router.conditions import Old, New

        exp = Old("pk").eq("foo") & New("count").lt(10)
        exp.record = {
            "StreamViewType": "OLD_AND_NEW_IMAGES",
            "OldImage": {"pk": "foo"},
            "NewImage": {"count": 5}}
        }

        exp.evaluate()
        # returns True

    """

    def __init__(self, exp):
        self.exp = exp
        self.record = None

    def __and__(self, other):
        return Expression(f"{self.exp} and {other.exp}")

    def __or__(self, other):
        return Expression(f"{self.exp} or {other.exp}")

    def __ror__(self, other):
        return Expression(f"{other.exp} or {self.exp}")

    def __rand__(self, other):
        return Expression(f"{other.exp} and {self.exp}")

    @staticmethod
    def __index_getter__(exp, index):
        if isinstance(exp, list) and len(exp) >= index:
            return exp[index]
        else:
            return False

    def __getitem__(self, index):
        if isinstance(index, int):
            return Expression(f"self.__index_getter__({self.exp}, {index})")
        else:
            return Expression(f"{self.exp}.get({self.quote_str(index)})")

    @staticmethod
    def __attribute_getter__(exp, name):
        if isinstance(exp, dict):
            return exp.get(name, False)
        else:
            return False

    def __getattribute__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            name = self.quote_str(name)
            return Expression(f"self.__attribute_getter__({self.exp}, {name})")

    def eq(self, val: Any):
        """
        Returns the equality of the current item and ``val`` when evaluated

        :Arguments:
            * *val:* (``Any``): The value to compare the current item to

        :returns:
            ``Expression``
        """
        return Expression(f"{self.exp} == {self.quote_str(val)}")

    @staticmethod
    def quote_str(val: Any) -> Any:
        """
        Returns a value wrapped if double quotes if it is a string, otherwise returns the original value

        :Keyword Arguments:
            * *val:* (``Any`): The value to process

        :returns: ``Any``
        """
        return f'"{val}"' if isinstance(val, str) else val

    def __str__(self):
        return self.exp

    def __call__(self, record: StreamRecord) -> bool:
        self.record = record
        return eval(self.exp)

    def _has_changed(self, keys):
        old_keys = list(self.record.OldImage.keys())
        new_keys = list(self.record.NewImage.keys())

        return bool([
            k for k in keys
            if (
                k in old_keys
                and k in new_keys
            )
            and self.record.NewImage[k] != self.record.OldImage[k]
        ])

    def _is_type(self, type_str):
        self.type_map = {
            "S": "lambda x: isinstance(x, str)",
            "L": lambda x: self.is_l(x),
            "SS": lambda x: self.is_ss(x),
            "NS": lambda x: self.is_ns(x),
            "M": lambda x: isinstance(x, dict),
            "B": lambda x: isinstance(x, bytes),
            "NULL": lambda x: x is None,
            "BOOL": lambda x: isinstance(x, bool)
        }

        if type_str not in self.type_map:
            raise TypeError(f"Unknown Dynamodb type '{type_str}'")

        return Expression(self.type_map[type_str])

    def is_bytes(self, val):
        return isinstance(val, bytes)

    def is_ss(self, val):
        return isinstance(val, list) and [
            x for x in val if isinstance(val, str)
        ]

    def is_ns(self, val):
        return isinstance(val, list) and [
            x for x in val if isinstance(val, (int, float))
        ]

    def is_l(self, val):
        if not isinstance(val, list):
            return False

        first_type = type(val)
        for x in val[1:]:
            if type(x) != first_type:
                return False

        return True

    def is_bool(self, val):
        return type(val, bool)

    def is_null(self, val):
        return val is None

    def is_str(self, val):
        return isinstance(val, str)

    def is_m(self, val):
        return isinstance(val, dict)

    def ne(self, val: Any) -> Expression:
        """
        Returns the non equality of the current item and ``val`` when evaluated

        :Arguments:
            * *val:* (``Any``): The value to compare the current item to

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} != {self.quote_str(val)}")

    def contains(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item contains ``val`` when evaluated. Equivalent
        to python's ``val in foo``

        :Arguments:
            * *val:* (``Any``): The value to test if in current item

        :returns:
            ``Expression``
        """
        return Expression(f"{self.quote_str(val)} in {self.path}")

    def not_contains(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item does not contain ``val`` when evaluated. Equivalent
        to python's ``val not in foo``

        :Arguments:
            * *val:* (``Any``): The value to test if in current item

        :returns:
            ``Expression``
        """
        return Expression(f"{self.quote_str(val)} not in {self.path}")

    def is_in(self, val: Any) -> Expression:
        """
        Returns a bool indicating if ``val`` contains the current item when evaluated. Equivalent
        to python's ``val in foo``

        :Arguments:
            * *val:* (``Any``): The value to test current item is in

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} in {self.quote_str(val)}")

    def not_in(self, val: Any) -> Expression:
        """
        Returns a bool indicating if ``val`` does not contain the current item when evaluated. Equivalent
        to python's ``val not in foo``

        :Arguments:
            * *val:* (``Any``): The value to test current item is in

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} not in {self.quote_str(val)}")

    def lt(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item is less than ``val`` when evaluated. Equivalent
        to python's ``foo < bar``

        :Arguments:
            * *val:* (``Any``): The value to test current item against

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} < {self.quote_str(val)}")

    def lte(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item is less than or equal to ``val`` when evaluated.
        Equivalent to python's ``foo <= bar``

        :Arguments:
            * *val:* (``Any``): The value to test current item against

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} <= {self.quote_str(val)}")

    def gt(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item is greater than ``val`` when evaluated. Equivalent
        to python's ``foo > bar``

        :Arguments:
            * *val:* (``Any``): The value to test current item against

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} > {self.quote_str(val)}")

    def gte(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item is greater than or equal to ``val`` when evaluated.
        Equivalent to python's ``foo >= bar``

        :Arguments:
            * *val:* (``Any``): The value to test current item against

        :returns:
            ``Expression``
        """

        return Expression(f"{self.path} >= {self.quote_str(val)}")

    def between(self, start: Any, end: Any) -> Expression:
        """
        Returns a bool indicating if the current item is greater than ``start`` and less than ``end`` when evaluated.
        Equivalent to python's ``start < foo < bar``

        :Arguments:
            * *start:* (``Any``): The value to test current item is greater than
            * *end:* (``Any``): The value to test current item is greater than

        :returns:
            ``Expression``
        """

        return Expression(f"{self.quote_str(start)} < {self.path} < {self.quote_str(end)}")

    def begins_with(self, val: str) -> Expression:
        """
        Returns a bool indicating if the current item begins with ``val`` when evaluated. Equivalent to python's
        ``"foo".startswith("bar")``

        :Arguments:
            * *val:* (``str``): The value to test current item against

        :returns:
            ``Expression``
        """

        return Expression(f"{self.path}.startswith({self.quote_str(val)})")

    def ends_with(self, val: Any) -> Expression:
        """
        Returns a bool indicating if the current item ends with ``val`` when evaluated. Equivalent to python's
        ``"foo".endswith("bar")``

        :Arguments:
            * *val:* (``str``): The value to test current item against

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path}.endswith({self.quote_str(val)})")

    def exists(self, key: Union[str, int]) -> Expression:
        """
        Returns a bool indicating if the specified key/index exists in the current item when evaluated

        :Arguments:
            * *val:* (``Union[str, int]``): The value to test current item against

        :returns:
            ``Expression``
        """
        return Expression(f"{self.quote_str(key)} in {self.base}")

    """
    def is_null(self) -> Expression:
        Returns a bool indicating if the current item is None when evaluated. Equivalent to
        python's ``foo is None``

        :returns:
            ``Expression``
        return Expression(f"{self.path} is None")
    """

    def is_not_null(self) -> Expression:
        """
        Returns a bool indicating if the current item is not None when evaluated. Equivalent to
        python's ``foo is not None``

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} is not None")

    def is_true(self) -> Expression:
        """
        Returns a bool indicating if the current item is True when evaluated. Equivalent to
        python's ``foo is True``

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} is True")

    def is_false(self) -> Expression:
        """
        Returns a bool indicating if the current item is False when evaluated. Equivalent to
        python's ``foo is False``

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} is False")

    def as_bool(self) -> Expression:
        """
        Casts the current item to a bool when evaluated. Equivalent to
        python's ``bool(foo)``

        :returns:
            ``Expression``
        """
        return Expression(f"bool({self.path})")

    def evaluate(self, record: StreamRecord) -> bool:
        """
        Evaluates the stored expression, using ``record`` as the stream record to inspect. If record is not passed then self.record
        will be used. If record is None and self.record is not set an exception will be raised

        :Keyword Arguments:
            * *record:* (``dict``): The stream record to inspect

        :returns:
            ``bool``
        """
        return self(record)

    def invert(self):
        """
        Inverts the end result of an Expression

        :returns:
            ``Expression``
        """
        return Expression(f"not {self.exp}")


class Group(Expression):
    """
    Used to group statements (New, Old, Key, HasChanged) together and set precedence.
    Example:

    .. highlight:: python
    .. code-block:: python

        from dynamodb_stream_router import Group, New, Old, HasChannged

        exp = Group(New("sk").eq("foo") & Old("sk").ne("bar")) | HasChanged(["baz"])

    In simple terms, Group provides the same functionality of grouping statements using '()' in python

    :Arguments:
        * *exp:* (``dynamodb_stream_router.condition.Expression``): The Expression (normally a group of Key/New/Old/HasChanged) to group
    """
    def __init__(self, exp: Expression):
        self.exp = f"({exp})"


class HasChanged(Expression):
    """
    Iterates through a string or a list of strings, representing dictionary keys, to test if that particular key exists
    in, and differs between, the old and new images. The first time a change is detected the method will return True. If
    no matches are found by the last iteration then the method will return False. This is a wrapper around Expression.__has_changed()
    and is the intended way to use it. When evaluated it will return a boolean if any of the values at ``keys`` has changed.

    :Keyword Arguments:
        * *keys:* (``List[str]``): A list of keys to test for change

    :returns:
        ``Expression``
    """
    def __new__(cls, keys: List[str]):
        return Expression(f"self._has_changed({keys})")


class IsType(Expression):
    __path = None

    def __new__(cls, path: Expression, type_str: str):
        cls.__path = str(path)
        cls.type_map = {
            "S": f"isinstance({cls.__path}, str)",
            "L": f"self.is_l({cls.__path})",
            "SS": f"self.is_ss({cls.__path})",
            "NS": f"self.is_ns({cls.__path})",
            "M": f"isinstance({cls.__path}, dict)",
            "B": f"isinstance({cls.__path}, bytes)",
            "NULL": f"{cls.__path} is None",
            "BOOL": f"isinstance({cls.__path}, bool)"
        }

        return Expression(cls.__is_type(type_str))

    @classmethod
    def __is_type(cls, type_str):
        if type_str not in cls.type_map:
            raise TypeError(f"Unknown Dynamodb type '{type_str}'")

        func = cls.type_map[type_str]

        return func


class Key(Expression):
    """
    Provides methods intended to be used with New() and Old() for building an Expression. While this can be accessed directly,
    it is recommended to use New and Old, which inherit all of Key's methods.

    :Keyword Arguments:
        * *image:* (``str``): The image to inspect. Can be old | new
        * *key:* (``str``): The key inside of the image to operate on
        * *full_path:* (``str``): If provided self.exp will be overwritten with this value
    """

    def __init__(
        self,
        image: str = None,
        key: str = None,
        full_path: str = None
    ):

        if not (
            full_path
            or (image and key)
        ):
            raise AttributeError("Key() expects either full_path, or image and key")

        path_base = "self.record"
        self.base = path_base
        if full_path:
            self.path = full_path
        else:
            self.path = f"""{path_base}[{self.quote_str(image)}][{self.quote_str(key)}]"""
        self.exp = self.path
        super().__init__(self.exp)

    def __getattribute__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            return f"{self.path}.get({self.quote_str(name)})"

    def get(self, key: str):
        """
        Returns the value at the specified key when evaluated. Mimics ``dict.get()``

        :Arguments:
            * *key:* (``str``): The key of the current item to return

        :returns:
            ``Expression``
        """
        return Key(full_path=f"{self.path}.get({self.quote_str(key)})")

    """
    def is_type(self, obj_type: type) -> Expression:
        Returns boolena indicating if the current item is of type ``obj_type`` when evaluated.
        Equivalent to python's ``isinstance(foo, SomeType)``. Supported types are:
        * list
        * dict
        * int
        * float
        * Decimal
        * str
        * bool

        Testing any other type will cause an exception to be raised

        :Arguments:
            * *obj_type:* (``type``): The type to test current item against

        if obj_type not in self.__known_types:
            raise TypeError(f"Key.is_type() only supports the following types: {self.__known_types_str}")

        type_name = obj_type.__name__

        return Expression(f"isinstance({self.path}, {type_name})")
    """

    def is_not_type(self, obj_type: type) -> Expression:
        """
        Returns boolena indicating if the current item is not of type ``obj_type`` when evaluated.
        Equivalent to python's ``not isinstance(foo, SomeType)``. Supported types are:
        * list
        * dict
        * int
        * float
        * Decimal
        * str
        * bool

        Testing any other type will cause an exception to be raised

        :Arguments:
            * *obj_type:* (``type``): The type to test current item against
        """

        if obj_type not in self.__known_types:
            raise TypeError(
                f"Key.is_type() only supports the following types: {self.__known_types_str}")

        type_name = obj_type.__name__

        return Expression(f"not isinstance({self.path}, {type_name})")


class Old(Key):
    """
    Creates an Expression that evaluates against the old image of a record. Expressions can be grouped together
    using the Group class and be and'd and or'd together using '&' and '|'.

    :Arguments:
        * *key:* (``str``): The key in the old image to inspect
    """

    def __init__(self, key: str = None):
        if key is not None:
            full_path = f"""self.record.OldImage["{key}"]"""
        else:
            full_path = "self.record.OldImage"

        super().__init__(full_path=full_path)

    def __str__(self):
        return self.path


class New(Key):
    """
    Creates an Expression that evaluates against the new image of a record. Expressions can be grouped together
    using the Group class and be and'd and or'd together using '&' and '|'

    :Arguments:
        * *key:* (``str``): The key in the new image to inspect
    """

    def __init__(self, key: str = None):
        if key is not None:
            full_path = f"""self.record.NewImage["{key}"]"""
        else:
            full_path = "self.record.NewImage"

        super().__init__(full_path=full_path)

    def __str__(self):
        return self.path
