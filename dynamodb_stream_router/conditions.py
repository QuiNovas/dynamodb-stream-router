#!/usr/bin/env python3.8
from decimal import Decimal
from typing import (
    Any,
    List,
    Union
)
from .router import Record


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
    def quote_str(val: Any) -> Any:
        """
        Returns a value wrapped if double quotes if it is a string, otherwise returns the original value

        :Keyword Arguments:
            * *val:* (``Any`): The value to process

        :returns: ``Any``
        """
        return f'"{val}"' if isinstance(val, str) else val


class Expression(ExpressionBase):
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
    __record = None

    def __init__(self, exp):
        self.exp = exp

    def __str__(self):
        return self.exp

    def __call__(self, record=None):
        if record:
            self.record = record

        if self.record is None:
            raise Exception(
                "Cannot evaluate without attribute 'record' being set")

        return eval(str(self))

    @property
    def record(self) -> dict:
        """
        The stream record that will be inspected by the expression. This must be set before a call to
        Expression.evaluate()

        :returns:
            ``dict``
        """
        return self.__record

    @record.setter
    def record(self, record: Union[dict, Record]) -> dict:
        if not isinstance(record, (dict, Record)):
            raise TypeError("Expression.record must be a dict or instance of dynamodb_stream_router.Record")

        if isinstance(record, dict):
            try:
                record = Record(**record)
            except TypeError:
                raise TypeError("Invalid stream record")

        self.__record = record

    def evaluate(self, record: dict = None) -> bool:
        """
        Evaluates the stored expression, using ``record`` as the stream record to inspect. If record is not passed then self.record
        will be used. If record is None and self.record is not set an exception will be raised

        :Keyword Arguments:
            * *record:* (``dict``): The stream record to inspect

        :returns:
            ``bool``
        """
        return self(record=record)

    def invert(self):
        """
        Inverts the end result of an Expression

        :returns:
            ``Expression``
        """
        return Expression(f"not {self.exp}")


class Group(ExpressionBase):
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
        return Expression(cls.__has_changed(keys))

    @classmethod
    def __has_changed(cls, key: Union[str, List[str]]) -> str:
        if isinstance(key, list):
            exps = [
                cls.__has_changed(x) for x in key
            ]
            op = " or "
            exp = op.join(exps)

        else:
            key = ExpressionBase.quote_str(key)
            exp = f"""self.record.OldImage.get({key}) != self.record.NewImage.get({key})"""

        return exp


class Key(ExpressionBase):
    """
    Provides methods intended to be used with New() and Old() for building an Expression. While this can be accessed directly,
    it is recommended to use New and Old, which inherit all of Key's methods.

    :Keyword Arguments:
        * *image:* (``str``): The image to inspect. Can be old | new
        * *key:* (``str``): The key inside of the image to operate on
        * *full_path:* (``str``): If provided self.exp will be overwritten with this value
    """
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

    def __init__(
        self,
        image: str = None,
        key: str = None,
        full_path: str = None
    ):

        super().__init__()
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
        self.exp = None

    def __getitem__(self, index):
        return Key(full_path=f"{self.path}[{self.quote_str(index)}]")

    def get(self, key: str):
        """
        Returns the value at the specified key when evaluated. Mimics ``dict.get()``

        :Arguments:
            * *key:* (``str``): The key of the current item to return

        :returns:
            ``Expression``
        """
        return Key(full_path=f"{self.path}.get({self.quote_str(key)})")

    def eq(self, val: Any) -> Expression:
        """
        Returns the equality of the current item and ``val`` when evaluated

        :Arguments:
            * *val:* (``Any``): The value to compare the current item to

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} == {self.quote_str(val)}")

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

    def is_null(self) -> Expression:
        """
        Returns a bool indicating if the current item is None when evaluated. Equivalent to
        python's ``foo is None``

        :returns:
            ``Expression``
        """
        return Expression(f"{self.path} is None")

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

    def is_type(self, obj_type: type) -> Expression:
        """
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
        """

        if obj_type not in self.__known_types:
            raise TypeError(f"Key.is_type() only supports the following types: {self.__known_types_str}")

        type_name = obj_type.__name__

        return Expression(f"isinstance({self.path}, {type_name})")

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
    def __init__(self, key: str):
        full_path = f"""self.record.OldImage["{key}"]"""
        super().__init__(full_path=full_path)


class New(Key):
    """
    Creates an Expression that evaluates against the new image of a record. Expressions can be grouped together
    using the Group class and be and'd and or'd together using '&' and '|'

    :Arguments:
        * *key:* (``str``): The key in the new image to inspect
    """
    def __init__(self, key: str):
        full_path = f"""self.record.NewImage["{key}"]"""
        super().__init__(full_path=full_path)
