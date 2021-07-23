#!/usr/bin/env python3.8
# pyright: reportUndefinedVariable=false
from sly import Parser
from .lexer import ExpressionLexer
from re import match

StreamRecord = object


def is_bytes(val):
    return isinstance(val, bytes)


def is_bs(val):
    return isinstance(val, list) and [x for x in val if isinstance(val, bytes)]


def is_ss(val):
    return isinstance(val, list) and [x for x in val if isinstance(val, str)]


def is_ns(val):
    return isinstance(val, list) and [
        x for x in val if isinstance(val, (int, float))
    ]


def is_l(val):
    if not isinstance(val, list):
        return False

    first_type = type(val)
    for x in val[1:]:
        if type(x) != first_type:
            return False

    return True


def is_bool(val):
    return type(val, bool)


def is_null(val):
    return val is None


def is_str(val):
    return isinstance(val, str)


def is_m(val):
    return isinstance(val, dict)


class Expression(Parser):

    _expression_cache = {}

    def __init__(self, record=None):
        self.__record = None
        self.__old_image = None
        self.__new_image = None
        self.__old_keys = None
        self.__new_keys = None
        if record is not None:
            self.record = record
        super().__init__()

    def evaluate(self, expression, record=None):
        if callable(expression):
            return expression(record or self.record)
        else:
            if record:
                self.record = record
            return self.parse(expression)(record)

    def parse(self, expression):
        if expression not in self._expression_cache:
            self._expression_cache[expression] = super().parse(
                ExpressionLexer().tokenize(expression)
            )
        return self._expression_cache[expression]

    @property
    def old(self) -> dict:
        return self.__old_image

    @property
    def new(self) -> dict:
        return self.__new_image

    @property
    def old_keys(self) -> list:
        return self.__old_keys

    @property
    def new_keys(self) -> list:
        return self.__new_keys

    @property
    def record(self) -> StreamRecord:
        return self.__record

    @staticmethod
    def strip_quotes(val):
        return val[1:-1]

    @record.setter
    def record(self, record: StreamRecord) -> None:
        self.__old_image = record.OldImage
        self.__new_image = record.NewImage
        self.__old_keys = list(record.OldImage.keys())
        self.__new_keys = list(record.NewImage.keys())
        self.__record = record

    # Get the token list from the lexer (required)
    tokens = ExpressionLexer.tokens

    # Define precendence
    precendence = (
        ("left", OR),  # noqa: 821
        ("left", AND),  # noqa: 821
        ("right", NOT),  # noqa: 821
    )

    # Grammar rules and actions
    @_("operand EQ operand")  # noqa: 821
    def condition(self, p):
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) == operand1(m)

    @_("operand NE operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) != operand1(m)

    @_("operand GT operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) > operand1(m)

    @_("operand GTE operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) >= operand1(m)

    @_("operand LT operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) < operand1(m)

    @_("operand LTE operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: operand0(m) <= operand1(m)

    @_("operand BETWEEN operand AND operand")  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        operand2 = p.operand2
        return lambda m: operand1(m) <= operand0(m) <= operand2(m)

    @_('operand IN "(" in_list ")"')  # noqa: 821
    def condition(self, p):  # noqa: 811
        operand = p.operand
        in_list = p.in_list
        return lambda m: operand(m) in in_list(m)

    @_("function")  # noqa: 821
    def condition(self, p):  # noqa: 811
        function = p.function
        return lambda m: function(m)

    @_("condition AND condition")  # noqa: 821
    def condition(self, p):  # noqa: 811
        condition0 = p.condition0
        condition1 = p.condition1
        return lambda m: condition0(m) and condition1(m)

    @_("condition OR condition")  # noqa: 821
    def condition(self, p):  # noqa: 811
        condition0 = p.condition0
        condition1 = p.condition1
        return lambda m: condition0(m) or condition1(m)

    @_("NOT condition")  # noqa: 821
    def condition(self, p):  # noqa: 811
        condition = p.condition
        return lambda m: not condition(m)

    @_('"(" condition ")"')  # noqa: 821
    def condition(self, p):  # noqa: 811
        condition = p.condition
        return lambda m: condition(m)

    @_('ATTRIBUTE_EXISTS "(" path ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path
        return lambda m: path(m) is not None

    @_('ATTRIBUTE_NOT_EXISTS "(" path ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path
        return lambda m: path(m) is None

    @_('ATTRIBUTE_TYPE "(" path "," operand ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path
        operand = p.operand
        return lambda x: path, operand

    @_('BEGINS_WITH "(" path "," operand ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path
        operand = p.operand
        return (
            lambda m: path(m).startswith(operand(m))
            if isinstance(path(m), str)
            else False
        )

    @_('CONTAINS "(" path "," operand ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path
        operand = p.operand
        return (
            lambda m: operand(m) in path(m)
            if isinstance(path(m), (str, set))
            else False
        )

    @_('SIZE "(" path ")"')  # noqa: 821
    def operand(self, p):  # noqa: 811
        path = p.path
        return (
            lambda m: len(path(m))
            if isinstance(path(m), (str, set, dict, bytearray, bytes, list))
            else -1
        )

    @_('in_list "," operand')  # noqa: 821
    def in_list(self, p):  # noqa: 811
        in_list = p.in_list
        operand = p.operand
        return lambda m: [*in_list(m), operand(m)]

    @_('operand "," operand')  # noqa: 821
    def in_list(self, p):  # noqa: 811
        operand0 = p.operand0
        operand1 = p.operand1
        return lambda m: [operand0(m), operand1(m)]

    @_("path")  # noqa: 821
    def operand(self, p):  # noqa: 811
        return p.path

    @_("VALUE")  # noqa: 821
    def operand(self, p):  # noqa: 811
        VALUE = p.VALUE
        if VALUE.startswith("'"):
            VALUE = VALUE.replace(r"\'", "'")
        else:
            VALUE = VALUE.replace(r'\"', '"')

        return lambda m: self.strip_quotes(VALUE)

    @_('operand MATCH operand')  # noqa: 821
    def function(self, f):  # noqa: 811
        regex = f.operand1(f)
        str_to_match = f.operand0(f)
        return lambda x: bool(match(regex, str_to_match))

    @_('path "." NAME')  # noqa: 821
    def path(self, p):  # noqa: 811
        path = p.path
        NAME = p.NAME
        return lambda m: path(m).get(NAME) if isinstance(path(m), dict) else None

    @_('path "[" VALUE "]"')  # noqa: 821
    def path(self, p):  # noqa: 811
        key = self.strip_quotes(p.VALUE)
        path = p.path

        return lambda m: path(m).get(key) if isinstance(path(m), dict) else None

    @_('path "[" INT "]"')  # noqa: 821
    def path(self, p):  # noqa: 811
        path = p.path
        INT = int(p.INT)
        return lambda m: path(m)[INT] if isinstance(path(m), list) and len(path(m)) >= INT else None

    @_('path "[" FLOAT "]"')  # noqa: 821
    def path(self, p):  # noqa: 811
        path = p.path
        FLOAT = float(p.FLOAT)
        return (
            lambda m: path(m)[FLOAT]
            if isinstance(path(m), list) and len(path(m)) >= FLOAT
            else None
        )

    @_("INT")  # noqa: 821
    def operand(self, o):  # noqa: 811
        INT = int(o.INT)
        return lambda m: INT

    @_("FLOAT")  # noqa: 821
    def path(self, p):  # noqa: 811
        FLOAT = float(p.FLOAT)
        return lambda m: FLOAT

    @_("NEW_IMAGE")  # noqa: 821
    def path(self, _):  # noqa: 811
        return lambda m: m.NewImage

    @_("OLD_IMAGE")  # noqa: 821
    def path(self, _):  # noqa: 811
        return lambda m: m.OldImage

    @_("NAME")  # noqa: 821
    def path(self, p):  # noqa: 811
        NAME = p.NAME
        return lambda m: m.get(NAME) if p(m) is not None else None

    @_('CHANGED "(" in_list ")"')  # noqa: 821
    @_('CHANGED "(" VALUE ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        # 1. Key is not in both dicts
        # 2. Key is in one and not the other
        # 3. Key is in both but the items differ
        if hasattr(p, "in_list"):
            key_list = p.in_list(p)
        else:
            key_list = [self.strip_quotes(p.VALUE)]

        def has_changed(record, keys=key_list):
            for k in keys:
                if (
                    k not in self.old and k in self.new
                    or k not in self.new and k in self.old
                    or k in self.new and k in self.old and self.old[k] != self.new[k]
                ):
                    return True

            return False

        return has_changed

    @_('IS_TYPE "(" path "," NAME ")"')  # noqa: 821
    def function(self, p):  # noqa: 811
        path = p.path

        TYPE_MAP = {
            "S": lambda m: isinstance(path(m), str),
            "L": lambda m: is_l(path(m)),
            "SS": lambda m: is_ss(path(m)),
            "BS": lambda m: is_bs(path(m)),
            "NS": lambda m: is_ns(path(m)),
            "M": lambda m: isinstance(path(m), dict),
            "B": lambda m: isinstance(path(m), bytes),
            "NULL": lambda m: path(m) is None,
            "BOOL": lambda m: isinstance(path(m), bool),
        }

        if p.NAME not in TYPE_MAP:
            raise TypeError(f"Unknown type '{p.NAME}'")

        return TYPE_MAP[p.NAME]