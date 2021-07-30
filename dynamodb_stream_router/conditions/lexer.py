# flake8: noqa
# pyright: reportUndefinedVariable=false
from sly import Lexer


class ExpressionLexer(Lexer):
    # Set of token names.
    tokens = {
        NAME,
        VALUE,
        INT,
        FLOAT,
        BETWEEN,
        AND,
        OR,
        NOT,
        IN,
        EQ,
        NE,
        GT,
        GTE,
        LT,
        LTE,
        ATTRIBUTE_EXISTS,
        ATTRIBUTE_NOT_EXISTS,
        ATTRIBUTE_TYPE,
        BEGINS_WITH,
        CONTAINS,
        SIZE,
        OLD_IMAGE,
        NEW_IMAGE,
        CHANGED,
        MATCH,
        IS_TYPE,
        FALSE,
        TRUE,
        IS
    }

    # Set of literal characters
    literals = {"(", ")", "[", "]", ",", "."}

    # String containing ignored characters
    ignore = " \t"

    # Regular expression rules for tokens
    OLD_IMAGE = r"\$OLD"
    NEW_IMAGE = r"\$NEW"
    VALUE = r""""([^"\\]*(\\.[^"\\]*)*)"|\'([^\'\\]*(\\.[^\'\\]*)*)\'"""
    AND = r"\&"
    OR = r"\|"
    NOT = "NOT"
    IN = "IN"
    IS = "IS"
    BETWEEN = "BETWEEN"
    CHANGED = "has_changed"
    IS_TYPE = "is_type"
    ATTRIBUTE_EXISTS = r"attribute_exists"
    ATTRIBUTE_NOT_EXISTS = r"attribute_not_exists"
    ATTRIBUTE_TYPE = r"attribute_type"
    BEGINS_WITH = r"begins_with"
    CONTAINS = r"contains"
    SIZE = r"size"
    TRUE = "True"
    FALSE = "False"

    """
    NAME has to come AFTER any keywords above. NAME is used as a path within OLD_IMAGE/NEW_IMAGE
    and also Dynamodb types such as S, L, SS, NS, BOOL, etc...
    """
    NAME = r"[a-zA-Z_][a-zA-Z0-9\-_]*"
    NE = r"!="
    GTE = r">="
    LTE = r"<="
    EQ = r"=="
    GT = r">"
    LT = r"<"
    INT = r"\d+"
    MATCH = r"=~"
    FLOAT = r"\d+\.\d+"

    # Line number tracking
    @_(r"\n+")
    def ignore_newline(self, t):
        self.lineno += t.value.count("\n")

    def error(self, t):
        print("Line %d: Bad character %r" % (self.lineno, t.value[0]))
        self.index += 1
