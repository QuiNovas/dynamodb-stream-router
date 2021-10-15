# dynamodb-stream-router

> **WARNING - Version 0.0.6 is a breaking change from version 0.0.5. Please review the documentation before upgrading**

Provies a framework for mapping records in a Dynamodb stream to callables based on the event name (MODIFY, INSERT, DELETE) and content.

## Installation
```bash
pip install dynamodb-stream-router
```

## Routing
- Routes are determined by examining the `eventName` in the `dynamodb` section of the DynamoDB stream [`Record`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html) and through a `condition` which examines the contents of the `Record`.
- Conditions can either be a callable that takes a `RouteRecord` and returns `True` or `False`, or it may be a string expression that will be parsed into a callable. See below for the expression language.
- Routes have a priority, which is honored in an acending order. If multiple matching routes have the same priority, they will be executed in random order (concurrectly, if an `executor` is provided)
- Routes are matched based on a `RouteRecord`, which is a helper class that (lazily) deserializers the DynamoDB item structure (used in `Keys`, `NewImage` and `OldImage`) into Python types, exactly in the same way that the boto3 dynamodb Table resource does.
- Route handling functions take a `RouteRecord` and can return anything. The return value is not used by the framework.

## Expressions

### Keywords and types:
| Type | Description | Example |
|------|-------------|---------|
| VALUE | A quoted string (single or double quote), integer, or float representing a literal value | 'foo', 1, 3.8  |
| $OLD | A reference to `RouteRecord.old_image` | $OLD.foo |
| $NEW | A reference to `RouteRecord.old_image` | $NEW.foo |
| PATH | A path starting from a root of $OLD or $NEW. Can be specified using dot syntax or python style keys. When using dot reference paths must conform to python's restrictions. | $OLD.foo, $NEW.foo.bar, $OLD["foo"] |
| INDEX | An integer used as an index into a list or set | $OLD.foo[0] |

### Operators:
| Symbol | Action |
|--------|--------|
| & | Logical AND |
| \| | Logical OR |
| () | Statement grouping |
| == | Equality |
| != | Non equality |
| > | Greater than |
| >= | Greater than or equal to |
| < | Less than |
| <= | Less than or equal to |
| =~ | Regex comparison `PATH` =~ 'regex' where *'regex'* is a quoted `VALUE` |

Comparison operators, except for regex comparison, can compare `PATH` to `VALUE`, `PATH` to `PATH`, or even `VALUE` to `VALUE`.

### functions
| Function | Arguments | Description |
|----------|-----------|-------------|
| has_changed(VALUE, VALUE) | VALUE - Comma separated list of quoted values | Tests $OLD and $NEW. If value is in one and not the other, or in both and differs, the the function will return True. Returns True if any key meets conditions. |
| is_type(PATH, TYPE) | <ul><li>PATH - The path to test in the form of $OLD.foo.bar</li<li> TYPE - A Dynamodb type. Can be one of S, SS, B, BS, N, NS, L, M, or BOOL</li></ul> | Tests if PATH exists and the VALUE at PATH is of type TYPE. |
| attribute_exists(PATH) | PATH - The path to test | Returns True if the provided path exists |
| from_json(PATH) | PATH - The path to decode | Returns object decoded using simplejson.loads() |


## Examples

```python
from dynamodb_stream_router import on_insert, on_modify, on_remove, on_operations, Operation, route_records, RouteRecord

@on_insert("$NEW.foo == 'bar'", 0)
def print_new_record(record: RouteRecord) -> None:
    print(record.new_image)

def test_old_foo(record: RouteRecord) -> bool:
    return record.old_image["foo"] == "bar2"

@on_remove(test_old_foo, 0)
def print_old_record(record: RouteRecord) -> None:
    print(record.old_record)

@on_modify("has_changed('foo') & attribute_exists($NEW.foo)", 1)
def print_changed_foo(record: RouteRecord) -> None:
    print(f'{record.old_image.get("foo")} -> {record.new_image.get("foo")}')

@on_operations({Operation.INSERT, Operation.MODIFY, Operation.REMOVE}, 1)
def hello_world(record: RouteRecord) -> str:
    return "Hello, DB STREAM"

def lambda_handler(event, context):
    route_records(event["Records"])

```