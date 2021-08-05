dynamodb-stream-router  (Alpha release)
=======================================


Provies a framework for mapping records in a Dynamodb stream to callables based on the event name (MODIFY, INSERT, DELETE) and content
---------------------------------------------------------------------------------------------------------------------------------------

Features:
    - Register functions/methods using decorators
    - Assign functions/methods to be called for specific db operations
    - Filter routes to call on a record using a conditional expression or custom function
    - Conditional expressions are parsed using a custom grammar lexer/parser written with `sly`, so they are really, really fast
    - Route return values include all the information about the execution of that route for debugging
    - Matching Routes for a record can be prioritized
    - Supports decoding values stored as JSON strings
    - Route resolution for a record can be stopped by either arguments when the route is registered or by returning `dynamodb_stream_router.router.Halt()`


Full API documentation available at https://quinovas.github.io/dynamodb-stream-router

Example Usage:
**************

.. code-block:: python

    from dynamodb_stream_router.router import Record, StreamRouter

    router = StreamRouter()

    records = [{
        "StreamViewType": "NEW_AND_OLD_IMAGES",  # Only NEW_AND_OLD_IMAGES are supported
        "eventName": "MODIFY",
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
                "type": {"S": "sometype"}
            }
        }
    }]


    @router.update(condition_expression="has_changed('type')")
    def my_first_route(record):
        return True


    res = router.resolve_all(records)
    print([
        x.value for x in res
    ])

    # prints '[True]'


In the example above the function *my_first_route()* will be called because *record.OldImage["type"]* has changed in comparison to *record.NewImage["type"].
This example uses `dynamodb_stream_router.conditions.Expression`_ as the condition_expression used to match the route to the record. In addition to passing
a string-based expression you could pass your own callable, for instance a lambda, that accepts *record* as its only required argument and returns a bool
indicating whether or not the route matches.

Example using a lambda as condition_expression:

.. code-block:: python

    from dynamodb_stream_router.router import StreamRouter

    router = StreamRouter()


    @router.update(condition_expression=lambda x: x.OldImage["type"] != x.NewImage["type"])
    def my_first_route(record):
        return True


    res = router.resolve_all(records)
    print([
        x.value for x in res
    ])

    # prints '[True]'


Expressions
-----------

Routes can be registered to be called either for all records whose operation matches the record (MODIFY, DELETE, INSERT) or include a
conditional_expression argument that decides whether or not the route matches. There are two types of condition_expression:

- Callable:
    * Any function/method/lambda that returns a bool
    * The record currently being parsed is passed as the first and only argument
    * The record is passed as a dynamodb_stream_router.router.Record object
    * If the function returns True then the route's function will be called
- Expression (dynamodb_stream_router.conditions.parser.Expression)
    * A string that will be parsed into a callable using dynamodb_stream_router.conditions.parser.Expression
    * The string uses the query language defined below


Condition query language
-------------------------

Keywords and types:
*******************

+----------+-------------------------------------------------------+-------------------------------------+
| **Type** |                    **Description**                    |             **Example**             |
+----------+-------------------------------------------------------+-------------------------------------+
| `VALUE`  | A quoted string (single or double quote), integer, or | 'foo', 1, 3.8                       |
|          | float representing a literal value                    |                                     |
+----------+-------------------------------------------------------+-------------------------------------+
| $OLD     | A reference to StreamRecord.OldImage                  | $OLD.foo                            |
+----------+-------------------------------------------------------+-------------------------------------+
| $NEW     | A reference to StreamRecord.NewImage                  | $NEW.foo                            |
+----------+-------------------------------------------------------+-------------------------------------+
| `PATH`   | A path starting from a root of $OLD or $NEW.          | $OLD.foo, $NEW.foo.bar, $OLD["foo"] |
|          | Can be specified using dot syntax or python           |                                     |
|          | style keys. When using dot reference paths must       |                                     |
|          | conform to python's restrictions                      |                                     |
+----------+-------------------------------------------------------+-------------------------------------+
| `INDEX`  | An integer used as an index into a list or set        | $OLD.foo[0]                         |
+----------+-------------------------------------------------------+-------------------------------------+


Operators:
**********

+------------+--------------------------------------------+
| **Symbol** |                 **Action**                 |
+------------+--------------------------------------------+
| &          | Logical AND                                |
+------------+--------------------------------------------+
| \|         | Logical OR                                 |
+------------+--------------------------------------------+
| ()         | Statement grouping                         |
+------------+--------------------------------------------+
| ==         | Equality                                   |
+------------+--------------------------------------------+
| !=         | Non equality                               |
+------------+--------------------------------------------+
| >          | Greater than                               |
+------------+--------------------------------------------+
| >=         | Greater than or equal to                   |
+------------+--------------------------------------------+
| <          | Less than                                  |
+------------+--------------------------------------------+
| <=         | Less than or equal to                      |
+------------+--------------------------------------------+
| =~         | Regex comparison <PATH> =~ '<expression>'  |
|            | where *'<expression>'* is a quoted VALUE   |
+------------+--------------------------------------------+


Comparison operators, except for regex comparison, can compare PATH to VALUE, PATH to PATH, or even VALUE to VALUE.


+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+
|          **Name**         |                      **Arguments**                     | **Description**                                                                    |
+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+
| has_changed(VALUE, VALUE) | VALUE - Comma separated list of quoted values          | Tests $OLD and $NEW. If value is in one and not the other, or in both and differs, |
|                           |                                                        | the the function will return True. Returns True if any key meets conditions.       |
+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+
| is_type(PATH, TYPE)       |  - PATH - The path to test in the form of $OLD.foo.bar | Tests if PATH exists and the VALUE at PATH is of type TYPE.                        |
|                           |  - TYPE - A Dynamodb type. Can be one of S, SS, B, BS, |                                                                                    |
|                           |    N, NS, L, M, or BOOL                                |                                                                                    |
+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+
| attribute_exists(PATH)    | PATH - The path to test                                | Returns True if the provided path exists                                           |
+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+
| from_json(PATH)           | PATH - The path to decode                              | Returns object decoded using simplejson.loads()                                    |
+---------------------------+--------------------------------------------------------+------------------------------------------------------------------------------------+


Example testing an expression directly:
***************************************

.. code-block:: python

    from dynamodb_stream_parser.conditions.parser import Expression
    from dynamodb_stream_router.router import StreamRouter, Record


    router = StreamRouter(threaded=True)

    item = {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "eventName": "MODIFY",
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
                "type": {"S": "sometype"}
            }
        }
    }

    parser = Expression()
    exp = "$NEW.type == 'sometype' & has_changed('type')"
    res = exp.evaluate(exp, record=Record(item))
    print(exp.evaluate())
    # Prints 'True'


    ''' Using an expression with StreamRouter '''
    from dynamodb_stream_parser.conditions.parser import Expression
    from dynamodb_stream_router.router import StreamRouter, Record

    router = StreamRouter()
    exp = "$NEW.type == 'sometype' & has_changed('type')


    @router.update(condition_expression=exp)
    def func_name(item):
        return 1


    records = [StreamRecord(item)]

    res = router.resolve_all(items)
    print([x.value for x in res])

    # prints '[1]'


Feature Roadmap
---------------

- Pre/post hooks
- Automatic importing of decorated callables from packages
- Shell scripts for testing Lambda locally
- Class for creating fan outs
- BETWEEN keyword
- contains(PATH | VALUE, PATH | VALUE) function
- startswith(PATH, VALUE) function
- endswith(PATH, VALUE) function
- from_json(PATH) function
- NOT keyword
- bitwise operators for binary types