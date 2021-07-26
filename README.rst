dynamodb-stream-router
======================


Provies a router framework for mapping records in a Dynamodb stream to callables based on the event name (UPDATE, INSERT, DELETE) and/or content
-------------------------------------------------------------------------------------------------------------------------------------------------

Features:
    - Register functions/methods using decorators
    - Assign functions/methods to be called for specific db operations
    - Filter routes to call on a record using a conditional expression or custom function
    - Conditional expressions are parsed using a custom grammar lexer/parser writtern with `sly`, so they are really, really fast
    - Route return values include all the information about the execution of that route for debugging


Full documentation available at https://quinovas.github.io/dynamodb-stream-router

Example Usage:

.. highlight:: python
.. code-block:: python

    from dynamodb_stream_router.router import Record, StreamRouter

    router = StreamRouter()

    records = [{
        "StreamViewType": "NEW_AND_OLD_IMAGES",  # Only NEW_AND_OLD_IMAGES are supported
        "eventName": "UPDATE",
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

.. highlight:: python
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

Routes can be registered to be called either for all records whose operation matches the record (UPDATE, DELETE, INSERT) or include a
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

Keywords:
*********

+----------+-------------------------------------------------------+-------------------------------------+
| **Type** |                    **Description**                    |             **Example**             |
+----------+-------------------------------------------------------+-------------------------------------+
| VALUE    | A quoted string (single or double quote), integer, or | 'foo', 1, 3.8                       |
|          | float representing a literal value                    |                                     |
+----------+-------------------------------------------------------+-------------------------------------+
| $OLD     | A reference to StreamRecord.OldImage                  | $OLD.foo                            |
+----------+-------------------------------------------------------+-------------------------------------+
| $NEW     | A reference to StreamRecord.NewImage                  | $NEW.foo                            |
+----------+-------------------------------------------------------+-------------------------------------+
| PATH     | A path starting from a root of $OLD or $NEW.          | $OLD.foo, $NEW.foo.bar, $OLD["foo"] |
|          | Can be specified using dot syntax or python           |                                     |
|          | style keys. When using dot reference paths must       |                                     |
|          | conform to python's restrictions                      |                                     |
+----------+-------------------------------------------------------+-------------------------------------+
| INDEX    | An integer used as an index into a list or set        | $OLD.foo[0]                         |
+----------+-------------------------------------------------------+-------------------------------------+


Operators:
**********

+------------+--------------------------------------------+
| **Symbol** |                 **Action**                 |
+------------+--------------------------------------------+
| &          | Logical AND                                |
+------------+--------------------------------------------+
| |          | Logical OR                                 |
+------------+--------------------------------------------+
| ()         | Grouping                                   |
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
| =~         | Regex comparison <value> =~ '<expression>' |
|            | `'<expression>' is a quoted VALUE          |
+------------+--------------------------------------------+


Comparison operators, except for regex comparison, can compare PATH to VALUE, PATH to PATH, or even VALUE to VALUE.


.. list-table:: Functions
    :widths: 20 20 50
    :header-rows: 1

    * - Name
        - Arguments
        - Description
    * - has_changed(VALUE, VALUE)
        - Comma-separated list of quoted values
        - Tests each value to see if that key in the top level of $OLD differs from $NEW. Returns True if any of the elements have changed
    * - is_type(PATH, TYPE)
        - PATH - The path to a value to test and the Dynamodb type to test for, TYPE - Any Dynamodb Type
        - Returns if PATH is of type TYPE. TYPE can be any unquoted Dynamodb type (S, SS, B, BS, N, NS, M, BOOL, L)
    * - attribute_exists(PATH)
        - PATH - An element to test the existence of
        - Returns a bool indicating if the specified key/index exists in PATH