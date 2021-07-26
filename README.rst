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


