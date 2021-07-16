#!/bin/bash

sphinx-build -a -c . ../dynamodb_stream_router/ build/
cp -r build/* .
