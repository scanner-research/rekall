#!/bin/bash

# Run tests in tests (need to be in the root directory)
if [ "${PWD##*/}" = "scripts" ]; then
    cd ..
fi
python3 -m unittest discover test

