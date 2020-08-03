#!/bin/bash
ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
if [ "$ver" -lt "30" ]; then
    echo "This script requires python 3 or greater"
    exit 1
fi
source ./venv/bin/activate
python3 migrate.py $1