#!/bin/bash

cd "$(dirname "$0")"
source ./.venv/bin/activate
source ./.env.local

export PYTHONPATH=/Users/shibuki/Documents/GTFS_delay/app:$PYTHONPATH

if [ "$1" = "update_gtfs_rt" ]; then
    python3 ./app/src/update_gtfs_rt.py $2
elif [ "$1" = "update_gtfs" ]; then
    python3 ./app/src/update_gtfs.py
elif [ "$1" = "calc_delay" ]; then
    python3 ./app/src/calc_delay.py
elif [ "$1" = "remove_outlier" ]; then
    python3 ./app/src/remove_outlier.py
else
    echo "コマンドが不明です"
fi

deactivate