#!/bin/bash

cd "$(dirname "$0")"
source ./.venv/bin/activate
source ./.env.local

export PYTHONPATH=/Users/shibuki/Documents/GTFS_delay/app:$PYTHONPATH

if [ "$1" = "load_env" ]; then
    echo "環境変数を読み込みました。"
elif [ "$1" = "update_gtfs_rt" ]; then
    python3 ./app/src/update_gtfs_rt.py $2
elif [ -e "./app/src/$1.py" ]; then
    python3 "./app/src/$1.py"
else
    echo "コマンドが不明です"
fi

deactivate