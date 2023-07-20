#!/bin/bash

FILE=$1

if [ ! -f "$FILE" ]; then
    echo "file not exists"
    exit 1
fi

if ! JSON=$(./parser < $1); then 
    echo "execute parser failed"
    exit 1
fi

echo "$JSON" | awk '{system("birdwatcher --olc=\"#connect --etcd "$2":2379 --rootPath="$1",show collections\" | grep \"Collection ID\" | awk '\''{ print \""$1" "$2" "$3" \"$3}'\'' ") }' | awk '{print("birdwatcher --olc=\"#connect --etcd "$2":2379 --rootPath="$1",repair index_metric_type --collection "$3"\"")}'