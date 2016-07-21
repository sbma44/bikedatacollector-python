#!/usr/bin/env bash

PYTHON="$(dirname $0)/venv/bin/python"

for run in $($PYTHON -m bikedatacollector list); do
    $PYTHON -m bikedatacollector fetch "$run" > data/$run.json
    echo "- retrieved $run"
done