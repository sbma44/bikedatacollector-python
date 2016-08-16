#!/usr/bin/env bash

PYTHON="$(dirname $0)/venv/bin/python"

for run in $($PYTHON -m bikedatacollector list); do
    if [ ! -f "data/$run.json" ]; then
	$PYTHON -m bikedatacollector fetch "$run" > data/$run.json
	echo "- retrieved $run"
    else
	echo "- skipping $run, file exists"
    fi
done
