#!/bin/bash

source datafactory.sh

# rebuild indexes
rebuild_indexes() {
    python3 rebuild_indexes.py $DATAFACTORY_NAME $RESOURCE_GROUP_NAME $RESOURCE_GROUP_LOCATION >> /tmp/pipeline_error.log 2>&1
}

cd ../datafactory
rebuild_indexes
