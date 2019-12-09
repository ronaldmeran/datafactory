#!/bin/bash

source datafactory.sh

# create data factory
initialize_datafactory() {
    python3 initialize_datafactory.py $DATAFACTORY_NAME $RESOURCE_GROUP_NAME $RESOURCE_GROUP_LOCATION >> /tmp/error.log 2>&1
}

cd ../datafactory
initialize_datafactory
