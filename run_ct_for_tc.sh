#!/bin/bash

SUITE_NAME=$1
CWD=`pwd`
RESULT_FILE="${CWD}/test/.out/${SUITE_NAME}.xml"
SUITE_PATH="test/${SUITE_NAME}_SUITE.erl"

echo "Running ct_run for $SUITE_PATH, results going to $RESULT_FILE"
ct_run \
    -noshell \
    -pa ebin apps/*/ebin deps/*/ebin \
    -ct_hooks cth_surefire "[{path,\"${RESULT_FILE}\"}]" \
    -suite $SUITE_PATH \
    -logdir /tmp \
    -boot start_sasl
