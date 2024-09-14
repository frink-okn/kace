#!/bin/bash

set -e

export WORKING_DIR=/mnt/repo

# Check if at least one file is provided
if [ "$#" -eq 0 ]; then
    echo "No files provided. Usage: ./convert-all.sh file1 file2 ..."
    exit 1
fi

export RIOT_TMP_DIR=$WORKING_DIR/riot-tmp/
export HDT_TMP_DIR=$WORKING_DIR/hdt-tmp/
export HDT_FINAL_DIR=$WORKING_DIR/hdt/

rm -rf $RIOT_TMP_DIR/*
rm -rf $HDT_TMP_DIR/*
rm -rf $HDT_FINAL_DIR/*

mkdir -p ${RIOT_TMP_DIR}
FILES=()
for file in "$@"; do
    file_full_path="${WORKING_DIR}/${file}"
    FILES+=("$file_full_path")
done

riot --debug  --nocheck -v --output TURTLE ${FILES[@]} > ${RIOT_TMP_DIR}/combined.ttl

python3 /bin/process_graph.py ${RIOT_TMP_DIR}/

mkdir -p ${HDT_TMP_DIR}
mkdir -p ${HDT_FINAL_DIR}

rdf2hdt.sh -index -quiet ${RIOT_TMP_DIR}/combined.ttl ${HDT_FINAL_DIR}/graph.hdt

rm -rf ${RIOT_TMP_DIR}
rm -rf ${HDT_TMP_DIR}


