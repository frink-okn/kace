#!/bin/bash

set -ex

export WORKING_DIR=${WORKING_DIR:-/mnt/repo}

# Check if at least one file is provided
if [ "$#" -eq 0 ]; then
    echo "No files provided. Usage: ./convert-all.sh file1 file2 ..."
    exit 1
fi

export RIOT_TMP_DIR=$WORKING_DIR/riot-tmp
export HDT_TMP_DIR=$WORKING_DIR/hdt-tmp
export HDT_FINAL_DIR=$WORKING_DIR/hdt
export REPORT_DIR=$WORKING_DIR/report
export JAVATMP_DIR=$WORKING_DIR/java-temp

rm -rf $RIOT_TMP_DIR/*
rm -rf $HDT_TMP_DIR/*
rm -rf $HDT_FINAL_DIR/*
rm -rf $REPORT_DIR/*
rm -rf $JAVATMP_DIR/*

mkdir -p ${RIOT_TMP_DIR}
mkdir -p ${JAVATMP_DIR}

export JAVA_OPTIONS="${JAVA_OPTIONS} -Djava.io.tmpdir=${JAVATMP_DIR}"
echo $JAVA_OPTIONS
# Build an array of files to process.
# For compressed files (.gz or .bz2), uncompress them into RIOT_TMP_DIR and add the uncompressed file.
FILES=()
for file in "$@"; do
    file_full_path="${WORKING_DIR}/${file}"
    if [[ "$file_full_path" == *.gz ]]; then
        uncompressed_file="${RIOT_TMP_DIR}/$(basename "$file_full_path" .gz)"
        echo "Uncompressing $file_full_path to $uncompressed_file"
        gunzip -c "$file_full_path" > "$uncompressed_file"
        FILES+=("$uncompressed_file")
    elif [[ "$file_full_path" == *.bz2 ]]; then
        uncompressed_file="${RIOT_TMP_DIR}/$(basename "$file_full_path" .bz2)"
        echo "Uncompressing $file_full_path to $uncompressed_file"
        bzip2 -dc "$file_full_path" > "$uncompressed_file"
        FILES+=("$uncompressed_file")
    else
        FILES+=("$file_full_path")
    fi
done

echo "Combining files:"
echo "${FILES[@]}"

# Process files: if a single file is provided, convert it to Turtle (if needed)
# for compatibility with rdf2hdt.sh; otherwise merge the files.

riot --merge -v --output NT  ${FILES[@]} > ${RIOT_TMP_DIR}/combined.nt
INPUT_FILE=${RIOT_TMP_DIR}/combined.nt


echo "validating..."
mkdir -p $REPORT_DIR
set +e
riot --validate --check --strict --sink ${INPUT_FILE} > $REPORT_DIR/riot_validate.log 2>&1
set -e
#python3 /bin/process_graph.py ${RIOT_TMP_DIR}/ $REPORT_DIR

mkdir -p ${HDT_TMP_DIR}
mkdir -p ${HDT_FINAL_DIR}

cd ${WORKING_DIR}

rdf2hdt.sh -cattree -cattreelocation ${HDT_TMP_DIR} -index ${INPUT_FILE} ${HDT_FINAL_DIR}/graph.hdt > /dev/null

rm -rf ${RIOT_TMP_DIR}
rm -rf ${HDT_TMP_DIR}
rm -rf ${JAVATMP_DIR}

