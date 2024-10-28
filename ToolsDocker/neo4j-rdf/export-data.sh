#!/bin/bash

# Exit on error, undefined variables, and pipe failures
set -euo pipefail
trap 'error_handler $? $LINENO $BASH_LINENO "$BASH_COMMAND" $(printf "::%s" ${FUNCNAME[@]:-})' ERR

# Constants
readonly DATADIR="/mnt/repo"
readonly LOG_FILE="${DATADIR}/neo4j-logs/conversion.log"
readonly MAX_RETRIES=3
readonly STARTUP_TIMEOUT=60  # seconds

# Logging functions
log() {
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${timestamp}] $*" | tee -a "${LOG_FILE}"
}

error() {
    log "ERROR: $*" >&2
}

# Error handler
error_handler() {
    local exit_code=$1
    local line_no=$2
    error "Error occurred in script at line: ${line_no}, exit code: ${exit_code}"
    cleanup
    exit "${exit_code}"
}

# Check required commands
check_dependencies() {
    local deps=("neo4j-admin" "cypher-shell" "neo4j")
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            error "Required command not found: $cmd"
            exit 1
        fi
    done
}

# Cleanup function with safety checks
cleanup() {
    log "Starting cleanup..."
    if [[ -d "${DATADIR}/neo4j-data" ]]; then
        rm -rf "${DATADIR}/neo4j-data" || error "Failed to remove neo4j-data directory"
    fi
    log "Cleanup completed"
}

# Create directories with proper permissions
setup_directories() {
    local dirs=("neo4j-data" "neo4j-logs" "neo4j-export")

    for dir in "${dirs[@]}"; do
        local full_path="${DATADIR}/${dir}"
        if ! mkdir -p "${full_path}"; then
            error "Failed to create directory: ${full_path}"
            exit 1
        fi
        if ! chmod 777 -R "${full_path}"; then
            error "Failed to set permissions for: ${full_path}"
            exit 1
        fi
    done
}

# Wait for Neo4j to start
wait_for_neo4j() {
    local timeout=$1
    local start_time=$(date +%s)

    log "Waiting for Neo4j to start..."
    while true; do
        if neo4j status | grep -q "Neo4j is running"; then
            log "Neo4j has started successfully"
            return 0
        fi

        local current_time=$(date +%s)
        if ((current_time - start_time > timeout)); then
            error "Timeout waiting for Neo4j to start"
            return 1
        fi

        sleep 5
    done
}

# Main process
main() {
    # Validate arguments
    if [[ "$#" -ne 1 ]]; then
        error "Usage: $0 <neo4j-dump-file>"
        exit 1
    fi

    local dump_file=$1

    # Validate input file
    if [[ ! -f "${DATADIR}/${dump_file}" ]]; then
        error "Dump file not found: ${DATADIR}/${dump_file}"
        exit 1
    fi

    # Initialize logging
    mkdir -p "$(dirname "${LOG_FILE}")"
    log "Starting conversion process..."

    # Check dependencies
    check_dependencies

    # Setup working directory
    WORK_DIR=$(mktemp -d "${DATADIR}/neo4j-work.XXXXXX")
    log "Created working directory: ${WORK_DIR}"

    # Clean up any existing data
    cleanup

    # Setup directories
    setup_directories

    # Copy dump file
    log "Copying dump file..."
    if ! cp "${DATADIR}/${dump_file}" "${WORK_DIR}/neo4j.dump"; then
        error "Failed to copy dump file"
        exit 1
    fi


    nohup /startup/docker-entrypoint.sh neo4j start > "${DATADIR}/neo4j-logs/startup.log" 2>&1 &

    # Wait for Neo4j to start
    if ! wait_for_neo4j "${STARTUP_TIMEOUT}"; then
        error "Neo4j failed to start"
        exit 1
    fi

    sleep 10

    echo "Node count in graph: ---------"
    echo "MATCH (c) return count(c)" | cypher-shell --format plain

    neo4j stop

    # Load database
    du -h ${DATADIR}/neo4j-data
    echo "loading"
    ls -alh ${WORK_DIR}/neo4j.dump
    neo4j-admin database load --from-path="${WORK_DIR}/" --overwrite-destination=true neo4j
    sleep 1
    chmod 777 -R ${DATADIR}/neo4j-data
    du -h ${DATADIR}/neo4j-data
    # make data dir readable


    # Start Neo4j
    log "Starting Neo4j..."
    nohup /startup/docker-entrypoint.sh neo4j start > "${DATADIR}/neo4j-logs/startup.log" 2>&1 &

    # Wait for Neo4j to start
    if ! wait_for_neo4j "${STARTUP_TIMEOUT}"; then
        error "Neo4j failed to start"
        exit 1
    fi

    sleep 10

    echo "Node count in graph: ---------"
    echo "MATCH (c) return count(c)" | cypher-shell --format plain

    # Export data
    log "Exporting data..."
    local export_command="CALL apoc.export.json.all(\"${DATADIR}/neo4j-export/neo4j-apoc-export.json\",{jsonFormat:\"JSON_LINES\",writeNodeProperties:true});"

    if ! echo "${export_command}" | cypher-shell --format plain > "${DATADIR}/neo4j-export/stats.txt"; then
        error "Failed to export data"
        exit 1
    fi

    # Stop Neo4j
    log "Stopping Neo4j..."
    if ! neo4j stop; then
        error "Failed to stop Neo4j"
        exit 1
    fi

    # Final cleanup
#    cleanup

    log "Conversion completed successfully"
}

# Run main function
main "$@"




##!/bin/bash
#
#set -e
#
#DATADIR=/mnt/repo
#
## check and continue if only a single reference
## remove old data
#cleanup() {
#  rm -rf ${DATADIR}/neo4j-data
#}
#
#cleanup
#
#
#if [ "$#" -ne 1 ]; then
#    echo "Script expects single neo4j dump file"
#    exit 1
#fi
#
## setup loading dir with data
#
#FILE_TO_CONVERT=$@
#WORK_DIR="${DATADIR}/work-dir"
#
#rm -rf $WORK_DIR
#mkdir -p $WORK_DIR
#cp $DATADIR/${FILE_TO_CONVERT} $WORK_DIR/neo4j.dump
#
#mkdir -p ${DATADIR}/neo4j-data
#chmod 777 -R ${DATADIR}/neo4j-data
#mkdir -p ${DATADIR}/neo4j-logs
#chmod 777 -R ${DATADIR}/neo4j-logs
#mkdir -p ${DATADIR}/neo4j-export
#chmod 777 -R ${DATADIR}/neo4j-export
#
#
## load to database
#neo4j-admin database load neo4j --from-path=$WORK_DIR --overwrite-destination=true
#chmod 777 -R ${DATADIR}/neo4j-data
#
## start neo4j
#nohup /startup/docker-entrypoint.sh neo4j start > ${DATADIR}/neo4j-logs/startup.log &
#
#
#sleep 30
#
#
#
#echo "CALL apoc.export.json.all(\"${DATADIR}/neo4j-export/neo4j-apoc-export.json\",{jsonFormat:\"JSON_LINES\",writeNodeProperties:true});" | cypher-shell --format plain > ${DATADIR}/neo4j-export/stats.txt
#
## stop neo4j
#neo4j stop
#
## clean up data dir
#cleanup
#
