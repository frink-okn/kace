#!/usr/bin/env bash
# convert-to-nt.sh
# Simplified RDF→NT.GZ converter (HDT logic removed)

set -o errexit
set -o nounset
set -o pipefail

# --- Config ---
WORKING_DIR=${WORKING_DIR:-/mnt/repo}
RIOT_TMP_DIR=${RIOT_TMP_DIR:-${WORKING_DIR}/riot-tmp}
NT_FINAL_DIR=${NT_FINAL_DIR:-${WORKING_DIR}/nt}
REPORT_DIR=${REPORT_DIR:-${WORKING_DIR}/report}
JAVATMP_DIR=${JAVATMP_DIR:-${WORKING_DIR}/java-temp}
HDT_TMP_DIR=${HDT_TMP_DIR:-${WORKING_DIR}/hdt-tmp}
RIOT_BIN=${RIOT_BIN:-riot}
KEEP_TEMP=${KEEP_TEMP:-0}
REPO_NAME=${REPO_NAME:-'Unknown'}
COMMIT_ID=${COMMIT_ID:-""}
KG_NAME=${KG_NAME:-""}
GH_HANDLES=${GH_HANDLES:-""}

JAVA_OPTIONS="${JAVA_OPTIONS:-} -Djava.io.tmpdir=${JAVATMP_DIR}"
export JAVA_OPTIONS

FAILURES_LOG=${REPORT_DIR}/failures.log
VALIDATE_LOG=${REPORT_DIR}/riot_validate.log

mkdir -p "${RIOT_TMP_DIR}" "${NT_FINAL_DIR}" "${REPORT_DIR}" "${JAVATMP_DIR}" "${HDT_TMP_DIR}"

log() { printf '%s %s\n' "$(date --iso-8601=seconds)" "$*" >&2; }
err_and_exit() { echo "$*" >&2; echo "$(date --iso-8601=seconds) - $*" >> "${FAILURES_LOG}"; exit 1; }

create_github_issue() {
    local title="$1"
    local body_file="$2"
    local repo="$3"
    local label="${4:-graph-validation}"

    # Base command
    local cmd=(gh issue create \
        --title "${title}" \
        --body-file "${body_file}" \
        --label "${label}" \
        --repo "${repo}")

    # Add assignees if GH_HANDLES is set and non-empty
    if [[ -n "${GH_HANDLES:-}" ]]; then
        cmd+=(--assignee "${GH_HANDLES}")
    fi

    "${cmd[@]}"
}



cleanup() {
    status=$?
    # FIX: Make cleanup conditional
    if [ "${KEEP_TEMP}" != "1" ]; then
        rm -rf "${RIOT_TMP_DIR}" "${JAVATMP_DIR}" "${HDT_TMP_DIR}"
    else
        log "Keeping temporary files (KEEP_TEMP=1)"
    fi
    if [ $status -ne 0 ]; then
        echo "Script failed with exit code $status. See ${FAILURES_LOG}" >&2
    fi
}
trap cleanup EXIT

# --- File detection ---
FILES=()
if [ "$#" -gt 0 ]; then
    for f in "$@"; do
        [[ "$f" = /* ]] && FILES+=("$f") || FILES+=("${WORKING_DIR}/${f}")
    done
else
    while IFS= read -r -d $'\0' file; do FILES+=("$file"); done < <(
        find "${WORKING_DIR}" -maxdepth 2 -type f \( -iname "*.ttl" -o -iname "*.nt" -o -iname "*.nq" -o -iname "*.rdf" -o -iname "*.gz" -o -iname "*.bz2" \) -print0
    )
fi

[ ${#FILES[@]} -eq 0 ] && err_and_exit "No RDF files found."

TEXT_FILES=()
HDT_FILES=()

decompress_file() {
    local src="$1"
    local dest="${RIOT_TMP_DIR}/$(basename "$src" | sed -E 's/(\.gz|\.bz2)$//')"
    if [[ "$src" == *.gz ]]; then
        gunzip -c "$src" > "$dest" || err_and_exit "Failed to decompress $src"
    elif [[ "$src" == *.bz2 ]]; then
        bzip2 -dc "$src" > "$dest" || err_and_exit "Failed to decompress $src"
    fi
    echo "$dest"
}

for f in "${FILES[@]}"; do
    [ ! -f "$f" ] && { log "Skipping missing file: $f"; continue; }
    lower=$(echo "$f" | tr '[:upper:]' '[:lower:]')
    case "$lower" in
        *.gz|*.bz2)
            inner=$(basename "$f" | sed -E 's/(\.gz|\.bz2)$//')
            if [[ "$inner" == *.hdt ]]; then
                tmp_hdt="${HDT_TMP_DIR}/$inner"
                gunzip -c "$f" > "$tmp_hdt" || err_and_exit "Failed decompressing $f"
                HDT_FILES+=("$tmp_hdt")
            else
                TEXT_FILES+=("$(decompress_file "$f")")
            fi;;
        *.hdt)
            HDT_FILES+=("$f");;
        *.ttl|*.rdf|*.nt|*.nq)
            TEXT_FILES+=("$f");;
        *)
            log "Unknown extension, treating as RDF text: $f"
            TEXT_FILES+=("$f");;
    esac
done

log "Total text files to process: ${#TEXT_FILES[@]}"

# --- Processing Functions ---

merge_text_to_nt() {
    local out_nt="${RIOT_TMP_DIR}/combined_text.nt"
    [ ${#TEXT_FILES[@]} -eq 0 ] && return 1

    log "Merging ${#TEXT_FILES[@]} RDF files..."
    if ! ${RIOT_BIN} --merge --nocheck --output NT "${TEXT_FILES[@]}" > "${out_nt}" 2> "${REPORT_DIR}/riot_merge.log"; then
        log "ERROR: riot merge failed, see ${REPORT_DIR}/riot_merge.log"

        local repo="frink-okn/graph-coordination"
        local title="RDF Merge failed for ${REPO_NAME}"
        local body_file="/tmp/gh_issue_body_merge.txt"

        {
            echo "RDF merge failed for **${REPO_NAME}**"
            echo
            echo "**Commit:** ${COMMIT_ID:-unknown}"
            echo "**KG:** ${KG_NAME:-unknown}"
            echo
            echo "### Error Log (last 100 lines)"
            echo '```'
            tail -n 100 "${REPORT_DIR}/riot_merge.log"
            echo '```'
        } > "${body_file}"

        create_github_issue "${title}" "${body_file}" "${repo}"
        err_and_exit "RDF merge failed — issue created on GitHub."
    fi

    if [ ! -s "${out_nt}" ]; then
        log "Warning: Merged output is empty."
        return 1
    fi

    echo "${out_nt}"
}

validate_nt() {
    local ntfile="$1"
    local repo="frink-okn/graph-coordination"
    local title="Validation failed for ${REPO_NAME}"
    local body_file="/tmp/gh_issue_body.txt"

    log "Validating ${ntfile}"
    ${RIOT_BIN} --validate --check --strict --sink "${ntfile}" > "${VALIDATE_LOG}" 2>&1

    if grep -iq "error" "${VALIDATE_LOG}"; then
        log "ERROR: Validation failed for ${KG_NAME}"
        {
            echo "Validation failed for ${REPO_NAME} commit: ${COMMIT_ID} "
            echo "### Error Log"
            echo '```'
            cat "${VALIDATE_LOG}"
            echo '```'
        } > "${body_file}"

        create_github_issue "${title}" "${body_file}" "${repo}"
        return 1
    fi
    log "Validation passed."
}

# --- Main execution ---
final_ntgz="${NT_FINAL_DIR}/graph.nt.gz"

log "========================================="
log "Starting RDF to NT.GZ merge process"
log "========================================="

merged_nt=""

if [ ${#TEXT_FILES[@]} -gt 0 ] && [ ${#HDT_FILES[@]} -gt 0 ]; then
    log "Converting ${#HDT_FILES[@]} HDT files to NT to participate in merge..."
    for hdt in "${HDT_FILES[@]}"; do
        extracted_nt="${RIOT_TMP_DIR}/$(basename "$hdt").nt"
        hdt2rdf.sh "$hdt" "$extracted_nt" 2>> "${REPORT_DIR}/hdt2rdf.log"
        TEXT_FILES+=("$extracted_nt")
    done
    merged_nt=$(merge_text_to_nt || err_and_exit "Merge of text files failed.")
elif [ ${#TEXT_FILES[@]} -gt 0 ]; then
    merged_nt=$(merge_text_to_nt || err_and_exit "No valid triples found or merge failed.")
elif [ ${#HDT_FILES[@]} -gt 0 ]; then
    if [ ${#HDT_FILES[@]} -eq 1 ]; then
        log "Exporting single HDT to NT..."
        merged_nt="${RIOT_TMP_DIR}/hdt_extracted.nt"
        hdt2rdf.sh "${HDT_FILES[0]}" "${merged_nt}" 2>> "${REPORT_DIR}/hdt2rdf_final.log"
    else
        log "Converting ${#HDT_FILES[@]} HDT files to NT for merging..."
        for hdt in "${HDT_FILES[@]}"; do
            extracted_nt="${RIOT_TMP_DIR}/$(basename "$hdt").nt"
            hdt2rdf.sh "$hdt" "$extracted_nt" 2>> "${REPORT_DIR}/hdt2rdf.log"
            TEXT_FILES+=("$extracted_nt")
        done
        merged_nt=$(merge_text_to_nt || err_and_exit "Merge of HDT extracted files failed.")
    fi
else
    err_and_exit "No valid files found."
fi

if [ ! -s "${merged_nt}" ]; then
    err_and_exit "Validation failed: merged NT file is empty."
fi

if ! validate_nt "${merged_nt}"; then
    err_and_exit "Validation failed for merged NT file."
fi

log "Compressing final output..."
gzip -c "${merged_nt}" > "${final_ntgz}"

log "✓ Process Complete: ${final_ntgz}"