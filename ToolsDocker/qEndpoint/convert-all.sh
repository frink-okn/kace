#!/usr/bin/env bash
# convert-all-modular.sh (2025-10-09 - FIXED)
# Robust, modular RDF→HDT+NT converter with HDT merge support.

set -o errexit
set -o nounset
set -o pipefail

# --- Config ---
WORKING_DIR=${WORKING_DIR:-/mnt/repo}
RIOT_TMP_DIR=${RIOT_TMP_DIR:-${WORKING_DIR}/riot-tmp}
HDT_TMP_DIR=${HDT_TMP_DIR:-${WORKING_DIR}/hdt-tmp}
HDT_FINAL_DIR=${HDT_FINAL_DIR:-${WORKING_DIR}/hdt}
NT_FINAL_DIR=${NT_FINAL_DIR:-${WORKING_DIR}/nt}
REPORT_DIR=${REPORT_DIR:-${WORKING_DIR}/report}
JAVATMP_DIR=${JAVATMP_DIR:-${WORKING_DIR}/java-temp}
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

mkdir -p "${RIOT_TMP_DIR}" "${HDT_TMP_DIR}" "${HDT_FINAL_DIR}" "${NT_FINAL_DIR}" "${REPORT_DIR}" "${JAVATMP_DIR}"

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
        rm -rf "${RIOT_TMP_DIR}" "${HDT_TMP_DIR}" "${JAVATMP_DIR}"
    else
        log "Keeping temporary files (KEEP_TEMP=1)"
    fi
    if [ $status -ne 0 ]; then
        echo "Script failed with exit code $status. See ${FAILURES_LOG}" >&2
    fi
}
trap cleanup EXIT

log() { printf '%s %s\n' "$(date --iso-8601=seconds)" "$*" >&2; }
err_and_exit() { echo "$*" >&2; echo "$(date --iso-8601=seconds) - $*" >> "${FAILURES_LOG}"; exit 1; }

# --- File detection ---
FILES=()
if [ "$#" -gt 0 ]; then
    for f in "$@"; do
        [[ "$f" = /* ]] && FILES+=("$f") || FILES+=("${WORKING_DIR}/${f}")
    done
else
    while IFS= read -r -d $'\0' file; do FILES+=("$file"); done < <(
        find "${WORKING_DIR}" -maxdepth 2 -type f \( -iname "*.ttl" -o -iname "*.nt" -o -iname "*.nq" -o -iname "*.rdf" -o -iname "*.hdt" -o -iname "*.gz" -o -iname "*.bz2" \) -print0
    )
fi
[ ${#FILES[@]} -eq 0 ] && err_and_exit "No RDF/HDT files found."

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
        *.hdt) HDT_FILES+=("$f");;
        *.gz|*.bz2)
            inner=$(basename "$f" | sed -E 's/(\.gz|\.bz2)$//')
            if [[ "$inner" == *.hdt ]]; then
                tmp_hdt="${HDT_TMP_DIR}/$inner"
                gunzip -c "$f" > "$tmp_hdt" || err_and_exit "Failed decompressing $f"
                HDT_FILES+=("$tmp_hdt")
            else
                TEXT_FILES+=("$(decompress_file "$f")")
            fi;;
        *.ttl|*.rdf|*.nt|*.nq) TEXT_FILES+=("$f");;
        *) log "Unknown extension, treating as RDF text: $f"; TEXT_FILES+=("$f");;
    esac
done

log "Text files: ${#TEXT_FILES[@]} HDT files: ${#HDT_FILES[@]}"

# --- Helper funcs ---
merge_text_to_nt() {
    local out_nt="${RIOT_TMP_DIR}/combined_text.nt"
    log "TEXT_FILES count=${#TEXT_FILES[@]}, content='${TEXT_FILES[*]}'"
    [ ${#TEXT_FILES[@]} -eq 0 ] && return 0

    log "Merging ${#TEXT_FILES[@]} RDF text files..."
    if ! ${RIOT_BIN} --merge --nocheck --output NT "${TEXT_FILES[@]}" > "${out_nt}" 2> "${REPORT_DIR}/riot_merge.log"; then
        log "ERROR: riot merge failed, see ${REPORT_DIR}/riot_merge.log"

        # --- Create GitHub issue on merge failure ---
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
            echo
            echo "_This issue was automatically created by the RDF merge step of the converter script._"
        } > "${body_file}"

        # Require gh CLI to succeed
        create_github_issue "${title}" "${body_file}" "${repo}"
        err_and_exit "GitHub issue creation failed for RDF merge error."

        err_and_exit "RDF merge failed — issue created on GitHub."
    fi

    # Check for empty output
    if [ ! -s "${out_nt}" ]; then
        log "Warning: merged NT is empty (metadata-only or VOID TTL). Skipping text merge."
        rm -f "${out_nt}"
        return 0
    fi

    echo "${out_nt}"

}

validate_nt() {
    local ntfile="$1"
    local repo="frink-okn/graph-coordination"
    local title="Validation failed for ${ntfile}"
    local body_file="/tmp/gh_issue_body.txt"

    log "Validating ${ntfile}"

    # Run validation and capture output
    ${RIOT_BIN} --validate --check --strict --sink "${ntfile}" > "${VALIDATE_LOG}" 2>&1

    # If output contains "ERROR" (case-insensitive), fail and create GitHub issue
    if grep -iq "error" "${VALIDATE_LOG}"; then
        log "ERROR: Validation failed for ${KG_NAME}"

        # Create issue body
        {
            echo "Validation failed for ${REPO_NAME}  commit: ${COMMIT_ID} "
            echo
            echo "### Error Log"
            echo '```'
            cat "${VALIDATE_LOG}"
            echo '```'
            echo
            echo "_This issue was automatically created by the validation script._"
        } > "${body_file}"

        # Create GitHub issue
        create_github_issue "${title}" "${body_file}" "${repo}"

        return 1
    fi

    log "Validation passed for ${ntfile} (warnings ignored)"
}

nt_to_hdt() {
    local ntfile="$1"
    local outdir="$2"
    local expected_hdt="${outdir}/graph.hdt"

    log "Converting NT to HDT: ${ntfile} -> ${expected_hdt}"
    rdf2hdt.sh -cattree -cattreelocation "${HDT_TMP_DIR}" -index "${ntfile}" "${expected_hdt}" > "${REPORT_DIR}/rdf2hdt.log" 2>&1

    [ -f "${expected_hdt}" ] || err_and_exit "HDT conversion failed: ${expected_hdt} not created"
    log "HDT created: ${expected_hdt}"
}

hdt_to_nt() {
    local hdtfile="$1"
    local out_nt="$2"
    log "Converting HDT to NT: ${hdtfile} -> ${out_nt}"
    hdt2rdf.sh "${hdtfile}" "${out_nt}" 2>> "${REPORT_DIR}/hdt2rdf.log"
}

combine_hdt_files() {
    local -n _src=$1; local out_hdt="$2"
    log "Combining ${#_src[@]} HDT files into ${out_hdt}..."

    if command -v hdtcat >/dev/null 2>&1; then
        log "Attempting hdtcat..."
        if hdtcat.sh "${_src[@]}" > "${out_hdt}" 2>> "${REPORT_DIR}/hdtcat.log"; then
            log "hdtcat succeeded"
            return 0
        fi
        log "hdtcat failed, falling back to manual merge."
    fi

    # Fallback: convert each HDT to NT, merge NTs, convert back
    tmpdir="${HDT_TMP_DIR}/hdt_nts"; mkdir -p "$tmpdir"
    local nts=()
    for f in "${_src[@]}"; do
        nt="${tmpdir}/$(basename "$f").nt"
        hdt_to_nt "$f" "$nt"
        nts+=("$nt")
    done

    merged="${HDT_TMP_DIR}/merged_hdt_parts.nt"
    log "Merging ${#nts[@]} NT files from HDTs..."

    if ! ${RIOT_BIN} --merge --nocheck --output NT "${nts[@]}" > "${merged}" 2> "${REPORT_DIR}/riot_merge_hdt.log"; then
        log "ERROR: riot merge failed, see ${REPORT_DIR}/riot_merge_hdt.log"

        # --- Create GitHub issue on merge failure ---
        local repo="frink-okn/graph-coordination"
        local title="RDF Merge failed for ${REPO_NAME}"
        local body_file="/tmp/gh_issue_body_merge.txt"

        {
            echo "RDF merge failed for **${REPO_NAME}**"
            echo
            echo "**Commit:** ${COMMIT_ID:-unknown}"
            echo "**KG:** ${KG_NAME:-unknown}"
            echo "**HDT to NT files:** ${nts} "
            echo "### Error Log (Last 100 lines)"
            echo '```'
            tail -n 100 "${REPORT_DIR}/riot_merge_hdt.log"
            echo '```'
            echo
            echo "_This issue was automatically created by the RDF merge step of the converter script._"
        } > "${body_file}"

        # Require gh CLI to succeed
        gh issue create \
            --title "${title}" \
            --body-file "${body_file}" \
            --label "graph-validation" \
            --repo "${repo}"
        err_and_exit "GitHub issue creation failed for RDF merge error."

        err_and_exit "RDF merge failed — issue created on GitHub."
    fi



    # FIX: Use unique temp directory to avoid collisions
    local temp_hdt_dir="${HDT_TMP_DIR}/temp_combine_$$"
    mkdir -p "${temp_hdt_dir}"
    nt_to_hdt "${merged}" "${temp_hdt_dir}"
    mv "${temp_hdt_dir}/graph.hdt" "$out_hdt"
    rm -rf "${temp_hdt_dir}"
}

# FIX: Remove unused parameter, clarify logic, add better temp file naming
merge_hdt_and_text() {
    local -n _hdt=$1  # Array of HDT files
    local out_hdt="$2"
    local out_ntgz="$3"

    log "=== Starting merge of HDT and text files ==="

    # Step 1: Process text files if any
    local text_hdt=""
    text_nt=$(merge_text_to_nt || true)

    # FIX: Better empty check
    if [ -n "${text_nt:-}" ] && [ -s "${text_nt:-}" ]; then
        log "Found valid text NT, converting to HDT..."
        if ! validate_nt "${text_nt}"; then
            err_and_exit "Validation failed for merged text NT: ${text_nt}"
        fi

        # FIX: Use unique temp directory to avoid collisions
        local text_temp_dir="${HDT_TMP_DIR}/text_convert_$$"
        mkdir -p "${text_temp_dir}"
        nt_to_hdt "${text_nt}" "${text_temp_dir}"
        text_hdt="${HDT_TMP_DIR}/text_only.graph.hdt"
        mv "${text_temp_dir}/graph.hdt" "${text_hdt}"
        rm -rf "${text_temp_dir}"
        log "Text HDT created: ${text_hdt}"
    else
        log "No valid text triples to convert"
    fi

    # Step 2: Process HDT files if any
    local hdt_hdt=""
    if [ ${#_hdt[@]} -gt 0 ]; then
        hdt_hdt="${HDT_TMP_DIR}/combined_hdt.graph.hdt"
        combine_hdt_files _hdt "${hdt_hdt}"
        log "HDT files combined: ${hdt_hdt}"
    else
        log "No HDT files to combine"
    fi

    # Step 3: Merge everything together
    log "=== Final merge phase ==="
    if [ -n "${text_hdt}" ] && [ -n "${hdt_hdt}" ]; then
        log "Merging text HDT + input HDTs..."
        local both=("${text_hdt}" "${hdt_hdt}")
        combine_hdt_files both "${out_hdt}"
        log "Both sources merged into ${out_hdt}"
    elif [ -n "${text_hdt}" ]; then
        log "Only text HDT available, using it as final..."
        mv "${text_hdt}" "${out_hdt}"
    elif [ -n "${hdt_hdt}" ]; then
        log "Only HDT sources available, using them as final..."
        mv "${hdt_hdt}" "${out_hdt}"
    else
        err_and_exit "No HDT produced from any source!"
    fi

    # Step 4: Export final HDT to NT
    log "Exporting final HDT to NT..."
    tmp_nt="${RIOT_TMP_DIR}/final_combined.nt"
    hdt2rdf.sh "${out_hdt}" > "${tmp_nt}" 2>> "${REPORT_DIR}/hdt2rdf_final.log"

    # FIX: Check NT was created
    if [ ! -s "${tmp_nt}" ]; then
        err_and_exit "Failed to export HDT to NT: ${tmp_nt} is empty"
    fi

    gzip -c "${tmp_nt}" > "${out_ntgz}"
    log "=== Merge complete: ${out_hdt} and ${out_ntgz} ==="
}

# --- Main execution ---
final_hdt="${HDT_FINAL_DIR}/graph.hdt"
final_ntgz="${NT_FINAL_DIR}/graph.nt.gz"

log "========================================="
log "Starting conversion process"
log "Text files: ${#TEXT_FILES[@]}, HDT files: ${#HDT_FILES[@]}"
log "========================================="

# Path 1: Only HDT files
if [ ${#TEXT_FILES[@]} -eq 0 ] && [ ${#HDT_FILES[@]} -gt 0 ]; then
    log "=== HDT-only path ==="
    combine_hdt_files HDT_FILES "${final_hdt}"
    tmp_nt="${RIOT_TMP_DIR}/from_hdt.nt"
    hdt_to_nt "${final_hdt}" "${tmp_nt}"
    gzip -c "${tmp_nt}" > "${final_ntgz}"
    log "✓ Done: ${final_hdt}, ${final_ntgz}"
    exit 0
fi

# Path 2: Only text files
# Path 2: Only text files
if [ ${#TEXT_FILES[@]} -gt 0 ] && [ ${#HDT_FILES[@]} -eq 0 ]; then
    log "=== Text-only path ==="
    text_nt=$(merge_text_to_nt || true)
    # FIX: Better empty check
    if [ -z "${text_nt:-}" ] || [ ! -s "${text_nt:-}" ]; then
        log "No valid text triples found, nothing to convert."
        exit 0
    fi

    if ! validate_nt "${text_nt}"; then
        err_and_exit "Validation failed for ${text_nt}"
    fi

    # FIX: Use temp directory to avoid collision
    temp_dir="${HDT_TMP_DIR}/text_final_$$"
    mkdir -p "${temp_dir}"
    nt_to_hdt "${text_nt}" "${temp_dir}"
    mv "${temp_dir}/graph.hdt" "${final_hdt}"
    rm -rf "${temp_dir}"

    gzip -c "${text_nt}" > "${final_ntgz}"
    log "✓ Done: ${final_hdt}, ${final_ntgz}"
    exit 0
fi

# Path 3: Mixed input
log "=== Mixed HDT + text path ==="
merge_hdt_and_text HDT_FILES "${final_hdt}" "${final_ntgz}"
log "✓ All done: ${final_hdt}, ${final_ntgz}"