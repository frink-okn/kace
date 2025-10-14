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

JAVA_OPTIONS="${JAVA_OPTIONS:-} -Djava.io.tmpdir=${JAVATMP_DIR}"
export JAVA_OPTIONS

FAILURES_LOG=${REPORT_DIR}/failures.log
VALIDATE_LOG=${REPORT_DIR}/riot_validate.log

mkdir -p "${RIOT_TMP_DIR}" "${HDT_TMP_DIR}" "${HDT_FINAL_DIR}" "${NT_FINAL_DIR}" "${REPORT_DIR}" "${JAVATMP_DIR}"

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

log() { printf '%s %s\n' "$(date --iso-8601=seconds)" "$*"; }
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
    [ ${#TEXT_FILES[@]} -eq 0 ] && return 0

    log "Merging ${#TEXT_FILES[@]} RDF text files..."
    if ! ${RIOT_BIN} --merge --nocheck --output NT "${TEXT_FILES[@]}" > "${out_nt}" 2> "${REPORT_DIR}/riot_merge.log"; then
        log "Warning: riot merge failed, see ${REPORT_DIR}/riot_merge.log"
        return 0
    fi

    # FIX: Check for empty files
    if [ ! -s "${out_nt}" ]; then
        log "Warning: merged NT is empty (metadata-only or VOID TTL). Skipping text merge."
        rm -f "${out_nt}"
        return 0
    fi

    echo "${out_nt}"
}

# FIX: Add proper error checking
validate_nt() {
    local ntfile="$1"
    log "Validating ${ntfile}"
    if ! ${RIOT_BIN} --validate --check --strict --sink "${ntfile}" > "${VALIDATE_LOG}" 2>&1; then
        log "ERROR: Validation failed for ${ntfile}"
        return 1
    fi
    log "Validation passed for ${ntfile}"
}

# FIX: Add existence check after conversion
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
    ${RIOT_BIN} --merge --nocheck --output NT "${nts[@]}" > "${merged}"

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

    nt_to_hdt "${text_nt}" "${HDT_FINAL_DIR}"
    mv "${HDT_FINAL_DIR}/graph.hdt" "${final_hdt}"
    gzip -c "${text_nt}" > "${final_ntgz}"
    log "✓ Done: ${final_hdt}, ${final_ntgz}"
    exit 0
fi

# Path 3: Mixed input
log "=== Mixed HDT + text path ==="
merge_hdt_and_text HDT_FILES "${final_hdt}" "${final_ntgz}"
log "✓ All done: ${final_hdt}, ${final_ntgz}"