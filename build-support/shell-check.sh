#!/bin/bash
# shellcheck disable=2311

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail
set +o posix

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
DORIS_HOME="$(
    cd "${SCRIPT_DIR}/.." &>/dev/null
    pwd
)"
DORIS_THIRDPARTY="$(
    source "${DORIS_HOME}/env.sh" &>/dev/null
    echo "${DORIS_THIRDPARTY}"
)"
INSTALL_PATH="${DORIS_THIRDPARTY}/installed/bin"

SHELLCHECK=''
SHFMT=''

log() {
    local level="${1}"
    local message="${2}"
    local date
    date="$(date +'%Y-%m-%d %H:%M:%S')"
    if [[ "${level}" == 'INFO' ]]; then
        level="[\033[32;1m ${level}  \033[0m]"
    elif [[ "${level}" == 'WARNING' ]]; then
        level="[\033[33;1m${level}\033[0m]"
    elif [[ "${level}" == 'ERROR' ]]; then
        level="[\033[31;1m ${level} \033[0m]"
    fi
    echo -e "${level} ${date} - ${message}"
}

log_info() {
    local message="${1}"
    log 'INFO' "${message}"
}

log_warning() {
    local message="${1}"
    log 'WARNING' "${message}"
}

log_error() {
    local message="${1}"
    log 'ERROR' "${message}"
    exit 1
}

get_version() {
    local program="${1}"
    local tool="${program##*/}"

    case "${tool}" in
    'shellcheck') "${program}" --version | sed -n 's/version: \(.*\)/\1/p' ;;
    'shfmt') "${program}" --version ;;
    *) ;;
    esac
}

check_tool() {
    local tool="${1}"
    local version="${2}"
    local program

    if program="$(command -v "${tool}" 2>/dev/null)" && [[ ! "$(get_version "${program}")" < "${version}" ]]; then
        echo "${program}"
        return 0
    fi

    program="${INSTALL_PATH}/${tool}"
    if [[ -f "${program}" && ! "$(get_version "${program}")" < "${version}" ]]; then
        echo "${program}"
        return 0
    fi

    return 255
}

get_url() {
    local tool="${1}"
    local os
    local arch

    os="$(uname -s | awk '{ print tolower($0) }')"
    arch="$(uname -m | awk '{ print tolower($0) }')"

    if [[ "${tool}" == 'shellcheck' ]]; then
        if [[ "${arch}" == 'arm64' ]]; then
            arch='aarch64'
        fi
        echo "https://github.com/koalaman/shellcheck/releases/download/v0.8.0/shellcheck-v0.8.0.${os}.${arch}.tar.xz"
        return 0
    elif [[ "${tool}" == 'shfmt' ]]; then
        if [[ "${arch}" == 'x86_64' ]]; then
            arch='amd64'
        elif [[ "${arch}" == 'aarch64' ]]; then
            arch='arm64'
        fi
        echo "https://github.com/mvdan/sh/releases/download/v3.5.1/shfmt_v3.5.1_${os}_${arch}"
        return 0
    fi
    return 255
}

get_md5() {
    local tool="${1}"
    local os
    local arch

    os="$(uname -s | awk '{ print tolower($0) }')"
    arch="$(uname -m | awk '{ print tolower($0) }')"

    case "${tool}" in
    'shellcheck')
        case "${os}" in
        'linux')
            case "${arch}" in
            'x86_64') echo '86ee889b1e771bc8292a7043df4b962a' ;;
            'arm64' | 'aarch64') echo 'a0338c733d1283a51777b27edf9ccc96' ;;
            *) ;;
            esac
            ;;
        'darwin')
            case "${arch}" in
            'x86_64') echo 'e744840256a77a1a277e81a3032f7bf4' ;;
            *) ;;
            esac
            ;;
        *) ;;
        esac
        ;;

    'shfmt')
        case "${os}" in
        'linux')
            case "${arch}" in
            'x86_64') echo '1d234f204e249bf1c524015ce842b117' ;;
            'arm64' | 'aarch64') echo '5c8494fd257d5cd9db1eef48af967e47' ;;
            *) ;;
            esac
            ;;
        'darwin')
            case "${arch}" in
            'x86_64') echo '42f14325207a47f3177c96683c5085a4' ;;
            'arm64' | 'aarch64') echo 'a9c51e5d4aeebfa2c8cc70af3006bc4e' ;;
            *) ;;
            esac
            ;;
        *) ;;
        esac
        ;;
    *) ;;
    esac
}

download_tool() {
    local url="${1}"
    local package="${2}"
    local md5="${3}"
    log_info "Download ${tool} from ${url}"

    if [[ -f "${package}" && "$(md5sum "${package}" | awk '{print $1}')" == "${md5}" ]]; then
        log_info "${tool} has been downloaded succesfully!"
        return 0
    fi

    for i in {0..2}; do
        if [[ "${i}" -gt 0 ]]; then
            log_warning "Retry #${i}..."
        fi
        if curl --connect-timeout 5 --speed-limit 1000 --speed-time 30 -L "${url}" -o "${package}" &&
            [[ "$(md5sum "${package}" | awk '{print $1}')" == "${md5}" ]]; then
            log_info "${tool} has been downloaded succesfully!"
            return 0
        fi
    done
    return 255
}

install_shellcheck() {
    local tool="shellcheck"
    local url
    local package
    local md5
    local dir

    SYSTEM_NAME="$(uname -s)"
    if [[ "${SYSTEM_NAME}" == 'Darwin' ]] && command -v 'brew' &>/dev/null; then
        brew install "${tool}"
    else
        url="$(get_url "${tool}")"
        package="${url##*/}"
        md5="$(get_md5 "${tool}")"
        download_tool "${url}" "${package}" "${md5}"

        log_info "Set ${tool} up."
        dir="$(mktemp -d)"
        tar -xf "${package}" -C "${dir}" --strip-component=1
        mkdir -p "${INSTALL_PATH}"
        mv "${dir}/${tool}" "${INSTALL_PATH}"
    fi
}

install_shfmt() {
    local tool='shfmt'
    local url
    local package
    local md5
    local dir

    SYSTEM_NAME="$(uname -s)"
    if [[ "${SYSTEM_NAME}" == 'Darwin' ]] && command -v 'brew' &>/dev/null; then
        brew install "${tool}"
    else
        url="$(get_url "${tool}")"
        package="${url##*/}"
        md5="$(get_md5 "${tool}")"
        download_tool "${url}" "${package}" "${md5}"

        log_info "Set ${tool} up."
        mkdir -p "${INSTALL_PATH}"
        cp "${package}" "${INSTALL_PATH}/shfmt"
        chmod a+x "${INSTALL_PATH}/shfmt"
    fi
}

install_tools_if_neccessary() {
    while ! SHELLCHECK="$(check_tool 'shellcheck' '0.8.0')"; do
        log_warning 'shellcheck was not found.'
        install_shellcheck
    done
    log_info "shellcheck found: ${SHELLCHECK}"

    while ! SHFMT="$(check_tool 'shfmt' '3.5.1')"; do
        log_warning 'shfmt was not found.'
        install_shfmt
    done
    log_info "shfmt found: ${SHFMT}"
}

find_shell_scripts() {
    local path="${1}"
    local exclude_patterns
    local content
    local files=()
    content="$(grep 'sh_checker_exclude:' "${DORIS_HOME}/.github/workflows/code-checks.yml")"
    read -r -a exclude_patterns <<<"${content#*sh_checker_exclude: }"
    while read -r file; do
        local matched=false
        for pattern in "${exclude_patterns[@]}"; do
            if echo "${file:((${#DORIS_HOME} + 1))}" | grep -E "${pattern}" &>/dev/null; then
                matched=true
                break
            fi
        done
        if ! "${matched}"; then
            files+=("${file}")
        fi
    done < <(find "${path}" -type f -name '*.sh')
    echo "${files[@]}"
}

run_tool() {
    set +e
    local tool="${1}"
    local opt="${2}"
    local status
    shift 2
    pushd "${DORIS_HOME}" >/dev/null
    log_info "Run tool: ${tool}"
    "${tool}" ${opt+${opt}} "${@}"
    status="${?}"
    popd >/dev/null
    return "${status}"
}

run_shellcheck() {
    run_tool "${SHELLCHECK}" '' "${@}"
}

run_shfmt() {
    run_tool "${SHFMT}" '-d' "${@}"
}

main() {
    local files
    local shellcheck_result=0
    local shfmt_result=0

    install_tools_if_neccessary
    read -r -a files < <(find_shell_scripts "${DORIS_HOME}")
    run_shellcheck "${files[@]}" || shellcheck_result="${?}"
    run_shfmt "${files[@]}" || shfmt_result="${?}"

    if [[ "${shellcheck_result}" -ne 0 || "${shfmt_result}" -ne 0 ]]; then
        echo
        log_error 'Some issues were detected!'
    else
        log_info 'Success!'
    fi
}

main "${@}"
