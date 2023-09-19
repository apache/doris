#!/bin/bash

function usage() {
    echo -e "Usage:
    source $0 <case_level> <cluster> <checkout_dir>
    note: 
        case_level only support P0, P1, 
        cluster can be one of Cluster0 to Cluster7
        checkout_dir should be tc checkout dir

    return generate coverage_report cmd
    " && return 1
}

function init_cov_tool() {
    DORIS_CLANG_HOME="$(dirname "$(command -v clang)")"/..
    export DORIS_CLANG_HOME

    covs=()
    while IFS='' read -r line; do covs+=("${line}"); done <<<"$(find "${DORIS_CLANG_HOME}" -name "llvm-cov*")"
    if [[ ${#covs[@]} -ge 1 ]]; then
        LLVM_COV="${covs[0]}"
    else
        LLVM_COV="$(command -v llvm-cov)"
    fi
    export LLVM_COV

    profdatas=()
    while IFS='' read -r line; do profdatas+=("${line}"); done <<<"$(find "${DORIS_CLANG_HOME}" -name "llvm-profdata*")"
    if [[ ${#profdatas[@]} -ge 1 ]]; then
        LLVM_PROFDATA="${profdatas[0]}"
    else
        LLVM_PROFDATA="$(command -v llvm-profdata)"
    fi
    export LLVM_PROFDATA

}

function get_profraw() {
    doris_deploy_path="/mnt/ssd01/pipline/OpenSourceDoris/clusterEnv/${case_level}/${cluster}/be"
    profraw="${tc_checkout_dir}/default.profraw"
    profdata="doris_be.profdata"
    bin="${tc_checkout_dir}/lib/doris_be"
    report="${tc_checkout_dir}/report"

    cp -r "${doris_deploy_path}/default.profraw" "${profraw}"
    cp -r "${doris_deploy_path}/lib/doris_be" "${bin}"
}

function compute_coverage() {
    cd ${deploy_path}

    cmd1="${LLVM_PROFDATA} merge -o ${profdata} \"${profraw}\""
    echo $cmd1
    eval $cmd1
    cmd2="${LLVM_COV} show -output-dir=${report} -format=html \
            -ignore-filename-regex='(.*gensrc/.*)|(.*_test\.cpp$)|(.*be/test.*)|(.*apache-orc/.*)|(.*clucene/.*)' \
            -instr-profile=${profdata} \
            -object=${bin}"
    echo $cmd2
    #eval $cmd2
    
}

function main() {
    init_cov_tool
    if [[ $@ -ge 2 ]];then
        case_level=$1
        cluster=$2
        tc_checkout_dir=$3
        get_profraw
    else
        usage
    fi
}

main "$@"