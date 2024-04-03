#!/usr/bin/env bash

########################### Teamcity Build Step: Command Line #######################
: <<EOF
#!/bin/bash
export PATH=/usr/local/software/apache-maven-3.6.3/bin:${PATH}
if [[ -f "${teamcity_build_checkoutDir:-}"/regression-test/pipeline/cloud_p0/clean.sh ]]; then
    cd "${teamcity_build_checkoutDir}"/regression-test/pipeline/cloud_p0/
    bash -x clean.sh
else
    echo "Build Step file missing: regression-test/pipeline/cloud_p0/clean.sh" && exit 1
fi
EOF
############################# clean.sh content ########################################
# shellcheck source=/dev/null
# stop_doris, clean_fdb
source "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/doris-utils.sh

echo "#### Check env"
if [[ -z "${teamcity_build_checkoutDir}" ]]; then echo "ERROR: env teamcity_build_checkoutDir not set" && exit 1; fi

# shellcheck source=/dev/null
source "$(bash "${teamcity_build_checkoutDir}"/regression-test/pipeline/common/get-or-set-tmp-env.sh 'get')"
if ${skip_pipeline:=false}; then echo "INFO: skip build pipline" && exit 0; else echo "INFO: no skip"; fi

echo "#### Run tpcds test on Doris ####"
DORIS_HOME="${teamcity_build_checkoutDir}/output"
export DORIS_HOME
stop_doris
clean_fdb "cloud_instance_0"
