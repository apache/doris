#!/bin/bash
set -ex

pipeline_home=${HOME}/teamcity/

skip_pipeline=${skip_pipeline:=false}

echo 'check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then
    echo "skip build pipline"
    exit 0
else
    echo "no skip"
fi

echo '====prepare===='

echo 'update scripts from git@github.com:selectdb/selectdb-qa.git'
cd "${pipeline_home}"
if [[ ! -d "${pipeline_home}/selectdb-qa" ]]; then
    git clone git@github.com:selectdb/selectdb-qa.git
fi
qa_home="${pipeline_home}/selectdb-qa"
cd "${qa_home}" && git stash && git checkout main && git pull && cd -






