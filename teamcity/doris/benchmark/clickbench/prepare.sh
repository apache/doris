#!/bin/bash
set -ex

pullrequestID=%teamcity.pullRequest.number%
test_branch=$pullrequestID
source_branch=%teamcity.pullRequest.source.branch%
target_branch=%teamcity.pullRequest.target.branch%
build_id=%teamcity.build.id%
pipeline_home=/home/ec2-user/teamcity/
checkout_home=$(pwd)
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

echo 'check if exists older commits of this pull request'
outdate_builds_of_pr=($(grep ${test_branch}_${source_branch}_${target_branch}_incubator-doris $pipeline_home/OpenSourceDorisBuild.log | awk '{print $1}'))
for old_build_id in ${outdate_builds_of_pr[@]}; do
    echo "STRAT checking build $old_build_id"
    old_build_status=$(bash ../common/teamcity_api.sh --show_build_state $old_build_id)
    if [[ $old_build_status == "running" ]]; then
        bash teamcity_api.sh --cancel_running_build $old_build_id
    fi
done