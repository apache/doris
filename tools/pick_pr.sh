#!/bin/sh
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

if [ $# -ne 4 ]; then
    echo "usage: $0 <branch_from> <branch_to> <git_push_remote> <pr_number>"
    echo
    echo "       branch_from/to  syntax: local_git_remote_name/branch_name  eg: upstream/master upstream/branch-2.0"
    echo "       git_push_remote syntax: local_git_remote_name              eg: origin your_name"
    echo "       use 'git remote -v' to get list of local_git_remote_name"
    echo
    echo "       NOTICE: This script depends on github cli tool 'gh', which can be get from https://cli.github.com/"
    echo "               You need to login using 'gh auth login'. It will ask your github token, which can be generated at https://github.com/settings/tokens"
    exit 1
fi

doris_repo='apache/doris'

branch_from=$1
remote_from=$(echo "${branch_from}" | awk -F/ '{print $1}')

branch_to=$2
branch_to_name=$(echo "${branch_to}" | awk -F/ '{print $2}')
remote_to=$(echo "${branch_to}" | awk -F/ '{print $1}')

push_remote=$3
push_url=$(git remote get-url --push "${push_remote}")
# https://github.com/your_name/doris.git
# https://token@github.com/your_name/doris.git
push_url=$(echo "${push_url}" | sed 's|^https://.*github.com/||')
# git@github.com:your_name/doris.git
push_url=$(echo "${push_url}" | sed 's|^git@.*github.com:||')
# get your_name
push_id=$(echo "${push_url}" | awk -F/ '{print $1}')

pr=$4

echo "step1: git fetch to update local git repos: ${remote_from} and ${remote_to}"
git fetch "${remote_from}"
git fetch "${remote_to}"

echo
echo "step2: get pr ${pr} commit id using gh cli"
commitid=$(gh pr view "${pr}" --repo "${doris_repo}" --json mergeCommit -t '{{.mergeCommit.oid}}')
if ! git show --stat "${commitid}"; then
    echo "git show --stat ${commitid} failed, ${commitid} is invalid"
    echo
    exit 2
fi

echo
echo "step3: create local branch ${branch_pick} based on remote branch ${branch_to}"
branch_pick=$(echo "pick_${pr}_to_${branch_to}" | sed 's|/|_|g')
if ! git checkout -b "${branch_pick}" "${branch_to}"; then
    echo "git checkout -b ${branch_pick} ${branch_to} failed"
    echo
    exit 3
fi

echo
echo "step4: will run git cherry-pick ${commitid} , please confirm y/n: "

read -r ans
echo
if [ "${ans}" = "y" ]; then
    if ! git cherry-pick "${commitid}"; then
        echo "git cherry-pick return none zero $?, wait for manual processing, please confirm continue or exit c/e: "
        read -r ans
        if [ "${ans}" != "c" ]; then
            echo "manual processing confirm ${ans} is not c, git cherry-pick --abort and exit now"
            git cherry-pick --abort
            exit 4
        fi
    fi

    echo
    echo "step5: push to your remote repo ${push_remote} $(git remote get-url --push "${push_remote}")"
    git push "${push_remote}"

    echo
    echo "step6: create pr using gh cli"
    title=$(gh pr view "${pr}" --repo "${doris_repo}" --json title -t '{{.title}}')
    newpr_url=$(gh pr create --repo "${doris_repo}" --base "${branch_to_name}" --head "${push_id}:${branch_pick}" --title "${title} #${pr}" --body "cherry pick from #${pr}")
    echo "new pr url: ${newpr_url}"

    echo
    echo "step7: comment 'run buildall' to trigger tests using gh cli"
    newpr=$(echo "${newpr_url}" | awk -F/ '{print $NF}')
    if [ "${newpr}" != "" ]; then
        gh pr comment --repo "${doris_repo}" "${newpr}" --body 'run buildall'
    fi
else
    echo "do nothing for ${pr}"
    exit 5
fi
