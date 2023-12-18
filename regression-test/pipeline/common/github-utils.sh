#!/usr/bin/env bash
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

function create_an_issue_comment() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    if [[ -z "${COMMENT_BODY}" ]]; then return 1; fi
    if [[ -z "${GITHUB_TOKEN}" ]]; then return 1; fi

    local OWNER='apache'
    local REPO='doris'
    COMMENT_BODY=$(echo "${COMMENT_BODY}" | sed -e ':a;N;$!ba;s/\t/\\t/g;s/\n/\\n/g') # 将所有的 Tab字符替换为\t 换行符替换为\n
    if ret=$(curl -s \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN:-}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        https://api.github.com/repos/"${OWNER}"/"${REPO}"/issues/"${ISSUE_NUMBER}"/comments \
        -d "{\"body\": \"${COMMENT_BODY}\"}"); then
        if echo "${ret}" | grep "Problems parsing JSON"; then
            is_succ=false
        else
            is_succ=true
        fi
    else
        is_succ=false
    fi

    if ${is_succ}; then
        echo -e "\033[32m Create issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && return 0
    else
        echo -e "\033[31m Create issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && return 1
    fi
}

function create_an_issue_comment_tpch() {
    local ISSUE_NUMBER="$1"
    local COMMENT_BODY="$2"
    local machine='aliyun_ecs.c7a.8xlarge_32C64G'
    COMMENT_BODY="
<details>
<summary>TPC-H test result on machine: '${machine}'</summary>

\`\`\`
${COMMENT_BODY}
\`\`\`
</details>
"
    create_an_issue_comment "${ISSUE_NUMBER}" "${COMMENT_BODY}"
}
