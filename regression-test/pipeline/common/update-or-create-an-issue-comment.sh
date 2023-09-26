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

create_issue_comment() {
    if [[ -z "$1" ]] || [[ -z "$2" ]]; then
        echo 'Usage: update_or_create_issue_comment <ISSUE_NUMBER> <COMMENT_BODY>' && return 1
    fi
    if [[ -z ${github_token} ]]; then
        echo "require env: github_token" && return 1
    fi
    ISSUE_NUMBER="$1"
    COMMENT_BODY="$2"

    if curl -s \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${github_token}" \
        https://api.github.com/repos/apache/doris/issues/"${ISSUE_NUMBER}"/comments \
        -d "{\"body\": \"${COMMENT_BODY}\"}"; then
        echo -e "\033[32m Create issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && return 0
    else
        echo -e "\033[31m Create issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && return 1
    fi
}

update_or_create_issue_comment() {
    if [[ -z "$1" ]] || [[ -z "$2" ]]; then
        echo 'Usage: update_or_create_issue_comment <ISSUE_NUMBER> <COMMENT_BODY>' && return 1
    fi
    if [[ -z ${github_token} ]]; then
        echo "require env: github_token" && return 1
    fi

    ISSUE_NUMBER="$1"
    COMMENT_BODY="$2"
    COMMENT_USER='"selectdb-robot"'

    # Refer to: https://docs.github.com/en/rest/issues/comments#create-an-issue-comment

    file_name='comments_file'
    if ! curl -s \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${github_token}" \
        https://api.github.com/repos/apache/doris/issues/"${ISSUE_NUMBER}"/comments \
        >"${file_name}"; then
        echo -e "\033[31m List issue(${ISSUE_NUMBER}) comments FAIL... \033[0m" && return 1
    fi

    comments_count=$(jq '.[] | length' "${file_name}" | wc -l)
    for ((i = 1; i <= comments_count; ++i)); do
        comment_body=$(jq ".[-${i}].body" "${file_name}")
        comment_user=$(jq ".[-${i}].user.login" "${file_name}")
        if [[ "${comment_user}" == "${COMMENT_USER}" ]] &&
            [[ "${comment_body}" == *"${COMMENT_BODY:0:18}"* ]]; then
            echo "Similar comment already exists, will update it..."
            comment_id=$(jq ".[-${i}].id" "${file_name}")
            if curl -s \
                -X PATCH \
                -H "Accept: application/vnd.github+json" \
                -H "Authorization: Bearer ${github_token}" \
                https://api.github.com/repos/apache/doris/issues/comments/"${comment_id}" \
                -d "{\"body\":\"${COMMENT_BODY}\"}"; then
                echo -e "\033[32m Update issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && return 0
            else
                echo -e "\033[31m Update issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && return 1
            fi
        fi
    done

    echo "No similar comment exists, will create a new one..."
    if curl -s \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${github_token}" \
        https://api.github.com/repos/apache/doris/issues/"${ISSUE_NUMBER}"/comments \
        -d "{\"body\": \"${COMMENT_BODY}\"}"; then
        echo -e "\033[32m Create issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && return 0
    else
        echo -e "\033[31m Create issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && return 1
    fi
}
