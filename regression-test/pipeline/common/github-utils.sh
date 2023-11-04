#!/bin/bash

function create_an_issue_comment() {
    ISSUE_NUMBER="$1"
    COMMENT_BODY="$2"
    if [[ -z "${COMMENT_BODY}" ]]; then return 1; fi
    if [[ -z "${GITHUB_TOKEN}" ]]; then return 1; fi

    local OWNER='apache'
    local REPO='doris'
    if ret=$(curl -s \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_TOKEN:-}" \
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
