#!/usr/bin/env python3
#
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
# This file is copied from
# https://github.com/ClickHouse/ClickHouse/blob/master/tests/ci/performance_comparison_check.py
# and modified by Doris.

import os
import logging
import sys
import json
import subprocess
import traceback
import re

from github import Github

from env_helper import GITHUB_EVENT_PATH, GITHUB_REPOSITORY, TEMP_PATH, REPO_COPY, REPORTS_PATH, S3_ENDPOINT
from env_helper import GITHUB_RUN_ID, GITHUB_TOKEN, S3_BUILDS_BUCKET
from pr_info import PRInfo
from s3_helper import S3Helper
from commit_status_helper import get_commit, post_commit_status
from tee_popen import TeePopen

IMAGE_NAME = "apache/incubator-doris:build-env-ldb-toolchain-latest"

def get_run_command(workspace, result_path, pr_to_test, sha_to_test, additional_env, image):
    return f"docker run --privileged --volume={workspace}:/workspace --volume={result_path}:/output " \
        f"--cap-add syslog --cap-add sys_admin --cap-add sys_rawio " \
        f"-e PR_TO_TEST={pr_to_test} -e SHA_TO_TEST={sha_to_test} {additional_env} " \
        f"{image} /bin/bash -c \"cd /workspace/tests/ci/performance_comparison; ./entrypoint.sh; \""

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    performance_result_path = TEMP_PATH+"/performance_result"
    reports_path = REPORTS_PATH

    check_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(GITHUB_EVENT_PATH, 'r', encoding='utf-8') as event_file:
        event = json.load(event_file)

    gh = Github(GITHUB_TOKEN)
    pr_info = PRInfo(event)
    commit = get_commit(gh, pr_info.sha)

    docker_env = ''

    docker_env += f" -e S3_URL={S3_ENDPOINT}/{S3_BUILDS_BUCKET}"

    if pr_info.number == 0:
        pr_link = commit.html_url
    else:
        pr_link = f"https://github.com/{GITHUB_REPOSITORY}/pull/{pr_info.number}"

    task_url = f"https://github.com/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
    docker_env += ' -e DRPC_ADD_REPORT_LINKS="<a href={}>Job (actions)</a> <a href={}>Tested commit</a>"'.format(
        task_url, pr_link)

    result_path = performance_result_path
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    # Download files needed by performance comparison.
    run_command = get_run_command(repo_path, result_path, pr_info.number, pr_info.sha, docker_env, IMAGE_NAME)
    logging.info("Going to run command %s", run_command)
    run_log_path = os.path.join(temp_path, "runlog.log")
    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    # subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    paths = {
        'compare.log': os.path.join(result_path, 'compare.log'),
        'output.7z': os.path.join(result_path, 'output.7z'),
        'report.html': os.path.join(result_path, 'report.html'),
        'all-queries.html': os.path.join(result_path, 'all-queries.html'),
        'queries.rep': os.path.join(result_path, 'queries.rep'),
        'all-query-metrics.tsv': os.path.join(result_path, 'report/all-query-metrics.tsv'),
        'runlog.log': run_log_path,
    }

    check_name_prefix = check_name.lower().replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_')
    s3_prefix = f'{pr_info.number}/{pr_info.sha}/{check_name_prefix}/'
    s3_helper = S3Helper(S3_ENDPOINT)
    for file in paths:
        try:
            paths[file] = s3_helper.upload_test_report_to_s3(paths[file],
                s3_prefix + file)
        except Exception:
            paths[file] = ''
            traceback.print_exc()

    # Upload all images and flamegraphs to S3
    try:
        s3_helper.upload_test_folder_to_s3(
            os.path.join(result_path, 'images'),
            s3_prefix + 'images'
        )
    except Exception:
        traceback.print_exc()

    # Try to fetch status from the report.
    status = ''
    message = ''
    try:
        report_text = open(os.path.join(result_path, 'report.html'), 'r').read()
        status_match = re.search('<!--[ ]*status:(.*)-->', report_text)
        message_match = re.search('<!--[ ]*message:(.*)-->', report_text)
        if status_match:
            status = status_match.group(1).strip()
        if message_match:
            message = message_match.group(1).strip()

        # TODO: Remove me, always green mode for the first time
        status = 'success'
    except Exception:
        traceback.print_exc()
        status = 'failure'
        message = 'Failed to parse the report.'

    if not status:
        status = 'failure'
        message = 'No status in report.'
    elif not message:
        status = 'failure'
        message = 'No message in report.'

    report_url = task_url

    if paths['runlog.log']:
        report_url = paths['runlog.log']

    if paths['compare.log']:
        report_url = paths['compare.log']

    if paths['output.7z']:
        report_url = paths['output.7z']

    if paths['report.html']:
        report_url = paths['report.html']


    # post_commit_status(gh, pr_info.sha, check_name, message, status, report_url)
