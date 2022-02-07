#!/usr/bin/env python3

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
# https://github.com/ClickHouse/ClickHouse/blob/master/tests/ci/build_check.py
# and modified by Doris.

import subprocess
import logging
import json
import os
import shutil
import sys
import time
from github import Github

from env_helper import REPO_COPY, TEMP_PATH, CACHES_PATH, IMAGES_PATH
from env_helper import S3_ENDPOINT, S3_BUILDS_BUCKET, GITHUB_TOKEN
from s3_helper import S3Helper
from tee_popen import TeePopen
from pr_info import PRInfo


def pull_image(image_name):
    logging.info("Will pull docker image with name: '{}'".format(image_name))
    try:
        subprocess.check_call("docker pull {}".format(image_name), shell=True)
        return True
    except subprocess.CalledProcessError as ex:
        logging.info("Cannot pull image {}".format(image_name))
        return False

def build_doris(logs_path, build_name):
    image_name = "apache/incubator-doris:build-env-ldb-toolchain-latest"
    if not pull_image(image_name):
        raise Exception("Can not pull docker image:{}.".format(image_name))

    build_log_path = os.path.join(logs_path, 'build_log.log')
    doris_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir)

    try:
        shutil.rmtree(doris_dir + '/output')
    except FileNotFoundError as e:
        pass

    if build_name == "performance":
        cmd = "docker run --network=host --rm --volume=/tmp/.m2:/root/.m2 --volume={doris_dir}:/build {img_name} /bin/bash -c \"cd /build && sh build.sh --be --fe -j 8 && sh build_plugin.sh\"".format(
            doris_dir=doris_dir,
            img_name=image_name
        )
    elif build_name == "asan":
        cmd = "docker run --network=host --rm --volume=/tmp/.m2:/root/.m2 --volume={doris_dir}:/build {img_name} /bin/bash -c \"cd /build && export BUILD_TYPE=ASAN && sh build.sh --be --fe -j 8 && sh build_plugin.sh\"".format(
            doris_dir=doris_dir,
            img_name=image_name
        )

    logging.info("Run cmd: {}".format(cmd))

    with TeePopen(cmd, build_log_path) as process:
        retcode = process.wait()
        if os.path.exists(doris_dir + '/output/'):
            build_results = os.listdir(doris_dir + '/output/')
        else:
            build_results = []

        if retcode == 0:
            if len(build_results) != 0:
                logging.info("Built successfully")
            else:
                logging.info("Success exit code, but no build artifacts => build failed")
        else:
            logging.info("Build failed")
    return build_log_path, retcode == 0 and len(build_results) > 0

def get_build_results_if_exists(s3_helper, s3_prefix):
    try:
        content = s3_helper.list_prefix(s3_prefix)
        return content
    except Exception as ex:
        logging.info("Got exception %s listing %s", ex, s3_prefix)
        return None

def create_json_artifact(temp_path, build_name, log_url, build_urls, elapsed, success):
    # subprocess.check_call(f"echo 'BUILD_NAME=build_urls_{build_name}' >> $GITHUB_ENV", shell=True)

    result = {
        "log_url": log_url,
        "build_urls": build_urls,
        "elapsed_seconds": elapsed,
        "status": success,
    }

    json_name = "build_urls_" + build_name + '.json'

    print ("Dump json report", result, "to", json_name, "with env", "build_urls_{build_name}")

    with open(os.path.join(temp_path, json_name), 'w') as build_links:
        json.dump(result, build_links)

def pack_git_info(repo_path):
    subprocess.check_call(f"cd {repo_path} && git fetch origin master:master", shell=True)
    git_path = f"{repo_path}/output/doris";
    subprocess.check_call(f"rm -rf {git_path}", shell=True)
    subprocess.check_call(f"mkdir {git_path}", shell=True)
    subprocess.check_call(f"git -C {git_path} init --bare", shell=True)
    subprocess.check_call(f"git -C {git_path} remote add origin {repo_path}", shell=True)
    subprocess.check_call(f"git -C {git_path} fetch --no-tags --depth 50 origin HEAD:pr", shell=True)
    subprocess.check_call(f"git -C {git_path} fetch --no-tags --depth 50 origin master:master", shell=True)
    subprocess.check_call(f"git -C {git_path} reset --soft pr", shell=True)
    subprocess.check_call(f"git -C {git_path} log -5", shell=True)

def pack_build_results(build_name, repo_path):
    cwd = os.getcwd()
    os.chdir(repo_path)
    os.mkdir("output/plugin")
    if build_name == "performance":
        shutil.copytree("tests/performance", "output/performance")
    os.rename("fe_plugins/output/auditloader.zip", "output/plugin/auditloader.zip")
    subprocess.check_call(f"tar -cv -I pigz -f {build_name}.tgz output/*", shell=True)
    subprocess.check_call(f"tar -cv -I pigz -f {build_name}.tgz output/*", shell=True)
    subprocess.check_call(f"rm {repo_path}/output/* -rf", shell=True)
    subprocess.check_call(f"mv {build_name}.tgz {repo_path}/output", shell=True)
    os.chdir(cwd)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = REPO_COPY
    temp_path = TEMP_PATH
    s3_endpoint = S3_ENDPOINT

    build_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    logging.info("Repo copy path %s", repo_path)

    s3_helper = S3Helper(s3_endpoint)

    pr_info = PRInfo()
    gh = Github(GITHUB_TOKEN)

    version = 'test'
    release_or_pr = str(pr_info.number)
    s3_path_prefix = "/".join((release_or_pr, pr_info.sha, build_name))

    logging.info("prefix: {} ".format(s3_path_prefix))
    # If this is rerun, then we try to find already created artifacts and just
    # put them as github actions artifcat (result)
    build_results = get_build_results_if_exists(s3_helper, s3_path_prefix)
    if build_results is not None and len(build_results) > 0:
        logging.info("Some build results found %s", build_results)
        build_urls = []
        log_url = ''
        for url in build_results:
            if 'build_log.log' in url:
                log_url = s3_endpoint + '/' + S3_BUILDS_BUCKET + '/' +  url.replace('+', '%2B').replace(' ', '%20')
            else:
                build_urls.append(s3_endpoint + '/' + S3_BUILDS_BUCKET + '/' + url.replace('+', '%2B').replace(' ', '%20'))
        create_json_artifact(temp_path, build_name, log_url, build_urls, 0, len(build_urls) > 0)
        sys.exit(0)

    logging.info("Updated local files with version")

    logging.info("Build short name %s", build_name)

    build_doris_log = os.path.join(temp_path, "build_log")
    if not os.path.exists(build_doris_log):
        os.makedirs(build_doris_log)

    start = time.time()
    log_path, success = build_doris(build_doris_log, build_name)
    elapsed = int(time.time() - start)
    logging.info("Build finished with {}, log path {}, elapsed {}".format(success, log_path, elapsed))

    if not success:
        sys.exit(1)

    pack_git_info(repo_path)
    pack_build_results(build_name, repo_path)

    if os.path.exists(log_path):
        log_url = s3_helper.upload_build_file_to_s3(log_path, s3_path_prefix + "/" + os.path.basename(log_path))
        logging.info("Log url %s", log_url)
    else:
        logging.info("Build log doesn't exist")

    # Put repository and output to object storage
    doris_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir)
    build_urls = s3_helper.upload_build_folder_to_s3(f"{repo_path}/output", s3_path_prefix, upload_symlinks=False)
    logging.info("Got build URLs %s", build_urls)

    print("::notice ::Build URLs: {}".format('\n'.join(build_urls)))

    print("::notice ::Log URL: {}".format(log_url))

    create_json_artifact(temp_path, build_name, log_url, build_urls, elapsed, success)
    # Fail build job if not successeded
    if not success:
        sys.exit(1)
