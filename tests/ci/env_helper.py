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
# https://github.com/ClickHouse/ClickHouse/blob/master/tests/ci/env_helper.py
# and modified by Doris.

import os

CI = bool(os.getenv("CI"))
TEMP_PATH = os.getenv("TEMP_PATH", os.path.abspath("."))

CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
# The tests should be driven by github action in the future. Now we just
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH", "github_event_example.json")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "apache/incubator-doris")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID")
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../"))
IMAGES_PATH = os.getenv("IMAGES_PATH")
REPORTS_PATH = os.getenv("REPORTS_PATH", "./reports")
REPO_COPY = os.getenv("REPO_COPY", os.path.abspath("../../"))
RUNNER_TEMP = os.getenv("RUNNER_TEMP", os.path.abspath("./tmp"))
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "doris-build")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://doris-build.oss-cn-beijing-internal.aliyuncs.com")
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "doris-test-reports")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "need_set")
