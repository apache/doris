<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CI/CD

The directory(tests/ci) contains scripts used by ci/cd, assuming ci/cd will be fired by github action. For now github action is not configured, so the environment var(GITHUB_EVENT_PATH) stores a file path in which a github webhook event content is stored in.

build_check.py builds doris in docker and uploads built binaries to OSS. The path of OSS contains pr number and sha, which is parsed from the file GITHUB_EVENT_PATH. performance_comparison_check.py downloads the binary to test and the binary to ref and runs performance tests with two binaries, then the two results is compared. The results is stored in performance_result/output.7z and also uploaded to oss.

You can run tests following below steps.

- Get a machine which has role to access doris-build bucket of OSS(Alibaba Cloud object storage service).
- Clone doris source code from github and cd to tests/ci.
- Copy a webhook event content from github and store it in the file of which the path is set to the env var(GITHUB_EVENT_PATH), the default value of GITHUB_EVENT_PATH is in env_helper.py.
- Run python3 build_check.py performance to prepare binary. Note that performance is an argument of build_check.py and use it for performance comparison. ASAN and others will be add in the future to support running tests with ASAN enabled.
- Set env var GITHUB_TOKEN to your github token string.
- Run python3 performance_comparison_check.py performance. Note that the first run will fail while downloading a binary, because performance comparison needs two binaries, you need copy the binary produced in the former step to the destination path in oss by using s3_helper.py. For example s3_helper.upload_build_folder_to_s3("../../output", "0/4f6d13a765cbc39e47194521febf999ac29f0de5/performance", upload_symlinks=False). The destination path can be found in error message.
- Run python3 performance_comparison_check.py performance again.
