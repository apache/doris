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

CUR_DIR=$(dirname "$0")
CUR_DIR=$(
  cd "$CUR_DIR" || exit
  pwd
)
FE_CMD=${CUR_DIR}/fe.sh
# In order to be compatible with the previous version, start in non-daemon mode by default
# for new user please use fe.sh directly
${FE_CMD} --start --nodaemon "$@"

