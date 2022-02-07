#!/bin/bash

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
# https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/performance-comparison/entrypoint.sh
# and modified by Doris.

set -ex

DPC_CHECK_START_TIMESTAMP="$(date +%s)"
export DPC_CHECK_START_TIMESTAMP

# Start the main comparison script.
{ \
    time ./download.sh && \
    time stage=configure ./compare.sh ; \
} 2>&1 | tee compare.log

cp compare.log /output
