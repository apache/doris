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

# shellcheck disable=SC2034
BUILD_FS_BENCHMARK=ON
ENABLE_EXPR_ASAN=OFF
# The TeamCity compile stage copies this file to the repo root and only appends
# BUILD_TYPE later, so source-level ASAN selections must live here.
DORIS_BE_ASAN_SOURCES=be/src/exprs/vexpr.cpp,be/src/exec/runtime_filter/utils.cpp,be/src/exec/runtime_filter/runtime_filter_consumer.cpp
