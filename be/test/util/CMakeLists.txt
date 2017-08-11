# Modifications copyright (C) 2017, Baidu.com, Inc.
# Copyright 2017 The Apache Software Foundation

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

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BINARY_DIR}/test/util")

#ADD_BE_TEST(integer-array-test)
#ADD_BE_TEST(runtime_profile_test)
#ADD_BE_TEST(benchmark-test)
#ADD_BE_TEST(decompress-test)
#ADD_BE_TEST(metrics-test)
#ADD_BE_TEST(debug-util-test)
#ADD_BE_TEST(url-coding-test)
#ADD_BE_TEST(thrift-util-test)
#ADD_BE_TEST(bit-util-test)
#ADD_BE_TEST(rle-test)
##ADD_BE_TEST(perf-counters-test)
ADD_BE_TEST(path_trie_test)
ADD_BE_TEST(count_down_latch_test)
ADD_BE_TEST(lru_cache_util_test)
ADD_BE_TEST(filesystem_util_test)
ADD_BE_TEST(internal_queue_test)
