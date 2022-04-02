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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/exprs")

ADD_BE_TEST(json_function_test)
#ADD_BE_TEST(binary_predicate_test)
#ADD_BE_TEST(in_predicate_test)
#ADD_BE_TEST(expr-test)
#ADD_BE_TEST(hybrid_set_test)
ADD_BE_TEST(string_functions_test)
ADD_BE_TEST(timestamp_functions_test)
ADD_BE_TEST(percentile_approx_test)
ADD_BE_TEST(percentile_test)
ADD_BE_TEST(bitmap_function_test)
ADD_BE_TEST(hll_function_test)
ADD_BE_TEST(encryption_functions_test)
#ADD_BE_TEST(in-predicate-test)
ADD_BE_TEST(math_functions_test)
ADD_BE_TEST(topn_function_test)
ADD_BE_TEST(runtime_filter_test)
ADD_BE_TEST(bloom_filter_predicate_test)
ADD_BE_TEST(array_functions_test)
ADD_BE_TEST(window_funnel_test)
