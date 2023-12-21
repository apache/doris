// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/integral_types.h"
#include "testutil/any_type.h"
#include "util/bitmap_value.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_hll.h"
namespace doris::vectorized {

TEST(function_unhex_test, function_unhex_to_bitmap_test) {
    std::string func_name = "unhex_to_bitmap";

    BitmapValue bitmap1((uint64_t)0);
    BitmapValue bitmap2((uint64_t)6);
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{Null()}, {Null()}},
                        {{std::string("")}, Null()},
                        {{std::string("@!#")}, Null()},
                        {{std::string("ò&ø")}, Null()},
                        {{std::string("@@")}, Null()},
                        {{std::string("61")}, Null()},
                        {{std::string("0100000000")}, &bitmap1},
                        {{std::string("0106000000")}, &bitmap2}};
    check_function<DataTypeBitMap, true>(func_name, input_types, data_set);
}

TEST(function_unhex_test, function_unhex_to_hll_test) {
    std::string func_name = "unhex_to_hll";

    char data1[5] = {'1', '0', '0', '0', '0'};
    char data2[5] = {'1', '6', '0', '0', '0'};

    HyperLogLog empty_hll;
    HyperLogLog hll1({data1, 5});
    HyperLogLog hll2({data2, 5});
    InputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{{Null()}, &empty_hll},
                        {{std::string("")}, &empty_hll},
                        {{std::string("@!#")}, &empty_hll},
                        {{std::string("ò&ø")}, &empty_hll},
                        {{std::string("@@")}, &empty_hll},
                        {{std::string("61")}, &empty_hll},
                        {{std::string("0100000000")}, &hll1},
                        {{std::string("0106000000")}, &hll2}};
    check_function<DataTypeHLL, false>(func_name, input_types, data_set);
}


} // namespace doris::vectorized
