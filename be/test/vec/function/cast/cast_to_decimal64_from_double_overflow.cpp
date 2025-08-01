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

#include "cast_to_decimal.h"

namespace doris::vectorized {
TEST_F(FunctionCastToDecimalTest, test_to_decimal64_from_double_overflow) {
    int table_index = 0;
    int test_data_index = 0;

    std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
    std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
    std::string regression_case_name = "test_cast_to_decimal64_from_double_overflow";
    if (FLAGS_gen_regression_case) {
        setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                     ofs_const_expected_result_uptr, ofs_case_uptr,
                                     ofs_expected_result_uptr, "to_decimal/from_float");
    }
    auto* ofs_const_case = ofs_const_case_uptr.get();
    auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
    auto* ofs_case = ofs_case_uptr.get();
    auto* ofs_expected_result = ofs_expected_result_uptr.get();
    if (FLAGS_gen_regression_case) {
        (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
    }
    from_float_double_overflow_test_func<TYPE_DOUBLE, Decimal64>(
            18, 0, table_index++, test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
            ofs_const_expected_result);
    from_float_double_overflow_test_func<TYPE_DOUBLE, Decimal64>(
            18, 9, table_index++, test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
            ofs_const_expected_result);
    from_float_double_overflow_test_func<TYPE_DOUBLE, Decimal64>(
            18, 17, table_index++, test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
            ofs_const_expected_result);
    if (FLAGS_gen_regression_case) {
        (*ofs_const_case) << "}";
        (*ofs_case) << "}";
    }
}
} // namespace doris::vectorized