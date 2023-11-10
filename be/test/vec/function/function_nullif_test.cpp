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

#include <iomanip>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(NullIfTest, Int_Test) {
    std::string func_name = "nullif";
    InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};
    DataSet data_set = {{{4, 10}, 4}, {{-4, -4}, Null()}, {{5, Null()}, 5}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(NullIfTest, Float_Test) {
    std::string func_name = "nullif";
    InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};
    DataSet data_set = {{{4.0, 10.0}, 4.0}, {{-4.0, -4.0}, Null()}, {{5.0, Null()}, 5.0}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(NullIfTest, String_Int_Test) {
    std::string func_name = "nullif";
    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::DateTime};
    DataSet data_set = {
            {{std::string("2021-10-24 12:32:31"), std::string("2021-10-24 13:00:01")},
             str_to_date_time("2021-10-24 12:32:31")},
            {{std::string("2021-10-24 13:00:01"), std::string("2021-10-24 13:00:01")}, Null()},
            {{std::string("2021-10-24 13:00:01"), Null()},
             str_to_date_time("2021-10-24 13:00:01")}};

    static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
