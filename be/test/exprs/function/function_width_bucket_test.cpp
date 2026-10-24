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

#include <string>

#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "testutil/any_type.h"

namespace doris {

using namespace ut_type;

TEST(FunctionWidthBucketTest, width_bucket) {
    std::string func_name = "width_bucket";

    InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT,
                                PrimitiveType::TYPE_INT, Consted {PrimitiveType::TYPE_INT}};

    DataSet data_set = {
            {{9, 0, 10, 20}, BIGINT(19)}, // max - min < num_buckets
            {{3, 0, 10, 20}, BIGINT(7)},
            {{1, 0, 2, 5}, BIGINT(3)},
            {{0, 0, 10, 20}, BIGINT(1)},
    };

    for (const auto& row : data_set) {
        DataSet one = {row};
        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, one));
    }
}

} // namespace doris
