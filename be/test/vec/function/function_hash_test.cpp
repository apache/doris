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

#include <stdint.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(HashFunctionTest, murmur_hash_3_test) {
    std::string func_name = "murmur_hash3_32";

    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{Null()}, Null()}, {{std::string("hello")}, (int32_t)1321743225}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world")}, (int32_t)984713481},
                            {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int32_t)-666935433},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash_3_64_test) {
    std::string func_name = "murmur_hash3_64";

    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("hello")}, (int64_t)-3215607508166160593}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("hello"), std::string("world")}, (int64_t)3583109472027628045},
                {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int64_t)1887828212617890932},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash_2_test) {
    std::string func_name = "murmurHash2_64";

    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("hello")}, (uint64_t)2191231550387646743ull}};

        static_cast<void>(check_function<DataTypeUInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("hello"), std::string("world")}, (uint64_t)11978658642541747642ull},
                {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeUInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (uint64_t)1367324781703025231ull},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeUInt64, true>(func_name, input_types, data_set));
    };
}

} // namespace doris::vectorized
