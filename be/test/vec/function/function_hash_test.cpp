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

TEST(HashFunctionTest, xxhash_32_test) {
    std::string func_name = "xxhash_32";

    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{Null()}, Null()}, {{std::string("hello")}, (int32_t)-83855367}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world")}, (int32_t)-920844969},
                            {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int32_t)352087701},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, xxhash_64_test) {
    std::string func_name = "xxhash_64";

    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("hello")}, (int64_t)-7685981735718036227}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("hello"), std::string("world")}, (int64_t)7001965798170371843},
                {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int64_t)6796829678999971400},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };
}

} // namespace doris::vectorized
