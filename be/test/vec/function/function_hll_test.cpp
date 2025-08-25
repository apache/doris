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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(function_hll_test, function_hll_cardinality_test) {
    std::string func_name = "hll_cardinality";
    InputTypeSet input_types = {PrimitiveType::TYPE_HLL};

    const std::string input1 = "test";
    const uint64_t hash_value1 =
            HashUtil::murmur_hash64A(input1.data(), input1.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll1(hash_value1);

    const std::string input2 = " ";
    const uint64_t hash_value2 =
            HashUtil::murmur_hash64A(input2.data(), input2.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll2(hash_value2);

    HyperLogLog hll3(HLL_DATA_EXPLICIT);
    hll3.update(hash_value1);
    hll3.update(hash_value2);

    // we update the same hash value twice, the result should be the same as update once, which is 2
    HyperLogLog hll4(HLL_DATA_EXPLICIT);
    hll4.update(hash_value1);
    hll4.update(hash_value1);

    HyperLogLog empty_hll;

    DataSet data_set = {{{&hll1}, (int64_t)1}, {{&hll2}, (int64_t)1},      {{&hll3}, (int64_t)3},
                        {{&hll4}, (int64_t)2}, {{&empty_hll}, (int64_t)0}, {{Null()}, (int64_t)0}};

    static_cast<void>(check_function<DataTypeInt64>(func_name, input_types, data_set));
}

TEST(function_hll_test, function_hll_to_base64_test) {
    std::string func_name = "hll_to_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_HLL};

    const std::string input1 = "test";
    const uint64_t hash_value1 =
            HashUtil::murmur_hash64A(input1.data(), input1.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll1(hash_value1);

    const std::string input2 = " ";
    const uint64_t hash_value2 =
            HashUtil::murmur_hash64A(input2.data(), input2.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll2(hash_value2);

    HyperLogLog hll3;
    hll3.update(hash_value1);
    hll3.update(hash_value2);

    // Although the hll4 update the hash_value1 twice, the result should be the same as update once.
    HyperLogLog hll4;
    hll4.update(hash_value1);
    hll4.update(hash_value2);
    hll4.update(hash_value1);

    HyperLogLog empty_hll;

    DataSet data_set = {{{&hll1}, std::string("AQHm5IIJCx0h/w==")},
                        {{&hll2}, std::string("AQG/Hk98sO59Sw==")},
                        {{&hll3}, std::string("AQLm5IIJCx0h/78eT3yw7n1L")},
                        {{&hll4}, std::string("AQLm5IIJCx0h/78eT3yw7n1L")},
                        {{&empty_hll}, std::string("AA==")},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_hll_test, function_hll_from_base64_test) {
    std::string func_name = "hll_from_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    const std::string input1 = "AQHm5IIJCx0h/w==";
    const std::string output1 = "test";
    const uint64_t hash_value1 =
            HashUtil::murmur_hash64A(output1.data(), output1.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll1(hash_value1);

    const std::string input2 = "AQG/Hk98sO59Sw==";
    const std::string output2 = " ";
    const uint64_t hash_value2 =
            HashUtil::murmur_hash64A(output2.data(), output2.size(), HashUtil::MURMUR_SEED);
    HyperLogLog hll2(hash_value2);

    const std::string input3 = "AQLm5IIJCx0h/78eT3yw7n1L";
    HyperLogLog hll3;
    hll3.update(hash_value1);
    hll3.update(hash_value2);

    // Although the hll4 update the hash_value1 twice, the result should be the same as update once.
    const std::string input4 = input3;
    HyperLogLog hll4;
    hll4.update(hash_value1);
    hll4.update(hash_value2);
    hll4.update(hash_value1);

    const std::string input5 = "AA==";
    HyperLogLog empty_hll;

    DataSet data_set = {{{input1}, &hll1},
                        {{input2}, &hll2},
                        {{input3}, &hll3},
                        {{input4}, &hll4},
                        {{input5}, &empty_hll}};

    static_cast<void>(check_function<DataTypeHLL, true>(func_name, input_types, data_set));
}
} // namespace doris::vectorized
