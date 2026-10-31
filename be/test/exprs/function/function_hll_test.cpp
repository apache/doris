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
#include <deque>
#include <string>
#include <utility>

#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

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

namespace {
// Keep a byte buffer alive and hand back a non-owning VARBINARY view over it.
// ut_type::VARBINARY is a StringView that does not own its bytes and StringView rejects
// std::string&&, so dynamic payloads must be parked somewhere stable for the lifetime of
// the test process; check_function copies the bytes into the input column immediately.
// deque never relocates its elements (unlike vector), so a view over an SSO string inside it
// stays valid across later insertions.
ut_type::VARBINARY keep_alive_binary(std::string bytes) {
    static std::deque<std::string> keep_alive;
    keep_alive.push_back(std::move(bytes));
    return ut_type::VARBINARY(keep_alive.back().data(), keep_alive.back().size());
}

// Serialize an HLL to the Doris wire format (HyperLogLog::serialize), used to build
// hll_from_binary inputs and hll_to_binary expectations.
ut_type::VARBINARY serialize_hll_to_binary(const HyperLogLog& hll) {
    std::string serialized(hll.max_serialized_size(), '\0');
    size_t real_size = hll.serialize(reinterpret_cast<uint8_t*>(serialized.data()));
    serialized.resize(real_size);
    return keep_alive_binary(std::move(serialized));
}
} // namespace

TEST(function_hll_test, function_hll_to_binary_test) {
    std::string func_name = "hll_to_binary";
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

    HyperLogLog empty_hll;

    DataSet data_set = {{{&hll1}, VARBINARY(serialize_hll_to_binary(hll1))},
                        {{&hll2}, VARBINARY(serialize_hll_to_binary(hll2))},
                        {{&hll3}, VARBINARY(serialize_hll_to_binary(hll3))},
                        {{&empty_hll}, VARBINARY(serialize_hll_to_binary(empty_hll))},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeVarbinary, true>(func_name, input_types, data_set));
}

TEST(function_hll_test, function_hll_from_binary_test) {
    std::string func_name = "hll_from_binary";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY};

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

    HyperLogLog empty_hll;

    {
        // valid payloads round-trip back to the original HLLs
        DataSet data_set = {{{VARBINARY(serialize_hll_to_binary(hll1))}, &hll1},
                            {{VARBINARY(serialize_hll_to_binary(hll2))}, &hll2},
                            {{VARBINARY(serialize_hll_to_binary(hll3))}, &hll3},
                            {{VARBINARY(serialize_hll_to_binary(empty_hll))}, &empty_hll},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeHLL, true>(func_name, input_types, data_set));
    }

    {
        // invalid / truncated user-controlled bytes must yield NULL, never crash:
        // - empty payload (no type code at all)
        // - unknown type code
        // - EXPLICIT payload cut short
        // - EXPLICIT with a count larger than the actual bytes
        // - SPARSE with a register count larger than the actual bytes
        // - SPARSE with a well-formed length but a register index past the register array
        // - FULL payload shorter than the full register array
        std::string explicit_bytes = serialize_hll_to_binary(hll3);
        explicit_bytes.resize(1); // type byte only
        std::string explicit_bad_count = explicit_bytes;
        explicit_bad_count += static_cast<char>(8);
        explicit_bad_count.resize(2); // num_explicits = 8 but no hash bytes follow
        std::string sparse_bad = std::string("\x02", 1);
        sparse_bad += std::string("\xff\xff\xff\xff", 4); // huge num_registers
        // type=SPARSE, num_registers=1, entry: index=16384 (== HLL_REGISTERS_COUNT), value=1
        std::string sparse_oob_index = std::string("\x02\x01\x00\x00\x00\x00\x40\x01", 8);
        // entry: index=65535, value=1
        std::string sparse_oob_index_max = std::string("\x02\x01\x00\x00\x00\xff\xff\x01", 8);
        std::string full_short = std::string(4, '\0');
        full_short[0] = HLL_DATA_FULL;

        DataSet data_set = {
                {{VARBINARY(keep_alive_binary(std::string()))}, Null()},
                {{VARBINARY(keep_alive_binary(std::string("\x7f", 1)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(explicit_bytes)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(explicit_bad_count)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(sparse_bad)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(sparse_oob_index)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(sparse_oob_index_max)))}, Null()},
                {{VARBINARY(keep_alive_binary(std::move(full_short)))}, Null()}};

        static_cast<void>(check_function<DataTypeHLL, true>(func_name, input_types, data_set));
    }
}
} // namespace doris
