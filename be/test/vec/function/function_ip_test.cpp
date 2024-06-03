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

#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(FunctionIpTest, FunctionIsIPAddressInRangeTest) {
    std::string func_name = "is_ip_address_in_range";

    DataSet data_set = {
            {{std::string("127.0.0.1"), std::string("127.0.0.0/8")}, (uint8_t)1},
            {{std::string("128.0.0.1"), std::string("127.0.0.0/8")}, (uint8_t)0},
            {{std::string("ffff::1"), std::string("ffff::/16")}, (uint8_t)1},
            {{std::string("fffe::1"), std::string("ffff::/16")}, (uint8_t)0},
            {{std::string("192.168.99.255"), std::string("192.168.100.0/22")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.103.255"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.104.0"), std::string("192.168.100.0/22")}, (uint8_t)0},
            {{std::string("::192.168.99.255"), std::string("::192.168.100.0/118")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.103.255"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.104.0"), std::string("::192.168.100.0/118")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/24")}, (uint8_t)1},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/32")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/120")}, (uint8_t)1},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/128")}, (uint8_t)0},
            {{std::string("192.168.100.1"), std::string("192.168.100.0/22")}, (uint8_t)1},
            {{std::string("192.168.103.255"), std::string("192.168.100.0/24")}, (uint8_t)0},
            {{std::string("::192.168.100.1"), std::string("::192.168.100.0/118")}, (uint8_t)1},
            {{std::string("::192.168.103.255"), std::string("::192.168.100.0/120")}, (uint8_t)0},
            {{std::string("127.0.0.1"), std::string("ffff::/16")}, (uint8_t)0},
            {{std::string("127.0.0.1"), std::string("::127.0.0.1/128")}, (uint8_t)0},
            {{std::string("::1"), std::string("127.0.0.0/8")}, (uint8_t)0},
            {{std::string("::127.0.0.1"), std::string("127.0.0.1/32")}, (uint8_t)0}};

    {
        // vector vs vector
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }

    {
        // vector vs scalar
        InputTypeSet input_types = {TypeIndex::String, Consted {TypeIndex::String}};
        for (const auto& line : data_set) {
            DataSet const_cidr_dataset = {line};
            static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types,
                                                                  const_cidr_dataset));
        }
    }

    {
        // scalar vs vector
        InputTypeSet input_types = {Consted {TypeIndex::String}, TypeIndex::String};
        for (const auto& line : data_set) {
            DataSet const_addr_dataset = {line};
            static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types,
                                                                  const_addr_dataset));
        }
    }
}

} // namespace doris::vectorized
