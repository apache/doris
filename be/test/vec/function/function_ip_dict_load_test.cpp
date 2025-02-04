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

#include <fstream>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>
#include <vector>

#include "function_ip_dict_test.h"
#include "function_test_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/ip_address_dictionary.h"

namespace doris::vectorized {

std::vector<std::string> splitString(const std::string& input, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream stream(input);
    while (std::getline(stream, token, delimiter)) {
        tokens.push_back(token);
    }

    return tokens;
}

std::string trim_string(const std::string& str) {
    auto start = std::find_if_not(str.begin(), str.end(),
                                  [](unsigned char ch) { return std::isspace(ch); });

    auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char ch) {
                   return std::isspace(ch);
               }).base();

    std::string trimmed = (start < end ? std::string(start, end) : std::string());

    if (!trimmed.empty() && trimmed.back() == '\"') {
        trimmed.pop_back();
    }
    if (!trimmed.empty() && trimmed.front() == '\"') {
        trimmed.erase(0, 1);
    }

    return trimmed;
}

struct CsvInfo {
    std::string name;
    std::vector<std::string> infos;
};

std::vector<CsvInfo> load_from_csv(const std::string file_name) {
    std::vector<CsvInfo> result;
    std::ifstream file(file_name);

    if (!file.is_open()) {
        throw std::runtime_error("Could not open file");
    }

    std::string line;
    std::getline(file, line);
    auto headers = splitString(line, ',');
    for (auto head : headers) {
        result.emplace_back(trim_string(head), std::vector<std::string>());
    }
    while (std::getline(file, line)) {
        auto values = splitString(line, ',');
        if (values.size() != headers.size()) {
            throw std::runtime_error("Invalid csv format");
        }

        for (size_t i = 0; i < headers.size(); ++i) {
            result[i].infos.push_back(trim_string(values[i]));
        }
    }
    file.close();
    return result;
}

ColumnWithTypeAndName column_string_from_csv(CsvInfo csv) {
    auto column_str = ColumnString::create();
    for (auto& str : csv.infos) {
        column_str->insert_value(str);
    }
    auto name = csv.name;
    return ColumnWithTypeAndName {column_str->clone(), std::make_shared<DataTypeString>(), name};
}

template <bool is_mock>
auto load_dict() {
    std::string file_path =
            std::string(__FILE__).substr(0, std::string(__FILE__).find_last_of("/\\") + 1) +
            "ip_dict.csv";
    auto data = load_from_csv(file_path);

    ColumnWithTypeAndName key_data = column_string_from_csv(data[0]);
    ColumnsWithTypeAndName value_data;
    for (int i = 1; i < data.size(); i++) {
        value_data.push_back(column_string_from_csv(data[i]));
    }

    if constexpr (is_mock) {
        return create_mock_ip_trie_dict_from_column("mock_ip_dict", key_data, value_data);
    } else {
        return create_ip_trie_dict_from_column("ip_dict", key_data, value_data);
    }
}

TEST(IpDictLoadTest, Test) {
    auto ip_dict = load_dict<false>();
    auto mock_ip_dict = load_dict<true>();

    std::vector<std::string> ip_string = {
            "119.140.240.0", "119.141.0.0",   "119.142.0.0",   "119.142.128.0", "119.142.240.0",
            "119.144.0.0",   "119.144.128.0", "119.145.6.0",   "119.145.76.0",  "119.145.82.0",
            "119.146.30.0",  "119.146.212.0", "119.146.240.0", "119.146.248.0", "121.8.0.0",
            "121.8.136.0",   "121.8.193.0",   "121.8.204.0",   "121.8.238.0",   "121.9.0.0",
            "121.9.16.0",    "121.9.32.0",    "121.9.64.0",    "121.9.80.0",    "121.9.96.0",
            "121.9.110.0",   "121.10.48.0",   "121.10.64.0"};

    auto key_type = std::make_shared<DataTypeIPv4>();
    auto ipv_column = DataTypeIPv4::ColumnType::create();
    for (const auto& ip : ip_string) {
        IPv4 ipv4;
        EXPECT_TRUE(IPv4Value::from_string(ipv4, ip));
        ipv_column->insert_value(ipv4);
    }

    std::string attribute_name = "org_name";
    auto attribute_type = std::make_shared<DataTypeString>();

    ColumnPtr key_column = ipv_column->clone();
    auto mock_result =
            mock_ip_dict->getColumn(attribute_name, attribute_type, key_column, key_type);
    auto result = ip_dict->getColumn(attribute_name, attribute_type, key_column, key_type);

    for (int i = 0; i < result->size(); i++) {
        auto mock_str = mock_result->get_data_at(i).to_string();
        auto str = result->get_data_at(i).to_string();
        EXPECT_EQ(mock_str, str);
    }
}

} // namespace doris::vectorized