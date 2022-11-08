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

#include "vec/columns/column_jsonb.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"

namespace doris::vectorized {

JsonBinaryValue FromStdString(const std::string& str) {
    char* ptr = const_cast<char*>(str.c_str());
    int len = str.size();
    return JsonBinaryValue(ptr, len);
}

TEST(ColumnJsonbTest, SingleValueTest) {
    auto data_column = ColumnJsonb::create();

    std::vector<IColumn::Offset> offs = {0};
    std::vector<std::string> vals = {"[\"val1\", \"val2\"]", "[false]",
                                     "{\"key1\": \"js6\", \"key2\": [\"val1\", \"val2\"]}"};
    for (size_t i = 0; i < vals.size(); i++) {
        JsonBinaryValue v = FromStdString(vals[i]);
        if (i) {
            offs.push_back(offs[i - 1] + v.size());
        }
        data_column->insert_data(v.value(), v.size());
    }

    for (size_t i = 0; i < offs.size(); i++) {
        auto v = data_column->get_data_at(offs[i]);
        std::string json_str = JsonbToJson::jsonb_to_json_string(v.data, v.size);
        EXPECT_EQ(vals[i], json_str);
    }
}
} // namespace doris::vectorized
