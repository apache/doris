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

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util/slice.h"
#include "vec/exec/format/csv/csv_reader.h"

namespace doris {
namespace vectorized {

class CsvReaderTest : public testing::Test {
public:
    CsvReaderTest() {}
};

TEST_F(CsvReaderTest, multibyte_sep_with_null) {
    bool trim_double_quotes = false;
    bool trim_tailing_spaces = false;
    std::string value_separator = "WWWWW";
    int value_separator_length = value_separator.size();
    doris::Slice line_data = "1900-02-04WWWWW51639WWWWWWWWWW886";
    std::vector<doris::Slice> split_values;
    PlainCsvTextFieldSplitter fields_splitter = PlainCsvTextFieldSplitter>(
                trim_tailing_spaces, trim_double_quotes, value_separator,
                value_separator_length);
    fields_splitter.split_line(line_data, &split_values);
    ASSERT_EQ(split_values.size(), 4);
	int idx = 0;
    EXPECT_EQ(split_values[idx++].to_string(), "1900-02-04");
    EXPECT_EQ(split_values[idx++].to_string(), "51639");
    EXPECT_EQ(split_values[idx++].to_string(), "");
    EXPECT_EQ(split_values[idx++].to_string(), "886");
}

} // namespace vectorized
} // namespace doris
