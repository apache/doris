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

#include "vec/functions/function_multi_match.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class FunctionMultiMatchTest : public testing::Test {
protected:
    void SetUp() override {
        auto query_type_col = ColumnString::create();
        query_type_str = "phrase";
        query_type_col->insert_data(query_type_str.data(), query_type_str.size());
        ColumnWithTypeAndName arg0;
        arg0.column = std::move(query_type_col);
        arg0.type = std::make_shared<DataTypeString>();
        arguments.push_back(arg0);

        auto query_str_col = ColumnString::create();
        query_str = "test query";
        query_str_col->insert_data(query_str.data(), query_str.size());
        ColumnWithTypeAndName arg1;
        arg1.column = std::move(query_str_col);
        arg1.type = std::make_shared<DataTypeString>();
        arguments.push_back(arg1);

        data_type_with_names.emplace_back("test_column", DataTypePtr());

        iterators.push_back(nullptr);
    }

    std::string query_type_str;
    std::string query_str;
    ColumnsWithTypeAndName arguments;
    std::vector<IndexFieldNameAndTypePair> data_type_with_names;
    std::vector<segment_v2::IndexIterator*> iterators;
};

TEST_F(FunctionMultiMatchTest, EvaluateInvertedIndexWithNullIterator) {
    uint32_t num_rows = 100;
    segment_v2::InvertedIndexResultBitmap bitmap_result;

    FunctionMultiMatch function;
    Status status = function.evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                                     num_rows, bitmap_result);

    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);

    std::string error_msg = status.to_string();
    EXPECT_NE(error_msg.find("test_column"), std::string::npos)
            << "Error message should contain column name. Actual message: " << error_msg;
}

} // namespace doris::vectorized
