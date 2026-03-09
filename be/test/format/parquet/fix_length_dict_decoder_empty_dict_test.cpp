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

#include "core/column/column_vector.h"
#include "format/parquet/fix_length_dict_decoder.hpp"

namespace doris {

class FixLengthDictDecoderEmptyDictDataTest : public ::testing::Test {
protected:
    void SetUp() override {
        _type_length = 6;
        _decoder.set_type_length(_type_length);
        auto dict_data = make_unique_buffer<uint8_t>(0);
        ASSERT_TRUE(_decoder.set_dict(dict_data, 0, 0).ok());
    }

    FixLengthDictDecoder<tparquet::Type::FIXED_LEN_BYTE_ARRAY> _decoder;
    size_t _type_length;
};

TEST_F(FixLengthDictDecoderEmptyDictDataTest,
       test_convert_dict_column_to_string_column_with_empty_dict_data_error) {
    MutableColumnPtr dict_column = ColumnInt32::create();
    dict_column->insert(Field::create_field<TYPE_INT>(0));
    dict_column->insert(Field::create_field<TYPE_INT>(1));

    auto io_error = TEST_RESULT_ERROR(_decoder.convert_dict_column_to_string_column(
            assert_cast<ColumnInt32*>(dict_column.get())));
    ASSERT_TRUE(io_error.is<ErrorCode::IO_ERROR>());
}

TEST_F(FixLengthDictDecoderEmptyDictDataTest,
       test_convert_dict_column_to_string_column_with_empty_dict_data_success) {
    MutableColumnPtr dict_column = ColumnInt32::create();

    auto string_column = TEST_TRY(_decoder.convert_dict_column_to_string_column(
            assert_cast<ColumnInt32*>(dict_column.get())));

    ASSERT_EQ(string_column->size(), 0);
}

} // namespace doris
