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

#include "format/csv/csv_reader.h"

#include <memory>
#include <string>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"

namespace doris {
namespace {

const std::vector<SlotDescriptor*>& empty_slot_descs() {
    static const std::vector<SlotDescriptor*> slots;
    return slots;
}

class CsvReaderForTest : public CsvReader {
public:
    CsvReaderForTest(const TFileScanRangeParams& params, const TFileRangeDesc& range)
            : CsvReader(nullptr, nullptr, nullptr, params, range, empty_slot_descs(), nullptr) {}

    using CsvReader::_deserialize_nullable_string;
    using CsvReader::_init_options;
    using CsvReader::is_enclosed_csv_field;
};

TFileScanRangeParams make_csv_params(bool empty_field_as_null) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_compress_type(TFileCompressType::UNKNOWN);
    params.__set_file_type(TFileType::FILE_LOCAL);
    params.file_attributes.text_params.column_separator = ",";
    params.file_attributes.text_params.line_delimiter = "\n";
    params.file_attributes.text_params.__isset.null_format = true;
    params.file_attributes.text_params.null_format = "\\N";
    params.file_attributes.text_params.__isset.empty_field_as_null = true;
    params.file_attributes.text_params.empty_field_as_null = empty_field_as_null;
    params.file_attributes.__isset.trim_double_quotes = true;
    params.file_attributes.trim_double_quotes = true;
    return params;
}

} // namespace

TEST(CsvReaderTest, EnclosedFieldDetectionTracksTrimmedQuotes) {
    std::string quoted_null = "\"\\N\"";
    EXPECT_TRUE(CsvReaderForTest::is_enclosed_csv_field(Slice(quoted_null), 0, quoted_null.size(),
                                                        '"', false));

    std::string quoted_null_with_spaces = "\"\\N\"   ";
    EXPECT_TRUE(CsvReaderForTest::is_enclosed_csv_field(Slice(quoted_null_with_spaces), 0,
                                                        quoted_null_with_spaces.size(), '"', true));
    EXPECT_FALSE(CsvReaderForTest::is_enclosed_csv_field(
            Slice(quoted_null_with_spaces), 0, quoted_null_with_spaces.size(), '"', false));

    std::string bare_null = "\\N";
    EXPECT_FALSE(CsvReaderForTest::is_enclosed_csv_field(Slice(bare_null), 0, bare_null.size(), '"',
                                                         false));
}

TEST(CsvReaderTest, NullableStringKeepsQuotedNullLiteralAsString) {
    TFileScanRangeParams params = make_csv_params(true);
    TFileRangeDesc range;
    CsvReaderForTest reader(params, range);
    ASSERT_TRUE(reader._init_options().ok());

    auto column = make_nullable(std::make_shared<DataTypeString>())->create_column();
    auto& nullable_column = assert_cast<ColumnNullable&>(*column);

    std::string bare_null_literal = "\\N";
    Slice bare_null_slice(bare_null_literal);
    EXPECT_TRUE(reader._deserialize_nullable_string(*column, bare_null_slice, false).ok());
    EXPECT_TRUE(nullable_column.is_null_at(0));

    std::string enclosed_null_literal = "\\N";
    Slice enclosed_null_slice(enclosed_null_literal);
    EXPECT_TRUE(reader._deserialize_nullable_string(*column, enclosed_null_slice, true).ok());
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_EQ(nullable_column.get_nested_column().get_data_at(1).to_string(), "\\N");

    std::string plain_quoted_null_literal = "\"\\N\"";
    Slice plain_quoted_null_slice(plain_quoted_null_literal);
    EXPECT_TRUE(reader._deserialize_nullable_string(*column, plain_quoted_null_slice, false).ok());
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_EQ(nullable_column.get_nested_column().get_data_at(2).to_string(), "\\N");

    std::string enclosed_empty_string;
    Slice enclosed_empty_slice(enclosed_empty_string);
    EXPECT_TRUE(reader._deserialize_nullable_string(*column, enclosed_empty_slice, true).ok());
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_EQ(nullable_column.get_nested_column().get_data_at(3).to_string(), "");
}

} // namespace doris
