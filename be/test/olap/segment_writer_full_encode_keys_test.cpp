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

#include <string>
#include <vector>

#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "util/key_util.h"
#include "util/slice.h"
#include "vec/common/string_view.h" // hex_dump
#include "vec/olap/olap_data_convertor.h"

namespace doris {
namespace segment_v2 {
using namespace doris::vectorized;

auto create_string_accessor(const std::vector<std::string>& str) {
    ColumnString::MutablePtr column = ColumnString::create();
    // ASSERT_TRUE(!str.empty());
    for (auto& s : str) column->insert_value(s);
    DataTypePtr data_type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_VARCHAR, 0, 0);
    ColumnWithTypeAndName typed_column(column->get_ptr(), data_type, "test_string_column");

    // Create a VARCHAR convertor, a convertor is an accessor
    auto convertor =
            std::make_shared<OlapBlockDataConvertor::OlapColumnDataConvertorVarChar>(false);
    convertor->set_source_column(typed_column, 0, str.size()); // row_pos=0, num_rows=str.size()

    // Convert to OLAP format
    auto status = convertor->convert_to_olap();
    EXPECT_TRUE(status.ok());
    if (status.ok()) {
        // Get the converted data
        const void* data = convertor->get_data_at(0);
        // const UInt8* nullmap = convertor->get_nullmap();
        std::cout << ((StringRef*)data)->to_string() << std::endl;
        std::cout << column->get_data_at(0) << std::endl;
        // Use the converted data as needed
    }
    return convertor;
}

auto create_int_accessor(const std::vector<PrimitiveTypeTraits<TYPE_BIGINT>::CppType>& values) {
    // ASSERT_TRUE(!values.empty());
    auto column = ColumnInt64::create();
    for (auto value : values) column->insert_value(value);
    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(TYPE_INT, 0, 0);
    ColumnWithTypeAndName typed_column(column->get_ptr(), data_type, "test_int_column");
    auto convertor =
            std::make_shared<OlapBlockDataConvertor::OlapColumnDataConvertorSimple<TYPE_BIGINT>>();
    convertor->set_source_column(typed_column, 0,
                                 values.size()); // row_pos=0, num_rows=values.size()
    auto status = convertor->convert_to_olap();
    EXPECT_TRUE(status.ok());
    return convertor;
}

TEST(SegmentWriterFullEncodeKeysTest, TestSegmentWriterKeyEncoding) {
    // 2 rows of key columns(int,string,string), expect encode bytes of row1 < row2
    //               0x05050505, a,     bb
    //               0x05050505, a\x01, cc
    // however the ending byte of 2nd row is \x01 (smaller than KEY_NORMAL_MARKER)
    // will be in reversed order after encoding
    auto int_accessor = create_int_accessor({0x05050505, 0x05050505});
    auto str_accessor0 = create_string_accessor({"a", "a\x01"});
    auto str_accessor1 = create_string_accessor({"bb", "cc"});
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns = {
            int_accessor.get(), str_accessor0.get(), str_accessor1.get()};
    auto int_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_INT);
    auto str_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    std::vector<const KeyCoder*> key_coders = {int_coder, str_coder, str_coder};
    ////////////////////////////////////////////////////////////////////////////
    std::string encoded0 = SegmentWriter::_full_encode_keys(key_coders, key_columns, 0);
    std::string encoded1 = SegmentWriter::_full_encode_keys(key_coders, key_columns, 1);
    ////////////////////////////////////////////////////////////////////////////
    std::cout << StringView(encoded0).dump_hex() << std::endl; // X'02850505050261026262'
    std::cout << StringView(encoded1).dump_hex() << std::endl; // X'0285050505026101026363'
    // EXPECT_LT(encoded0, encoded1); // BANG! not satisfied
}

} // namespace segment_v2
} // namespace doris
