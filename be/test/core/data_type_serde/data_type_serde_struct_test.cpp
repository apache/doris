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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_struct.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "storage/olap_common.h"
#include "testutil/test_util.h"

namespace doris {
static auto serde_int32 = std::make_shared<DataTypeNumberSerDe<TYPE_INT>>();
static auto serde_str = std::make_shared<DataTypeStringSerDe>(TYPE_STRING);

class DataTypeStructSerDeTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {}
};

TEST_F(DataTypeStructSerDeTest, GetName) {
    DataTypeStructSerDe serde_struct({serde_int32, serde_str}, {"id", "name"});

    EXPECT_EQ(serde_struct.get_name(), "Struct(id:INT, name:String)");
}

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeStructSerDeTest, ArrowMemNotAligned) {
    // 1.Prepare the data.
    std::vector<int32_t> int_data = {1, 2, 3, 4, 5, 6};
    std::vector<std::string> string_data = {"hello", "world", "test", "data", "arrow", "struct"};

    std::vector<int32_t> string_offsets = {0};
    int32_t current_string_offset = 0;
    for (const auto& str : string_data) {
        current_string_offset += static_cast<int32_t>(str.length());
        string_offsets.push_back(current_string_offset);
    }

    std::vector<uint8_t> string_value_data;
    for (const auto& str : string_data) {
        string_value_data.insert(string_value_data.end(), str.begin(), str.end());
    }

    std::vector<int8_t> validity_bitmap = {0x3F};

    const int64_t num_elements = int_data.size();
    const int64_t int_element_size = sizeof(int32_t);
    const int64_t offset_element_size = sizeof(int32_t);

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> int_storage(int_data.size() * int_element_size + 10);
    uint8_t* unaligned_ints = int_storage.data() + 1;

    std::vector<uint8_t> string_offset_storage(string_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_string_offsets = string_offset_storage.data() + 1;

    std::vector<uint8_t> string_value_storage(string_value_data.size() + 10);
    uint8_t* unaligned_string_values = string_value_storage.data() + 1;

    std::vector<uint8_t> validity_storage(validity_bitmap.size() + 10);
    uint8_t* unaligned_validity = validity_storage.data() + 1;

    // 3. Copy data to unaligned memory
    for (size_t i = 0; i < int_data.size(); ++i) {
        memcpy(unaligned_ints + i * int_element_size, &int_data[i], int_element_size);
    }

    for (size_t i = 0; i < string_offsets.size(); ++i) {
        memcpy(unaligned_string_offsets + i * offset_element_size, &string_offsets[i],
               offset_element_size);
    }

    memcpy(unaligned_string_values, string_value_data.data(), string_value_data.size());
    memcpy(unaligned_validity, validity_bitmap.data(), validity_bitmap.size());

    // 4. Create Arrow array with unaligned memory
    auto int_buffer = arrow::Buffer::Wrap(unaligned_ints, int_data.size() * int_element_size);
    auto int_array = std::make_shared<arrow::Int32Array>(num_elements, int_buffer, nullptr, 0);

    auto string_value_buffer =
            arrow::Buffer::Wrap(unaligned_string_values, string_value_data.size());
    auto string_offsets_buffer = arrow::Buffer::Wrap(unaligned_string_offsets,
                                                     string_offsets.size() * offset_element_size);
    auto string_array = std::make_shared<arrow::StringArray>(num_elements, string_offsets_buffer,
                                                             string_value_buffer, nullptr, 0);

    auto validity_buffer = arrow::Buffer::Wrap(unaligned_validity, validity_bitmap.size());

    auto field_int = arrow::field("int_field", arrow::int32());
    auto field_string = arrow::field("string_field", arrow::utf8());

    auto struct_type = arrow::struct_({field_int, field_string});

    arrow::ArrayVector field_arrays = {int_array, string_array};

    auto arr = std::make_shared<arrow::StructArray>(struct_type, num_elements, field_arrays,
                                                    validity_buffer);

    const auto* concrete_array = dynamic_cast<const arrow::StructArray*>(arr.get());

    const auto* int_field_array =
            dynamic_cast<const arrow::Int32Array*>(concrete_array->field(0).get());
    const auto* ints_ptr = int_field_array->raw_values();
    uintptr_t ints_address = reinterpret_cast<uintptr_t>(ints_ptr);
    EXPECT_EQ(ints_address % 4, 1);

    const auto* string_field_array =
            dynamic_cast<const arrow::StringArray*>(concrete_array->field(1).get());
    const auto* string_values_ptr = string_field_array->value_data()->data();
    uintptr_t string_values_address = reinterpret_cast<uintptr_t>(string_values_ptr);
    EXPECT_EQ(string_values_address % 4, 1);

    // 5.Test read_column_from_arrow
    // Create sub-columns exclusively (no extra refs) so that ColumnStruct::get_column()
    // non-const path does not find use_count > 1.
    auto ser_col = ColumnStruct::create(Columns {ColumnInt32::create(), ColumnString::create()});
    cctz::time_zone tz;
    DataTypeSerDeSPtrs elem_serdes = {serde_int32, serde_str};
    Strings field_names = {"int_field", "string_field"};

    auto serde_struct = std::make_shared<DataTypeStructSerDe>(elem_serdes, field_names);

    auto st = serde_struct->read_column_from_arrow(*ser_col, arr.get(), 0, num_elements, tz);
    EXPECT_TRUE(st.ok());
}

// Regression test for OPENSOURCE-374: from_string (used by CAST string->struct, which is the
// stream-load JSON path) must match struct sub-fields by name, tolerate missing fields, and
// keep working for positional input. This covers every branch of DataTypeStructSerDe::_from_string.
TEST_F(DataTypeStructSerDeTest, FromStringByFieldName) {
    DataTypePtr f1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr f2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>());
    DataTypePtr f3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr st =
            std::make_shared<DataTypeStruct>(DataTypes {f1, f2, f3}, Strings {"f1", "f2", "f3"});
    auto serde = st->get_serde(1);
    DataTypeSerDe::FormatOptions opt;

    auto from_string = [&](const std::string& s, MutableColumnPtr& col) -> Status {
        col = st->create_column();
        std::string buf = s;
        StringRef ref(buf.data(), buf.size());
        return serde->from_string(ref, *col, opt);
    };
    // to_string is the inverse of from_string and always writes fields in schema order, so the
    // input field order cancels out and we can compare results structurally.
    auto to_string = [&](const MutableColumnPtr& col) -> std::string {
        auto out = ColumnString::create();
        VectorBufferWriter bw(*out);
        serde->to_string(*col, 0, bw, opt);
        bw.commit();
        auto s = out->get_data_at(0);
        return std::string(s.data, s.size);
    };
    auto has = [](const Status& st, const std::string& sub) {
        return st.to_string().find(sub) != std::string::npos;
    };

    MutableColumnPtr ordered;
    MutableColumnPtr c;
    ASSERT_TRUE(from_string(R"({"f1":1,"f2":2.5,"f3":"a"})", ordered).ok());

    // 1) out-of-order keys are matched by name (the core fix)
    ASSERT_TRUE(from_string(R"({"f2":2.5,"f1":1,"f3":"a"})", c).ok());
    EXPECT_EQ(to_string(ordered), to_string(c));

    // 2) field names are matched case-insensitively (input is lower-cased before lookup)
    ASSERT_TRUE(from_string(R"({"F2":2.5,"F1":1,"F3":"a"})", c).ok());
    EXPECT_EQ(to_string(ordered), to_string(c));

    // 3) a missing field is filled with NULL (named mode tolerates fewer fields)
    MutableColumnPtr with_null;
    ASSERT_TRUE(from_string(R"({"f1":1,"f2":2.5,"f3":null})", with_null).ok());
    ASSERT_TRUE(from_string(R"({"f2":2.5,"f1":1})", c).ok());
    EXPECT_EQ(to_string(with_null), to_string(c));

    // 4) positional input (no field names) still works
    ASSERT_TRUE(from_string(R"({1,2.5,"a"})", c).ok());
    EXPECT_EQ(to_string(ordered), to_string(c));

    // 5) empty struct '{}' yields all-NULL fields
    MutableColumnPtr empty;
    ASSERT_TRUE(from_string("{}", empty).ok());
    ASSERT_TRUE(from_string(R"({"f1":null,"f2":null,"f3":null})", c).ok());
    EXPECT_EQ(to_string(c), to_string(empty));

    // 6) an unknown field is ignored, missing schema fields become NULL (consistent with the
    //    simdjson reader and PostgreSQL/Spark/Trino)
    MutableColumnPtr f2_null;
    ASSERT_TRUE(from_string(R"({"f1":1,"f2":null,"f3":"a"})", f2_null).ok());
    ASSERT_TRUE(from_string(R"({"f1":1,"fx":2.5,"f3":"a"})", c).ok());
    EXPECT_EQ(to_string(f2_null), to_string(c));

    // 7) extra named fields beyond the schema are ignored (4 fields into a 3-field struct)
    ASSERT_TRUE(from_string(R"({"f1":1,"f2":2.5,"f3":"a","f4":9})", c).ok());
    EXPECT_EQ(to_string(ordered), to_string(c));

    // --- error paths (each exercises a distinct branch / message) ---
    // 8) name-value pair missing the collection delimiter, e.g. {"f1":1:"f2":2}
    Status e = from_string(R"({"f1":1:"f2":2})", c);
    EXPECT_FALSE(e.ok());
    EXPECT_TRUE(has(e, "does not have collection delimiter"));

    // 9) positional input whose count does not match the schema is rejected (too few or too
    //    many) -- without field names the arity must be exact, same as PG/Spark/Trino
    e = from_string(R"({1,2.5})", c); // 2 values into a 3-field struct
    EXPECT_FALSE(e.ok());
    EXPECT_TRUE(has(e, "not equal to schema field number"));
    e = from_string(R"({1,2.5,a,9})", c); // 4 values into a 3-field struct
    EXPECT_FALSE(e.ok());
    EXPECT_TRUE(has(e, "not equal to schema field number"));

    // 10) positional value not separated by the collection delimiter, e.g. {1,2.5:3}
    e = from_string(R"({1,2.5:x})", c);
    EXPECT_FALSE(e.ok());
    EXPECT_TRUE(has(e, "not separated by collection_delim"));

    // 11) bad framing (missing braces)
    EXPECT_FALSE(from_string(R"("f1":1)", c).ok());

    // 12) strict mode propagates a sub-field parse error
    auto sc = st->create_column();
    std::string sbuf = R"({"f1":"notanint","f2":2.5,"f3":"a"})";
    StringRef sref(sbuf.data(), sbuf.size());
    EXPECT_FALSE(serde->from_string_strict_mode(sref, *sc, opt).ok());
}

} // namespace doris
