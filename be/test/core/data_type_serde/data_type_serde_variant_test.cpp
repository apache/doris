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

#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_variant.h"
#include "core/data_type_serde/data_type_variant_serde.h"
#include "core/string_buffer.hpp"
#include "exprs/function/parse/variant_string_parse.h"
#include "gen_cpp/types.pb.h"
#include "util/mysql_row_buffer.h"
#include "util/slice.h"

namespace doris {

TEST(VariantSerdeTest, BasicUnsupportedAndArrowPaths) {
    DataTypeVariantSerDe serde;
    auto column = ColumnVariant::create(0, false);
    DataTypeSerDe::FormatOptions options;
    std::string json = R"({"k": 1})";
    Slice slice(json.data(), json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    EXPECT_EQ(serde.get_name(), "Variant");

    PValues values;
    EXPECT_FALSE(serde.write_column_to_pb(*column, values, 0, column->size()).ok());
    EXPECT_FALSE(serde.read_column_from_pb(*column, values).ok());
    EXPECT_FALSE(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).ok());

    auto string_column = ColumnString::create();
    VectorBufferWriter writer(*string_column);
    serde.to_string(*column, 0, writer, options);
    writer.commit();
    EXPECT_FALSE(string_column->get_data_at(0).to_string().empty());

    arrow::StringBuilder string_builder;
    EXPECT_TRUE(serde.write_column_to_arrow(*column, nullptr, &string_builder, 0, column->size(),
                                            cctz::utc_time_zone())
                        .ok());
    std::shared_ptr<arrow::Array> string_array;
    ASSERT_TRUE(string_builder.Finish(&string_array).ok());
    EXPECT_EQ(string_array->length(), column->size());

    arrow::Int32Builder int_builder;
    EXPECT_FALSE(serde.write_column_to_arrow(*column, nullptr, &int_builder, 0, column->size(),
                                             cctz::utc_time_zone())
                         .ok());
}

TEST(VariantSerdeTest, StructArrowEncodesLegacyVariantAndSqlNull) {
    DataTypeVariantSerDe serde;
    auto column = ColumnVariant::create(0, false);
    DataTypeSerDe::FormatOptions options;
    const std::string json = R"({"id":7,"tags":["doris"]})";
    const std::vector<std::string> json_rows {json, "null", json};
    for (const auto& json_row : json_rows) {
        Slice slice(json_row.data(), json_row.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    auto value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto metadata_builder = std::make_shared<arrow::BinaryBuilder>();
    auto arrow_type = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                      arrow::field("metadata", arrow::binary(), false)});
    arrow::StructBuilder builder(arrow_type, arrow::default_memory_pool(),
                                 {value_builder, metadata_builder});
    NullMap forced_nulls {0, 0, 1};
    ASSERT_TRUE(serde.write_column_to_arrow(*column, &forced_nulls, &builder, 0, column->size(),
                                            cctz::utc_time_zone())
                        .ok());

    std::shared_ptr<arrow::StructArray> output;
    ASSERT_TRUE(builder.Finish(&output).ok());
    ASSERT_EQ(output->length(), 3);
    EXPECT_FALSE(output->IsNull(0));
    EXPECT_FALSE(output->IsNull(1));
    EXPECT_TRUE(output->IsNull(2));

    JsonStringToVariantEncoder expected_encoder({.max_json_key_length = 1024,
                                                 .throw_on_invalid_json = true,
                                                 .check_duplicate_json_path = false});
    expected_encoder.add_json({json.data(), json.size()});
    // Legacy VARIANT V1 materializes a root JSON null as an empty object.
    constexpr std::string_view legacy_json_null = "{}";
    expected_encoder.add_json({legacy_json_null.data(), legacy_json_null.size()});
    VariantBatchBuilder expected = expected_encoder.finish_batch();
    const auto& values = assert_cast<const arrow::BinaryArray&>(*output->field(0));
    const auto& metadatas = assert_cast<const arrow::BinaryArray&>(*output->field(1));
    for (size_t row = 0; row < 2; ++row) {
        const VariantRef expected_value = expected.value_at(row);
        EXPECT_EQ(values.GetView(cast_set<int64_t>(row)),
                  std::string_view(expected_value.value.data, expected_value.value.size));
        EXPECT_EQ(metadatas.GetView(cast_set<int64_t>(row)),
                  std::string_view(expected_value.metadata.data, expected_value.metadata.size));
    }
}

TEST(VariantSerdeTest, StructArrowPreservesScalarStringSemantics) {
    DataTypeVariantSerDe serde;
    auto column = ColumnVariant::create(0, false);
    DataTypeSerDe::FormatOptions options;
    const std::string json = R"("123")";
    Slice slice(json.data(), json.size());
    ASSERT_TRUE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    auto value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto metadata_builder = std::make_shared<arrow::BinaryBuilder>();
    auto arrow_type = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                      arrow::field("metadata", arrow::binary(), false)});
    arrow::StructBuilder builder(arrow_type, arrow::default_memory_pool(),
                                 {value_builder, metadata_builder});
    ASSERT_TRUE(serde.write_column_to_arrow(*column, nullptr, &builder, 0, column->size(),
                                            cctz::utc_time_zone())
                        .ok());

    std::shared_ptr<arrow::StructArray> output;
    ASSERT_TRUE(builder.Finish(&output).ok());
    JsonStringToVariantEncoder expected_encoder({.max_json_key_length = 1024,
                                                 .throw_on_invalid_json = true,
                                                 .check_duplicate_json_path = false});
    expected_encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder expected = expected_encoder.finish_batch();
    const VariantRef expected_value = expected.value_at(0);
    const auto& values = assert_cast<const arrow::BinaryArray&>(*output->field(0));
    const auto& metadatas = assert_cast<const arrow::BinaryArray&>(*output->field(1));
    EXPECT_EQ(values.GetView(0),
              std::string_view(expected_value.value.data, expected_value.value.size));
    EXPECT_EQ(metadatas.GetView(0),
              std::string_view(expected_value.metadata.data, expected_value.metadata.size));
}

TEST(VariantSerdeTest, StructArrowPreservesMixedObjectAndScalarStringSemantics) {
    DataTypeVariantSerDe serde;
    auto column = ColumnVariant::create(0, false);
    DataTypeSerDe::FormatOptions options;
    const std::vector<std::string> json_rows {R"({"id":7})", R"("123")"};
    for (const auto& json : json_rows) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    auto value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto metadata_builder = std::make_shared<arrow::BinaryBuilder>();
    auto arrow_type = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                      arrow::field("metadata", arrow::binary(), false)});
    arrow::StructBuilder builder(arrow_type, arrow::default_memory_pool(),
                                 {value_builder, metadata_builder});
    ASSERT_TRUE(serde.write_column_to_arrow(*column, nullptr, &builder, 0, column->size(),
                                            cctz::utc_time_zone())
                        .ok());

    std::shared_ptr<arrow::StructArray> output;
    ASSERT_TRUE(builder.Finish(&output).ok());
    JsonStringToVariantEncoder expected_encoder({.max_json_key_length = 1024,
                                                 .throw_on_invalid_json = true,
                                                 .check_duplicate_json_path = false});
    for (const auto& json : json_rows) {
        expected_encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder expected = expected_encoder.finish_batch();
    const auto& values = assert_cast<const arrow::BinaryArray&>(*output->field(0));
    const auto& metadatas = assert_cast<const arrow::BinaryArray&>(*output->field(1));
    for (size_t row = 0; row < json_rows.size(); ++row) {
        const VariantRef expected_value = expected.value_at(row);
        EXPECT_EQ(values.GetView(cast_set<int64_t>(row)),
                  std::string_view(expected_value.value.data, expected_value.value.size));
        EXPECT_EQ(metadatas.GetView(cast_set<int64_t>(row)),
                  std::string_view(expected_value.metadata.data, expected_value.metadata.size));
    }
}

} // namespace doris
