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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <gtest/gtest.h>

#include "core/column/column_variant.h"
#include "core/data_type_serde/data_type_variant_serde.h"
#include "core/string_buffer.hpp"
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

} // namespace doris
