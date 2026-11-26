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

#include "format_v2/table/delta_sys_table_reader.h"

#include <gtest/gtest.h>

#include <limits>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/timestamptz_value.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"

namespace doris::format::deltalake {
namespace {

class DeltaSysTableReaderTest : public testing::Test {
protected:
    std::shared_ptr<io::FileSystemProperties> fs = std::make_shared<io::FileSystemProperties>();
    std::unique_ptr<io::FileDescription> file = std::make_unique<io::FileDescription>();
    std::vector<ColumnDefinition> projection;
    TDeltaLakeSysTableBatch batch;

    DeltaSysTableReaderTest() {
        batch.__set_columns({});
        batch.__set_rows({});
    }

    void add_column(const std::string& name, DataTypePtr type, int32_t ordinal) {
        TDeltaLakeSysTableColumn column;
        column.__set_name(name);
        column.__set_schema_ordinal(ordinal);
        batch.columns.push_back(column);
        projection.push_back({.identifier = Field {},
                              .name = name,
                              .type = type,
                              .children = materialized_schema_children(type)});
    }

    void add_row(const std::vector<TDeltaLakeSysTableField>& cells) {
        TDeltaLakeSysTableRow row;
        row.__set_cells(cells);
        batch.rows.push_back(std::move(row));
    }
};

TEST_F(DeltaSysTableReaderTest, ScalarAndNestedValues) {
    const auto text = make_nullable(std::make_shared<DataTypeString>());
    const auto integer = make_nullable(std::make_shared<DataTypeInt64>());
    add_column("label", text, 9);
    add_column("enabled", make_nullable(std::make_shared<DataTypeUInt8>()), 2);
    add_column("number", integer, 4);
    add_column("fraction", make_nullable(std::make_shared<DataTypeFloat64>()), 1);
    add_column("time", make_nullable(std::make_shared<DataTypeTimeStampTz>(6)), 5);
    add_column("items", make_nullable(std::make_shared<DataTypeArray>(integer)), 0);
    add_column("attributes", make_nullable(std::make_shared<DataTypeMap>(text, text)), 3);
    add_column("record",
               make_nullable(std::make_shared<DataTypeStruct>(DataTypes {integer, text},
                                                              Strings {"first", "second"})),
               7);
    std::vector<TDeltaLakeSysTableField> cells(8);
    cells[0].__set_string_val("value");
    cells[1].__set_bool_val(false);
    cells[2].__set_int64_val(-9);
    cells[3].__set_double_val(1.25);
    cells[4].__set_timestamp_millis(-1);
    cells[5].__set_array_val({cells[2], {}});
    TDeltaLakeSysTableMapEntry entry;
    entry.__set_key("");
    entry.__set_value({});
    cells[6].__set_map_val({entry});
    cells[7].__set_struct_val({cells[2], {}});
    add_row(cells);
    cells.assign(8, {});
    cells[5].__set_array_val({});
    cells[6].__set_map_val({});
    add_row(cells);

    DeltaSysTableReader reader(fs, file, nullptr, nullptr, batch, projection);
    ASSERT_TRUE(reader.init(nullptr).ok());
    reader.set_batch_size(1);
    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader.get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), cells.size());

    auto mapper = reader.create_column_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(
            mapper->create_mapping({projection[2], projection[0], projection[2]}, {}, schema).ok());
    ASSERT_EQ(mapper->mappings().size(), 3);
    EXPECT_EQ(mapper->mappings()[0].file_local_id, 2);
    EXPECT_EQ(mapper->mappings()[1].file_local_id, 0);
    EXPECT_EQ(mapper->mappings()[2].file_local_id, 2);

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    Block block;
    for (const auto& column : schema) {
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(column.local_id)).ok());
        block.insert({column.type->create_column(), column.type, column.name});
    }
    ASSERT_TRUE(reader.open(request).ok());
    size_t rows = 0;
    bool eof = false;
    auto field = [&](size_t position) { return (*block.get_by_position(position).column)[0]; };
    auto status = reader.get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 1);
    ASSERT_FALSE(eof);
    EXPECT_EQ(field(0).get<TYPE_STRING>(), "value");
    EXPECT_FALSE(field(1).get<TYPE_BOOLEAN>());
    EXPECT_EQ(field(2).get<TYPE_BIGINT>(), -9);
    EXPECT_EQ(field(3).get<TYPE_DOUBLE>(), 1.25);
    TimestampTzValue instant(field(4).get<TYPE_TIMESTAMPTZ>());
    EXPECT_EQ(instant.year(), 1969);
    EXPECT_EQ(instant.microsecond(), 999000);
    EXPECT_EQ(field(5).get<TYPE_ARRAY>()[0].get<TYPE_BIGINT>(), -9);
    EXPECT_TRUE(field(5).get<TYPE_ARRAY>()[1].is_null());
    EXPECT_EQ(field(6).get<TYPE_MAP>()[0].get<TYPE_ARRAY>()[0].get<TYPE_STRING>(), "");
    EXPECT_TRUE(field(6).get<TYPE_MAP>()[1].get<TYPE_ARRAY>()[0].is_null());
    EXPECT_EQ(field(7).get<TYPE_STRUCT>()[0].get<TYPE_BIGINT>(), -9);
    EXPECT_TRUE(field(7).get<TYPE_STRUCT>()[1].is_null());

    block.clear_column_data();
    ASSERT_TRUE(reader.get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    ASSERT_TRUE(eof);
    EXPECT_TRUE(field(0).is_null());
    EXPECT_TRUE(field(5).get<TYPE_ARRAY>().empty());
    EXPECT_TRUE(field(6).get<TYPE_MAP>()[0].get<TYPE_ARRAY>().empty());
    EXPECT_TRUE(field(7).is_null());
}

TEST_F(DeltaSysTableReaderTest, CountOnlyAndEmptyBatches) {
    for (int count : {0, 2}) {
        for (int i = 0; i < count; ++i) {
            add_row({});
        }
        DeltaSysTableReader reader(fs, file, nullptr, nullptr, batch, projection);
        ASSERT_TRUE(reader.init(nullptr).ok());
        auto request = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(request.get());
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
        request->count_star_placeholder_columns.push_back(LocalColumnId(0));
        ASSERT_TRUE(reader.open(request).ok());
        auto type = std::make_shared<DataTypeUInt8>();
        Block block;
        block.insert({type->create_column(), type, "placeholder"});
        size_t rows;
        bool eof;
        ASSERT_TRUE(reader.get_block(&block, &rows, &eof).ok());
        EXPECT_EQ(rows, count);
        EXPECT_EQ(block.rows(), count);
        EXPECT_TRUE(eof);
    }
}

TEST_F(DeltaSysTableReaderTest, IntValuesAndOverflow) {
    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    add_column("protocol", type, 0);
    const int64_t min = std::numeric_limits<int32_t>::min();
    const int64_t max = std::numeric_limits<int32_t>::max();
    for (int64_t number : {min, int64_t(0), max, min - 1, max + 1}) {
        batch.rows.clear();
        TDeltaLakeSysTableField cell;
        cell.__set_int64_val(number);
        add_row({cell});
        add_row({{}});
        DeltaSysTableReader reader(fs, file, nullptr, nullptr, batch, projection);
        ASSERT_TRUE(reader.init(nullptr).ok());
        auto request = std::make_shared<FileScanRequest>();
        FileScanRequestBuilder builder(request.get());
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
        ASSERT_TRUE(reader.open(request).ok());
        Block block;
        block.insert({type->create_column(), type, "protocol"});
        size_t rows = 0;
        bool eof = false;
        auto status = reader.get_block(&block, &rows, &eof);
        if (number < min || number > max) {
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("INT out of range"), std::string::npos);
        } else {
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_EQ(rows, 2);
            EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_INT>(), number);
            EXPECT_TRUE((*block.get_by_position(0).column)[1].is_null());
        }
    }
}

TEST_F(DeltaSysTableReaderTest, DuplicateWireColumnNames) {
    add_column("protocol", std::make_shared<DataTypeInt32>(), 0);
    add_column("PROTOCOL", std::make_shared<DataTypeInt32>(), 1);
    DeltaSysTableReader reader(fs, file, nullptr, nullptr, batch, projection);
    std::vector<ColumnDefinition> schema;
    auto status = reader.get_schema(&schema);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Duplicate Delta system table column"), std::string::npos);
}

TEST_F(DeltaSysTableReaderTest, InvalidRowWidth) {
    add_column("number", std::make_shared<DataTypeInt64>(), 0);
    add_row({});
    DeltaSysTableReader reader(fs, file, nullptr, nullptr, batch, projection);
    EXPECT_FALSE(reader.init(nullptr).ok());
}

} // namespace
} // namespace doris::format::deltalake
