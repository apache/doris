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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "exec/schema_scanner.h"
#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "runtime/string_value.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/vparquet_column_chunk_reader.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"

namespace doris {
namespace vectorized {

class ParquetThriftReaderTest : public testing::Test {
public:
    ParquetThriftReaderTest() {}
};

TEST_F(ParquetThriftReaderTest, normal) {
    LocalFileReader reader("./be/test/exec/test_data/parquet_scanner/localfile.parquet", 0);

    auto st = reader.open();
    EXPECT_TRUE(st.ok());

    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();

    LOG(WARNING) << "num row groups: " << metaData->num_row_groups();
    LOG(WARNING) << "num columns: " << metaData->num_columns();
    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.row_groups) {
        LOG(WARNING) << "row group num_rows: " << value.num_rows;
    }
    LOG(WARNING) << "=====================================";
    for (auto value : t_metadata.schema) {
        LOG(WARNING) << "schema column name: " << value.name;
        LOG(WARNING) << "schema column type: " << value.type;
        LOG(WARNING) << "schema column repetition_type: " << value.repetition_type;
        LOG(WARNING) << "schema column num children: " << value.num_children;
    }
}

TEST_F(ParquetThriftReaderTest, complex_nested_file) {
    // hive-complex.parquet is the part of following table:
    // complex_nested_table(
    //   `name` string,
    //   `income` array<array<int>>,
    //   `hobby` array<map<string,string>>,
    //   `friend` map<string,string>,
    //   `mark` struct<math:int,english:int>)
    LocalFileReader reader("./be/test/exec/test_data/parquet_scanner/hive-complex.parquet", 0);

    auto st = reader.open();
    EXPECT_TRUE(st.ok());

    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();
    FieldDescriptor schemaDescriptor;
    schemaDescriptor.parse_from_thrift(t_metadata.schema);

    // table columns
    ASSERT_EQ(schemaDescriptor.get_column_index("name"), 0);
    auto name = schemaDescriptor.get_column("name");
    ASSERT_TRUE(name->children.size() == 0 && name->physical_column_index >= 0);
    ASSERT_TRUE(name->repetition_level == 0 && name->definition_level == 1);

    ASSERT_EQ(schemaDescriptor.get_column_index("income"), 1);
    auto income = schemaDescriptor.get_column("income");
    // should be parsed as ARRAY<ARRAY<INT32>>
    ASSERT_TRUE(income->type.type == TYPE_ARRAY);
    ASSERT_TRUE(income->children.size() == 1);
    ASSERT_TRUE(income->children[0].type.type == TYPE_ARRAY);
    ASSERT_TRUE(income->children[0].children.size() == 1);
    auto i_physical = income->children[0].children[0];
    // five levels for ARRAY<ARRAY<INT32>>
    // income --- bag --- array_element --- bag --- array_element
    //  opt       rep          opt          rep         opt
    // R=0,D=1  R=1,D=2       R=1,D=3     R=2,D=4      R=2,D=5
    ASSERT_TRUE(i_physical.repetition_level == 2 && i_physical.definition_level == 5);

    ASSERT_EQ(schemaDescriptor.get_column_index("hobby"), 2);
    auto hobby = schemaDescriptor.get_column("hobby");
    // should be parsed as ARRAY<MAP<STRUCT<STRING,STRING>>>
    ASSERT_TRUE(hobby->children.size() == 1 && hobby->children[0].children.size() == 1 &&
                hobby->children[0].children[0].children.size() == 2);
    ASSERT_TRUE(hobby->type.type == TYPE_ARRAY && hobby->children[0].type.type == TYPE_MAP &&
                hobby->children[0].children[0].type.type == TYPE_STRUCT);
    // hobby(opt) --- bag(rep) --- array_element(opt) --- map(rep)
    //                                                      \------- key(req)
    //                                                      \------- value(opt)
    // R=0,D=1        R=1,D=2          R=1,D=3             R=2,D=4
    //                                                       \------ R=2,D=4
    //                                                       \------ R=2,D=5
    auto h_key = hobby->children[0].children[0].children[0];
    auto h_value = hobby->children[0].children[0].children[1];
    ASSERT_TRUE(h_key.repetition_level == 2 && h_key.definition_level == 4);
    ASSERT_TRUE(h_value.repetition_level == 2 && h_value.definition_level == 5);

    ASSERT_EQ(schemaDescriptor.get_column_index("friend"), 3);
    ASSERT_EQ(schemaDescriptor.get_column_index("mark"), 4);
}

static Status get_column_values(FileReader* file_reader, tparquet::ColumnChunk* column_chunk,
                                FieldSchema* field_schema, ColumnPtr& doris_column,
                                DataTypePtr& data_type) {
    tparquet::ColumnMetaData chunk_meta = column_chunk->meta_data;
    size_t start_offset = chunk_meta.__isset.dictionary_page_offset
                                  ? chunk_meta.dictionary_page_offset
                                  : chunk_meta.data_page_offset;
    size_t chunk_size = chunk_meta.total_compressed_size;
    BufferedFileStreamReader stream_reader(file_reader, start_offset, chunk_size);

    ColumnChunkReader chunk_reader(&stream_reader, column_chunk, field_schema);
    // initialize chunk reader
    chunk_reader.init();
    // seek to next page header
    chunk_reader.next_page();
    // load page data into underlying container
    chunk_reader.load_page_data();
    // decode page data
    return chunk_reader.decode_values(doris_column, data_type, chunk_reader.num_values());
}

static void create_block(std::unique_ptr<vectorized::Block>& block) {
    // Current supported column type:
    SchemaScanner::ColumnDesc column_descs[] = {
            {"tinyint_col", TYPE_TINYINT, sizeof(int8_t), true},
            {"smallint_col", TYPE_SMALLINT, sizeof(int16_t), true},
            {"int_col", TYPE_INT, sizeof(int32_t), true},
            {"bigint_col", TYPE_BIGINT, sizeof(int64_t), true},
            {"boolean_col", TYPE_BOOLEAN, sizeof(bool), true},
            {"float_col", TYPE_FLOAT, sizeof(float_t), true},
            {"double_col", TYPE_DOUBLE, sizeof(double_t), true},
            {"string_col", TYPE_STRING, sizeof(StringValue), true}};
    SchemaScanner schema_scanner(column_descs,
                                 sizeof(column_descs) / sizeof(SchemaScanner::ColumnDesc));
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_slots = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc())->slots();
    block.reset(new vectorized::Block());
    for (const auto& slot_desc : tuple_slots) {
        auto is_nullable = slot_desc->is_nullable();
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(),
                                                                                  is_nullable);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
}

TEST_F(ParquetThriftReaderTest, type_decoder) {
    /*
     * type-decoder.parquet is the part of following table:
     * create table `type_decoder`(
     * `tinyint_col` tinyint, // 0
     * `smallint_col` smallint, // 1
     * `int_col` int, // 2
     * `bigint_col` bigint, // 3
     * `boolean_col` boolean, // 4
     * `float_col` float, // 5
     * `double_col` double, // 6
     * `string_col` string, // 7
     * `binary_col` binary, // 8
     * `timestamp_col` timestamp, // 9
     * `decimal_col` decimal(10,2), // 10
     * `char_col` char(10), // 11
     * `varchar_col` varchar(50), // 12
     * `date_col` date, // 13
     * `list_string` array<string>) // 14
     */

    LocalFileReader reader("./be/test/exec/test_data/parquet_scanner/type-decoder.parquet", 0);
    /*
     * Data in type-decoder.parquet:
     * -1	-1	-1	-1	false	-1.14	-1.14	s-row0	b-row0	2022-08-01 00:00:00	-1.14	c-row0    	vc-row0	2022-08-01	["as-0","as-1"]
     * 2	2	2	2	true	2.14	2.14	NULL	b-row1	2022-08-02 00:00:00	2.14	c-row1    	vc-row1	2022-08-02	[null,"as-3"]
     * -3	-3	-3	-3	false	-3.14	-3.14	s-row2	b-row2	2022-08-03 00:00:00	-3.14	c-row2    	vc-row2	2022-08-03	[]
     * 4	4	4	4	true	4.14	4.14	NULL	b-row3	2022-08-04 00:00:00	4.14	c-row3    	vc-row3	2022-08-04	["as-4"]
     * -5	-5	-5	-5	false	-5.14	-5.14	s-row4	b-row4	2022-08-05 00:00:00	-5.14	c-row4    	vc-row4	2022-08-05	["as-5",null]
     * 6	6	6	6	false	6.14	6.14	s-row5	b-row5	2022-08-06 00:00:00	6.14	c-row5    	vc-row5	2022-08-06	[null,null]
     * -7	-7	-7	-7	true	-7.14	-7.14	s-row6	b-row6	2022-08-07 00:00:00	-7.14	c-row6    	vc-row6	2022-08-07	["as-6","as-7"]
     * 8	8	8	8	false	8.14	8.14	NULL	b-row7	2022-08-08 00:00:00	8.14	c-row7    	vc-row7	2022-08-08	["as-0","as-8"]
     * -9	-9	-9	-9	false	-9.14	-9.14	s-row8	b-row8	2022-08-09 00:00:00	-9.14	c-row8    	vc-row8	2022-08-09	["as-9","as-10"]
     * 10	10	10	10	false	10.14	10.14	s-row9	b-row9	2022-08-10 00:00:00	10.14	c-row9    	vc-row9	2022-08-10	["as-11","as-12"]
     */
    auto st = reader.open();
    EXPECT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block;
    create_block(block);
    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();
    FieldDescriptor schema_descriptor;
    schema_descriptor.parse_from_thrift(t_metadata.schema);
    int rows = 10;

    // the physical_type of tinyint_col, smallint_col and int_col are all INT32
    // they are distinguished by converted_type(in FieldSchema.parquet_schema.converted_type)
    {
        auto& column_name_with_type = block->get_by_position(0);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[0],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(0)), data_column,
                          data_type);
        int int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += (int8_t)data_column->get64(i);
        }
        ASSERT_EQ(int_sum, 5);
    }
    {
        auto& column_name_with_type = block->get_by_position(1);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[1],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(1)), data_column,
                          data_type);
        int int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += (int16_t)data_column->get64(i);
        }
        ASSERT_EQ(int_sum, 5);
    }
    {
        auto& column_name_with_type = block->get_by_position(2);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[2],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(2)), data_column,
                          data_type);
        int int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += (int32_t)data_column->get64(i);
        }
        ASSERT_EQ(int_sum, 5);
    }
    {
        auto& column_name_with_type = block->get_by_position(3);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[3],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(3)), data_column,
                          data_type);
        int64_t int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += (int64_t)data_column->get64(i);
        }
        ASSERT_EQ(int_sum, 5);
    }
    // `boolean_col` boolean, // 4
    {
        auto& column_name_with_type = block->get_by_position(4);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[4],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(4)), data_column,
                          data_type);
        ASSERT_FALSE(static_cast<bool>(data_column->get64(0)));
        ASSERT_TRUE(static_cast<bool>(data_column->get64(1)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(2)));
        ASSERT_TRUE(static_cast<bool>(data_column->get64(3)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(4)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(5)));
        ASSERT_TRUE(static_cast<bool>(data_column->get64(6)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(7)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(8)));
        ASSERT_FALSE(static_cast<bool>(data_column->get64(9)));
    }
    // `double_col` double, // 6
    {
        auto& column_name_with_type = block->get_by_position(6);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        get_column_values(&reader, &t_metadata.row_groups[0].columns[6],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(6)), data_column,
                          data_type);
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(data_column)).mutate().get());
        MutableColumnPtr nested_column = nullable_column->get_nested_column_ptr();
        ASSERT_EQ(nested_column->get_float64(0), -1.14);
        ASSERT_EQ(nested_column->get_float64(1), 2.14);
        ASSERT_EQ(nested_column->get_float64(2), -3.14);
        ASSERT_EQ(nested_column->get_float64(3), 4.14);
    }
    // `string_col` string, // 7
    {
        auto& column_name_with_type = block->get_by_position(7);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        tparquet::ColumnChunk column_chunk = t_metadata.row_groups[0].columns[7];
        tparquet::ColumnMetaData chunk_meta = column_chunk.meta_data;
        size_t start_offset = chunk_meta.__isset.dictionary_page_offset
                                      ? chunk_meta.dictionary_page_offset
                                      : chunk_meta.data_page_offset;
        size_t chunk_size = chunk_meta.total_compressed_size;
        BufferedFileStreamReader stream_reader(&reader, start_offset, chunk_size);
        ColumnChunkReader chunk_reader(&stream_reader, &column_chunk,
                                       const_cast<FieldSchema*>(schema_descriptor.get_column(7)));
        // initialize chunk reader
        chunk_reader.init();
        // seek to next page header
        chunk_reader.next_page();
        // load page data into underlying container
        chunk_reader.load_page_data();

        level_t defs[rows];
        // Analyze null string
        chunk_reader.get_def_levels(defs, rows);
        ASSERT_EQ(defs[1], 0);
        ASSERT_EQ(defs[3], 0);
        ASSERT_EQ(defs[7], 0);

        chunk_reader.decode_values(data_column, data_type, 7);
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(data_column)).mutate().get());
        MutableColumnPtr nested_column = nullable_column->get_nested_column_ptr();
        auto row0 = nested_column->get_data_at(0).data;
        auto row2 = nested_column->get_data_at(1).data;
        ASSERT_STREQ("s-row0", row0);
        ASSERT_STREQ("s-row2", row2);
    }
}

} // namespace vectorized

} // namespace doris
