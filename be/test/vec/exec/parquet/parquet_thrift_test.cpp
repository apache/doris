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

#include "io/buffered_reader.h"
#include "io/file_reader.h"
#include "io/local_file_reader.h"
#include "util/runtime_profile.h"
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
                                FieldSchema* field_schema, Slice& slice) {
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
    return chunk_reader.decode_values(slice, chunk_reader.num_values());
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

    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();
    FieldDescriptor schema_descriptor;
    schema_descriptor.parse_from_thrift(t_metadata.schema);
    int rows = 10;

    // the physical_type of tinyint_col, smallint_col and int_col are all INT32
    // they are distinguished by converted_type(in FieldSchema.parquet_schema.converted_type)
    for (int col_idx = 0; col_idx < 3; ++col_idx) {
        char data[4 * rows];
        Slice slice(data, 4 * rows);
        get_column_values(&reader, &t_metadata.row_groups[0].columns[col_idx],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(col_idx)), slice);
        auto out_data = reinterpret_cast<int32_t*>(data);
        int int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += out_data[i];
        }
        ASSERT_EQ(int_sum, 5);
    }
    // `bigint_col` bigint, // 3
    {
        char data[8 * rows];
        Slice slice(data, 8 * rows);
        get_column_values(&reader, &t_metadata.row_groups[0].columns[3],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(3)), slice);
        auto out_data = reinterpret_cast<int64_t*>(data);
        int int_sum = 0;
        for (int i = 0; i < rows; ++i) {
            int_sum += out_data[i];
        }
        ASSERT_EQ(int_sum, 5);
    }
    // `boolean_col` boolean, // 4
    {
        char data[1 * rows];
        Slice slice(data, 1 * rows);
        get_column_values(&reader, &t_metadata.row_groups[0].columns[4],
                          const_cast<FieldSchema*>(schema_descriptor.get_column(4)), slice);
        auto out_data = reinterpret_cast<bool*>(data);
        ASSERT_FALSE(out_data[0]);
        ASSERT_TRUE(out_data[1]);
        ASSERT_FALSE(out_data[2]);
        ASSERT_TRUE(out_data[3]);
        ASSERT_FALSE(out_data[4]);
        ASSERT_FALSE(out_data[5]);
        ASSERT_TRUE(out_data[6]);
        ASSERT_FALSE(out_data[7]);
        ASSERT_FALSE(out_data[8]);
        ASSERT_FALSE(out_data[9]);
    }
    // `string_col` string, // 7
    {
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

        char data[50 * rows];
        Slice slice(data, 50 * rows);
        level_t defs[rows];
        // Analyze null string
        chunk_reader.get_def_levels(defs, rows);
        ASSERT_EQ(defs[1], 0);
        ASSERT_EQ(defs[3], 0);
        ASSERT_EQ(defs[7], 0);

        chunk_reader.decode_values(slice, 7);
        ASSERT_STREQ("s-row0", slice.data);
        ASSERT_STREQ("s-row2", slice.data + 7);
    }
}

} // namespace vectorized

} // namespace doris
