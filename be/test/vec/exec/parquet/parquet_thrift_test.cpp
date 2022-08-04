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

TEST_F(ParquetThriftReaderTest, column_reader) {
    // type-decoder.parquet is the part of following table:
    // create table type-decoder (
    //   int_col int)
    // TODO(gaoxin): add more hive types
    LocalFileReader reader("./be/test/exec/test_data/parquet_scanner/type-decoder.parquet", 0);
    auto st = reader.open();
    EXPECT_TRUE(st.ok());

    std::shared_ptr<FileMetaData> metaData;
    parse_thrift_footer(&reader, metaData);
    tparquet::FileMetaData t_metadata = metaData->to_thrift_metadata();

    // read the `int_col` column, it's the int-type column, and has ten values:
    // -1, 2, -3, 4, -5, 6, -7, 8, -9, 10
    tparquet::ColumnChunk column_chunk = t_metadata.row_groups[0].columns[0];
    tparquet::ColumnMetaData chunk_meta = column_chunk.meta_data;
    size_t start_offset = chunk_meta.__isset.dictionary_page_offset
                                  ? chunk_meta.dictionary_page_offset
                                  : chunk_meta.data_page_offset;
    size_t chunk_size = chunk_meta.total_compressed_size;
    BufferedFileStreamReader stream_reader(&reader, start_offset, chunk_size);

    FieldDescriptor schema_descriptor;
    schema_descriptor.parse_from_thrift(t_metadata.schema);
    auto field_schema = const_cast<FieldSchema*>(schema_descriptor.get_column(0));

    ColumnChunkReader chunk_reader(&stream_reader, &column_chunk, field_schema);
    size_t batch_size = 10;
    size_t int_length = 4;
    char data[batch_size * int_length];
    Slice slice(data, batch_size * int_length);
    chunk_reader.init();
    uint64_t int_sum = 0;
    while (chunk_reader.has_next_page()) {
        // seek to next page header
        chunk_reader.next_page();
        // load data to decoder
        chunk_reader.load_page_data();
        while (chunk_reader.num_values() > 0) {
            size_t num_values = chunk_reader.num_values() < batch_size
                                        ? chunk_reader.num_values() < batch_size
                                        : batch_size;
            chunk_reader.decode_values(slice, num_values);
            auto out_data = reinterpret_cast<Int32*>(slice.data);
            for (int i = 0; i < num_values; i++) {
                Int32 value = out_data[i];
                int_sum += value;
            }
        }
    }
    ASSERT_EQ(int_sum, 5);
}

} // namespace vectorized

} // namespace doris
