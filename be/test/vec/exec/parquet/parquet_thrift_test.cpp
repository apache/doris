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

#include <cctz/time_zone.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>
#include <sys/types.h>

#include <algorithm>
#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/schema_scanner.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "util/spinlock.h"
#include "util/timezone_utils.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/parquet_column_convert.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exec/format/parquet/parquet_thrift_util.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_column_chunk_reader.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/format/parquet/vparquet_group_reader.h"

namespace doris {
namespace vectorized {

class ParquetThriftReaderTest : public testing::Test {
public:
    ParquetThriftReaderTest() {}
};

TEST_F(ParquetThriftReaderTest, normal) {
    io::FileSystemSPtr local_fs = io::LocalFileSystem::create("");
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/localfile.parquet",
                                  &reader);
    EXPECT_TRUE(st.ok());

    FileMetaData* meta_data;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &meta_data, &meta_size, nullptr));
    tparquet::FileMetaData t_metadata = meta_data->to_thrift();

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
    delete meta_data;
}

TEST_F(ParquetThriftReaderTest, complex_nested_file) {
    // hive-complex.parquet is the part of following table:
    // complex_nested_table(
    //   `name` string,
    //   `income` array<array<int>>,
    //   `hobby` array<map<string,string>>,
    //   `friend` map<string,string>,
    //   `mark` struct<math:int,english:int>)

    io::FileSystemSPtr local_fs = io::LocalFileSystem::create("");
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/hive-complex.parquet",
                                  &reader);
    EXPECT_TRUE(st.ok());

    FileMetaData* metadata;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &metadata, &meta_size, nullptr));
    tparquet::FileMetaData t_metadata = metadata->to_thrift();
    FieldDescriptor schemaDescriptor;
    static_cast<void>(schemaDescriptor.parse_from_thrift(t_metadata.schema));

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
    delete metadata;
}

static int fill_nullable_column(ColumnPtr& doris_column, level_t* definitions, size_t num_values) {
    CHECK(doris_column->is_nullable());
    auto* nullable_column = const_cast<vectorized::ColumnNullable*>(
            static_cast<const vectorized::ColumnNullable*>(doris_column.get()));
    NullMap& map_data = nullable_column->get_null_map_data();
    int null_cnt = 0;
    for (int i = 0; i < num_values; ++i) {
        bool nullable = definitions[i] == 0;
        if (nullable) {
            null_cnt++;
        }
        map_data.emplace_back(nullable);
    }
    return null_cnt;
}

static Status get_column_values(io::FileReaderSPtr file_reader, tparquet::ColumnChunk* column_chunk,
                                FieldSchema* field_schema, ColumnPtr& doris_column,
                                DataTypePtr& data_type, level_t* definitions) {
    tparquet::ColumnMetaData chunk_meta = column_chunk->meta_data;
    size_t start_offset = chunk_meta.__isset.dictionary_page_offset
                                  ? chunk_meta.dictionary_page_offset
                                  : chunk_meta.data_page_offset;
    size_t chunk_size = chunk_meta.total_compressed_size;

    bool need_convert = false;
    auto& parquet_physical_type = column_chunk->meta_data.type;
    auto& show_type = field_schema->type.type;

    ColumnPtr src_column = ParquetConvert::get_column(parquet_physical_type, show_type,
                                                      doris_column, data_type, &need_convert);

    io::BufferedFileStreamReader stream_reader(file_reader, start_offset, chunk_size, 1024);

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    ColumnChunkReader chunk_reader(&stream_reader, column_chunk, field_schema, &ctz, nullptr);
    // initialize chunk reader
    static_cast<void>(chunk_reader.init());
    // seek to next page header
    static_cast<void>(chunk_reader.next_page());
    // load page data into underlying container
    static_cast<void>(chunk_reader.load_page_data());
    int rows = chunk_reader.remaining_num_values();
    // definition levels
    if (field_schema->definition_level == 0) { // required field
        std::fill(definitions, definitions + rows, 1);
    } else {
        chunk_reader.get_def_levels(definitions, rows);
    }
    MutableColumnPtr data_column;
    if (src_column->is_nullable()) {
        // fill nullable values
        fill_nullable_column(src_column, definitions, rows);
        auto* nullable_column = const_cast<vectorized::ColumnNullable*>(
                static_cast<const vectorized::ColumnNullable*>(src_column.get()));
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        data_column = src_column->assume_mutable();
    }
    ColumnSelectVector run_length_map;
    // decode page data
    if (field_schema->definition_level == 0) {
        // required column
        std::vector<u_short> null_map = {(u_short)rows};
        run_length_map.set_run_length_null_map(null_map, rows, nullptr);
        RETURN_IF_ERROR(chunk_reader.decode_values(data_column, data_type, run_length_map, false));
    } else {
        // column with null values
        level_t level_type = definitions[0];
        int num_values = 1;
        for (int i = 1; i < rows; ++i) {
            if (definitions[i] != level_type) {
                if (level_type == 0) {
                    // null values
                    chunk_reader.insert_null_values(data_column, num_values);
                } else {
                    std::vector<u_short> null_map = {(u_short)num_values};
                    run_length_map.set_run_length_null_map(null_map, rows, nullptr);
                    RETURN_IF_ERROR(chunk_reader.decode_values(data_column, data_type,
                                                               run_length_map, false));
                }
                level_type = definitions[i];
                num_values = 1;
            } else {
                num_values++;
            }
        }
        if (level_type == 0) {
            // null values
            chunk_reader.insert_null_values(data_column, num_values);
        } else {
            std::vector<u_short> null_map = {(u_short)num_values};
            run_length_map.set_run_length_null_map(null_map, rows, nullptr);
            RETURN_IF_ERROR(
                    chunk_reader.decode_values(data_column, data_type, run_length_map, false));
        }
    }
    if (need_convert) {
        std::unique_ptr<ParquetConvert::ColumnConvert> converter;
        ParquetConvert::ConvertParams convert_params;
        convert_params.init(field_schema, &ctz, doris_column->size());
        RETURN_IF_ERROR(ParquetConvert::get_converter(parquet_physical_type, show_type, data_type,
                                                      &converter, &convert_params));
        auto x = doris_column->assume_mutable();
        RETURN_IF_ERROR(converter->convert(src_column, x));
    }

    return Status::OK();
}

// Only the unit test depend on this, but it is wrong, should not use TTupleDesc to create tuple desc, not
// use columndesc
static doris::TupleDescriptor* create_tuple_desc(
        doris::ObjectPool* pool, std::vector<doris::SchemaScanner::ColumnDesc>& column_descs) {
    using namespace doris;
    int null_column = 0;
    for (int i = 0; i < column_descs.size(); ++i) {
        if (column_descs[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    std::vector<SlotDescriptor*> slots;
    int null_byte = 0;
    int null_bit = 0;

    for (int i = 0; i < column_descs.size(); ++i) {
        TSlotDescriptor t_slot_desc;
        if (column_descs[i].type == TYPE_DECIMALV2) {
            t_slot_desc.__set_slotType(TypeDescriptor::create_decimalv2_type(27, 9).to_thrift());
        } else {
            TypeDescriptor descriptor(column_descs[i].type);
            if (column_descs[i].precision >= 0 && column_descs[i].scale >= 0) {
                descriptor.precision = column_descs[i].precision;
                descriptor.scale = column_descs[i].scale;
            }
            t_slot_desc.__set_slotType(descriptor.to_thrift());
        }
        t_slot_desc.__set_colName(column_descs[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        if (column_descs[i].is_null) {
            t_slot_desc.__set_nullIndicatorByte(null_byte);
            t_slot_desc.__set_nullIndicatorBit(null_bit);
            null_bit = (null_bit + 1) % 8;

            if (0 == null_bit) {
                null_byte++;
            }
        } else {
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
        }

        t_slot_desc.id = i;
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));
        slots.push_back(slot);
        offset += column_descs[i].size;
    }

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.__set_byteSize(offset);
    t_tuple_desc.__set_numNullBytes((null_byte * 8 + null_bit + 7) / 8);
    doris::TupleDescriptor* tuple_desc =
            pool->add(new (std::nothrow) doris::TupleDescriptor(t_tuple_desc));

    for (int i = 0; i < slots.size(); ++i) {
        tuple_desc->add_slot(slots[i]);
    }

    return tuple_desc;
}

static void create_block(std::unique_ptr<vectorized::Block>& block) {
    // Current supported column type:
    std::vector<SchemaScanner::ColumnDesc> column_descs = {
            {"tinyint_col", TYPE_TINYINT, sizeof(int8_t), true},
            {"smallint_col", TYPE_SMALLINT, sizeof(int16_t), true},
            {"int_col", TYPE_INT, sizeof(int32_t), true},
            {"bigint_col", TYPE_BIGINT, sizeof(int64_t), true},
            {"boolean_col", TYPE_BOOLEAN, sizeof(bool), true},
            {"float_col", TYPE_FLOAT, sizeof(float_t), true},
            {"double_col", TYPE_DOUBLE, sizeof(double_t), true},
            {"string_col", TYPE_STRING, sizeof(StringRef), true},
            // binary is not supported, use string instead
            {"binary_col", TYPE_STRING, sizeof(StringRef), true},
            // 64-bit-length, see doris::get_slot_size in primitive_type.cpp
            {"timestamp_col", TYPE_DATETIMEV2, sizeof(int128_t), true},
            {"decimal_col", TYPE_DECIMALV2, sizeof(DecimalV2Value), true},
            {"char_col", TYPE_CHAR, sizeof(StringRef), true},
            {"varchar_col", TYPE_VARCHAR, sizeof(StringRef), true},
            {"date_col", TYPE_DATEV2, sizeof(uint32_t), true},
            {"date_v2_col", TYPE_DATEV2, sizeof(uint32_t), true},
            {"timestamp_v2_col", TYPE_DATETIMEV2, sizeof(int128_t), true, 18, 0}};
    SchemaScanner schema_scanner(column_descs);
    ObjectPool object_pool;
    doris::TupleDescriptor* tuple_desc = create_tuple_desc(&object_pool, column_descs);
    auto tuple_slots = tuple_desc->slots();
    block = vectorized::Block::create_unique();
    for (const auto& slot_desc : tuple_slots) {
        auto data_type = slot_desc->get_data_type_ptr();
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
}

static void read_parquet_data_and_check(const std::string& parquet_file,
                                        const std::string& result_file, int rows) {
    /*
     * table schema in parquet file:
     * create table `decoder`(
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

    io::FileSystemSPtr local_fs = io::LocalFileSystem::create("");
    io::FileReaderSPtr reader;
    auto st = local_fs->open_file(parquet_file, &reader);
    EXPECT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block;
    create_block(block);
    FileMetaData* metadata;
    size_t meta_size;
    static_cast<void>(parse_thrift_footer(reader, &metadata, &meta_size, nullptr));
    tparquet::FileMetaData t_metadata = metadata->to_thrift();
    FieldDescriptor schema_descriptor;
    static_cast<void>(schema_descriptor.parse_from_thrift(t_metadata.schema));
    level_t defs[rows];

    for (int c = 0; c < 14; ++c) {
        auto& column_name_with_type = block->get_by_position(c);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[c],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(c)),
                                  data_column, data_type, defs));
    }
    // `date_v2_col` date, // 14 - 13, DATEV2
    {
        auto& column_name_with_type = block->get_by_position(14);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[13],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(13)),
                                  data_column, data_type, defs));
    }
    // `timestamp_v2_col` timestamp, // 15 - 9, DATETIMEV2
    {
        auto& column_name_with_type = block->get_by_position(15);
        auto& data_column = column_name_with_type.column;
        auto& data_type = column_name_with_type.type;
        static_cast<void>(
                get_column_values(reader, &t_metadata.row_groups[0].columns[9],
                                  const_cast<FieldSchema*>(schema_descriptor.get_column(9)),
                                  data_column, data_type, defs));
    }

    io::FileReaderSPtr result;
    auto rst = local_fs->open_file(result_file, &result);
    EXPECT_TRUE(rst.ok());
    uint8_t result_buf[result->size() + 1];
    result_buf[result->size()] = '\0';
    size_t bytes_read;
    Slice res(result_buf, result->size());
    static_cast<void>(result->read_at(0, res, &bytes_read));
    ASSERT_STREQ(block->dump_data(0, rows).c_str(), reinterpret_cast<char*>(result_buf));
    delete metadata;
}

TEST_F(ParquetThriftReaderTest, type_decoder) {
    read_parquet_data_and_check("./be/test/exec/test_data/parquet_scanner/type-decoder.parquet",
                                "./be/test/exec/test_data/parquet_scanner/type-decoder.txt", 10);
}

TEST_F(ParquetThriftReaderTest, dict_decoder) {
    read_parquet_data_and_check("./be/test/exec/test_data/parquet_scanner/dict-decoder.parquet",
                                "./be/test/exec/test_data/parquet_scanner/dict-decoder.txt", 12);
}
} // namespace vectorized

} // namespace doris
