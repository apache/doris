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
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "exec/scan/file_scanner.h"
#include "exprs/vexpr_context.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/segment/column_reader.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {
class VExprContext;

class OrcReadLinesTest : public testing::Test {
public:
    OrcReadLinesTest() {}
};

static void read_orc_line(int64_t line, std::string block_dump,
                          const std::string& time_zone = "CST") {
    auto runtime_state = RuntimeState::create_unique();

    std::vector<std::string> column_names = {"col1", "col2", "col3", "col4", "col5",
                                             "col6", "col7", "col8", "col9"};
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx = {
            {"col1", 0}, {"col2", 1}, {"col3", 2}, {"col4", 3}, {"col5", 4},
            {"col6", 5}, {"col7", 6}, {"col8", 7}, {"col9", 8},
    };
    ObjectPool object_pool;
    DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_BIGINT, true),
                                       "col1")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_BOOLEAN, true),
                                       "col2")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_VARCHAR, true),
                                       "col3")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_DATEV2, true),
                                       "col4")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_DOUBLE, true),
                                       "col5")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_FLOAT, true),
                                       "col6")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_INT, true),
                                       "col7")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_SMALLINT, true),
                                       "col8")
                            << std::make_tuple<DataTypePtr, std::string>(
                                       DataTypeFactory::instance().create_data_type(
                                               PrimitiveType::TYPE_VARCHAR, true),
                                       "col9");
    DescriptorTbl* desc_tbl = builder.build();
    auto* tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    RowDescriptor row_desc(tuple_desc);
    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;
    TFileRangeDesc range;
    range.path = "./be/test/exec/test_data/orc_scanner/my-file.orc";
    range.start_offset = 0;
    range.size = 2024;
    range.table_format_params.table_format_type = "hive";
    range.__isset.table_format_params = true;

    io::IOContext io_ctx;
    io::FileReaderStats file_reader_stats;
    io_ctx.file_reader_stats = &file_reader_stats;
    auto reader = OrcReader::create_unique(nullptr, runtime_state.get(), params, range, 100,
                                           time_zone, &io_ctx, nullptr, true);
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr file_reader;
    static_cast<void>(reader->read_by_rows({line}));

    static_cast<void>(local_fs->open_file(range.path, &file_reader));

    auto iter = std::make_shared<RowIdColumnIteratorV2>(IdManager::ID_VERSION,
                                                        BackendOptions::get_backend_id(), 10);
    reader->register_synthesized_column_handler("row_id", [&](Block* block, size_t rows) -> Status {
        return reader->fill_topn_row_id(iter, "row_id", block, rows);
    });

    // Construct OrcInitContext for standalone reader (no column_descs).
    OrcInitContext orc_ctx;
    orc_ctx.column_names = column_names;
    orc_ctx.col_name_to_block_idx = &col_name_to_block_idx;
    orc_ctx.tuple_descriptor = tuple_desc;
    orc_ctx.row_descriptor = &row_desc;
    orc_ctx.params = &params;
    orc_ctx.range = &range;
    auto status = reader->init_reader(&orc_ctx);

    EXPECT_TRUE(status.ok());

    // set_fill_columns logic is now inlined in _do_init_reader,
    // so no separate call is needed.
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = slot_desc->type();
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    auto data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_VARCHAR, false);
    block->insert(ColumnWithTypeAndName(data_type->create_column()->assume_mutable(), data_type,
                                        "row_id"));

    bool eof = false;
    size_t read_row = 0;
    Status st = reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    auto row_id_string_column = static_cast<const ColumnString&>(
            *block->get_by_position(block->get_position_by_name("row_id")).column.get());
    for (auto i = 0; i < row_id_string_column.size(); i++) {
        GlobalRowLoacationV2 info =
                *((GlobalRowLoacationV2*)row_id_string_column.get_data_at(i).data);
        EXPECT_EQ(info.file_id, 10);
        EXPECT_EQ(info.row_id, line);
        EXPECT_EQ(info.backend_id, BackendOptions::get_backend_id());
        EXPECT_EQ(info.version, IdManager::ID_VERSION);
    }
    block->erase(block->get_position_by_name("row_id"));

    std::cout << block->dump_data();
    EXPECT_EQ(block->dump_data(), block_dump);

    range.format_type = TFileFormatType::FORMAT_ORC;
    range.__isset.format_type = true;
    range.table_format_params.table_format_type = "hive";
    range.__isset.table_format_params = true;
    std::unordered_map<std::string, int> colname_to_slot_id;
    for (auto slot : tuple_desc->slots()) {
        TFileScanSlotInfo slot_info;
        slot_info.slot_id = slot->id();
        slot_info.is_file_slot = true;
        params.required_slots.emplace_back(slot_info);
    }
    runtime_state->_timezone = time_zone;

    std::unique_ptr<RuntimeProfile> runtime_profile;
    runtime_profile = std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");

    auto vf = FileScanner::create_unique(runtime_state.get(), runtime_profile.get(), &params,
                                         &colname_to_slot_id, tuple_desc);
    EXPECT_TRUE(vf->prepare_for_read_lines(range).ok());
    ExternalFileMappingInfo external_info(0, range, false);
    int64_t init_reader_ms = 0;
    int64_t get_block_ms = 0;
    st = vf->read_lines_from_range(range, {line}, block.get(), external_info, &init_reader_ms,
                                   &get_block_ms);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_EQ(block->dump_data(1), block_dump);
}

TEST_F(OrcReadLinesTest, test0) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     0|                NULL|                 "
            "doris|                  NULL|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(0, block_dump);
}
TEST_F(OrcReadLinesTest, test1) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     1|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(1, block_dump);
}

TEST_F(OrcReadLinesTest, test2) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     2|                NULL|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(2, block_dump);
}

TEST_F(OrcReadLinesTest, test3) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     3|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(3, block_dump);
}
TEST_F(OrcReadLinesTest, test4) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     4|                NULL|                 "
            "doris|                  NULL|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(4, block_dump);
}
TEST_F(OrcReadLinesTest, test5) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     5|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(5, block_dump);
}
TEST_F(OrcReadLinesTest, test6) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     6|                NULL|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(6, block_dump);
}
TEST_F(OrcReadLinesTest, test7) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     7|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(7, block_dump);
}
TEST_F(OrcReadLinesTest, test8) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     8|                NULL|                 "
            "doris|                  NULL|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(8, block_dump);
}
TEST_F(OrcReadLinesTest, test9) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     9|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(9, block_dump);
}

TEST_F(OrcReadLinesTest, date_should_not_shift_in_west_timezone) {
    std::string block_dump =
            "+----------------------+--------------------+----------------------+------------------"
            "----+----------------------+---------------------+-------------------+----------------"
            "--------+----------------------+\n|col1(Nullable(BIGINT))|col2(Nullable(BOOL))|col3("
            "Nullable(String))|col4(Nullable(DateV2))|col5(Nullable(DOUBLE))|col6(Nullable(FLOAT))|"
            "col7(Nullable(INT))|col8(Nullable(SMALLINT))|col9(Nullable(String))|\n+---------------"
            "-------+--------------------+----------------------+----------------------+-----------"
            "-----------+---------------------+-------------------+------------------------+-------"
            "---------------+\n|                     1|                   1|                 "
            "doris|            1900-01-01|                 1.567|                1.567|            "
            "  12345|                       1|                 "
            "doris|\n+----------------------+--------------------+----------------------+----------"
            "------------+----------------------+---------------------+-------------------+--------"
            "----------------+----------------------+\n";
    read_orc_line(1, block_dump, "America/Mexico_City");
}

} // namespace doris
