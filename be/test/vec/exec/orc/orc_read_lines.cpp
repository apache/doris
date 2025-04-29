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
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/orc/orc_memory_pool.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/scan/file_scanner.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {
class VExprContext;

class OrcReadLinesTest : public testing::Test {
public:
    OrcReadLinesTest() {}
};

static void read_orc_line(int64_t line, std::string block_dump) {
    auto runtime_state = RuntimeState::create_unique();

    std::vector<std::string> column_names = {"col1", "col2", "col3", "col4", "col5",
                                             "col6", "col7", "col8", "col9"};
    ObjectPool object_pool;
    DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << std::make_tuple(TYPE_BIGINT, "col1")
                            << std::make_tuple(TYPE_BOOLEAN, "col2")
                            << std::make_tuple(TYPE_STRING, "col3")
                            << std::make_tuple(TYPE_DATEV2, "col4")
                            << std::make_tuple(TYPE_DOUBLE, "col5")
                            << std::make_tuple(TYPE_FLOAT, "col6")
                            << std::make_tuple(TYPE_INT, "col7")
                            << std::make_tuple(TYPE_SMALLINT, "col8")
                            << std::make_tuple(TYPE_STRING, "col9");
    DescriptorTbl* desc_tbl = builder.build();
    auto* tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    RowDescriptor row_desc(tuple_desc, false);
    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;
    TFileRangeDesc range;
    range.path = "./be/test/exec/test_data/orc_scanner/my-file.orc";
    range.start_offset = 0;
    range.size = 2024;

    io::IOContext io_ctx;
    std::string time_zone = "CST";
    auto reader = OrcReader::create_unique(nullptr, runtime_state.get(), params, range, 100,
                                           time_zone, &io_ctx, true);
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr file_reader;
    static_cast<void>(reader->set_read_lines_mode({line}));

    static_cast<void>(local_fs->open_file(range.path, &file_reader));

    std::pair<std::shared_ptr<RowIdColumnIteratorV2>, int> iterator_pair;
    iterator_pair =
            std::make_pair(std::make_shared<RowIdColumnIteratorV2>(
                                   IdManager::ID_VERSION, BackendOptions::get_backend_id(), 10),
                           tuple_desc->slots().size());
    reader->set_row_id_column_iterator(iterator_pair);

    auto status = reader->init_reader(&column_names, {}, nullptr, {}, false, tuple_desc, &row_desc,
                                      nullptr, nullptr);

    EXPECT_TRUE(status.ok());

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    static_cast<void>(reader->set_fill_columns(partition_columns, missing_columns));
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(slot_desc->type(), true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    auto data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TypeIndex::String, false);
    block->insert(ColumnWithTypeAndName(data_type->create_column()->assume_mutable(), data_type,
                                        "row_id"));

    bool eof = false;
    size_t read_row = 0;
    static_cast<void>(reader->get_next_block(block.get(), &read_row, &eof));
    auto row_id_string_column =
            static_cast<const ColumnString&>(*block->get_by_name("row_id").column.get());
    for (auto i = 0; i < row_id_string_column.size(); i++) {
        GlobalRowLoacationV2 info =
                *((GlobalRowLoacationV2*)row_id_string_column.get_data_at(i).data);
        EXPECT_EQ(info.file_id, 10);
        EXPECT_EQ(info.row_id, line);
        EXPECT_EQ(info.backend_id, BackendOptions::get_backend_id());
        EXPECT_EQ(info.version, IdManager::ID_VERSION);
    }
    block->erase("row_id");

    std::cout << block->dump_data();
    EXPECT_EQ(block->dump_data(), block_dump);

    range.format_type = TFileFormatType::FORMAT_ORC;
    range.__isset.format_type = true;
    std::unordered_map<std::string, int> colname_to_slot_id;
    for (auto slot : tuple_desc->slots()) {
        TFileScanSlotInfo slot_info;
        slot_info.slot_id = slot->id();
        slot_info.is_file_slot = true;
        params.required_slots.emplace_back(slot_info);
    }
    runtime_state->_timezone = "CST";

    std::unique_ptr<RuntimeProfile> runtime_profile;
    runtime_profile = std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");

    auto vf = FileScanner::create_unique(runtime_state.get(), runtime_profile.get(), &params,
                                          &colname_to_slot_id, tuple_desc);
    EXPECT_TRUE(vf->prepare_for_read_one_line(range).ok());
    ExternalFileMappingInfo external_info(0, range, false);
    int64_t init_reader_ms = 0;
    int64_t get_block_ms = 0;
    auto st = vf->read_one_line_from_range(range, line, block.get(), external_info, &init_reader_ms,
                                           &get_block_ms);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(block->dump_data(1), block_dump);
}

TEST_F(OrcReadLinesTest, test0) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    0|                 NULL|                 doris|                  "
            "NULL|                  1.567|                  1.567|                12345|           "
            "         1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(0, block_dump);
}
TEST_F(OrcReadLinesTest, test1) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    1|                    1|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(1, block_dump);
}

TEST_F(OrcReadLinesTest, test2) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    2|                 NULL|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(2, block_dump);
}

TEST_F(OrcReadLinesTest, test3) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    3|                    1|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(3, block_dump);
}
TEST_F(OrcReadLinesTest, test4) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    4|                 NULL|                 doris|                  "
            "NULL|                  1.567|                  1.567|                12345|           "
            "         1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(4, block_dump);
}
TEST_F(OrcReadLinesTest, test5) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    5|                    1|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(5, block_dump);
}
TEST_F(OrcReadLinesTest, test6) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    6|                 NULL|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(6, block_dump);
}
TEST_F(OrcReadLinesTest, test7) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    7|                    1|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(7, block_dump);
}
TEST_F(OrcReadLinesTest, test8) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    8|                 NULL|                 doris|                  "
            "NULL|                  1.567|                  1.567|                12345|           "
            "         1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(8, block_dump);
}
TEST_F(OrcReadLinesTest, test9) {
    std::string block_dump =
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|col1(Nullable(Int64))|col2(Nullable(UInt8))|col3(Nullable(String))|col4(Nullable("
            "DateV2))|col5(Nullable(Float64))|col6(Nullable(Float32))|col7(Nullable(Int32))|col8("
            "Nullable(Int16))|col9(Nullable(String))|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n"
            "|                    9|                    1|                 doris|            "
            "1900-01-01|                  1.567|                  1.567|                12345|     "
            "               1|                 doris|\n"
            "+---------------------+---------------------+----------------------+------------------"
            "----+-----------------------+-----------------------+---------------------+-----------"
            "----------+----------------------+\n";
    read_orc_line(9, block_dump);
}

} // namespace vectorized
} // namespace doris
