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
#include "orc/sargs/SearchArgument.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/scan/file_scanner.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {
class VExprContext;

class ParquetReadLinesTest : public testing::Test {
public:
    ParquetReadLinesTest() {}
};

static void read_parquet_lines(std::vector<std::string> numeric_types,
                               std::vector<TPrimitiveType::type> types,
                               std::list<int64_t> read_lines, String block_dump) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    for (int i = 0; i < numeric_types.size(); i++) {
        TSlotDescriptor tslot_desc;
        {
            tslot_desc.id = i;
            tslot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(types[i]);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            tslot_desc.slotType = type;
            tslot_desc.columnPos = 0;
            tslot_desc.byteOffset = 0;
            tslot_desc.nullIndicatorByte = 0;
            tslot_desc.nullIndicatorBit = -1;
            tslot_desc.colName = numeric_types[i];
            tslot_desc.slotIdx = 0;
            tslot_desc.isMaterialized = true;
            t_desc_table.slotDescriptors.push_back(tslot_desc);
        }
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 16;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    static_cast<void>(
            local_fs->open_file("./be/test/exec/test_data/"
                                "parquet_scanner/type-decoder.parquet",
                                &reader));

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::vector<std::string> missing_column_names;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
    }
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader =
            new ParquetReader(nullptr, scan_params, scan_range, 992, &ctz, nullptr, nullptr);
    std::pair<std::shared_ptr<RowIdColumnIteratorV2>, int> iterator_pair;
    iterator_pair =
            std::make_pair(std::make_shared<RowIdColumnIteratorV2>(
                                   IdManager::ID_VERSION, BackendOptions::get_backend_id(), 10),
                           tuple_desc->slots().size());
    p_reader->set_row_id_column_iterator(iterator_pair);
    p_reader->set_file_reader(reader);
    static_cast<void>(p_reader->set_read_lines_mode(read_lines));

    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    static_cast<void>(p_reader->open());
    static_cast<void>(p_reader->init_reader(column_names, missing_column_names, nullptr, {},
                                            nullptr, nullptr, nullptr, nullptr, nullptr));
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    static_cast<void>(p_reader->set_fill_columns(partition_columns, missing_columns));
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = make_nullable(slot_desc->type());
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }

    auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_VARCHAR, false);
    block->insert(ColumnWithTypeAndName(data_type->create_column()->assume_mutable(), data_type,
                                        "row_id"));

    bool eof = false;
    size_t read_row = 0;
    static_cast<void>(p_reader->get_next_block(block.get(), &read_row, &eof));
    auto row_id_string_column =
            static_cast<const ColumnString&>(*block->get_by_name("row_id").column.get());
    auto read_lines_tmp = read_lines;
    for (auto i = 0; i < row_id_string_column.size(); i++) {
        GlobalRowLoacationV2 info =
                *((GlobalRowLoacationV2*)row_id_string_column.get_data_at(i).data);
        EXPECT_EQ(info.file_id, 10);
        EXPECT_EQ(info.row_id, read_lines_tmp.front());
        read_lines_tmp.pop_front();
        EXPECT_EQ(info.backend_id, BackendOptions::get_backend_id());
        EXPECT_EQ(info.version, IdManager::ID_VERSION);
    }
    block->erase("row_id");

    EXPECT_EQ(block->dump_data(), block_dump);
    std::cout << block->dump_data();
    EXPECT_TRUE(eof);
    delete p_reader;

    scan_params.file_type = TFileType::FILE_LOCAL;
    scan_range.path =
            "./be/test/exec/test_data/parquet_scanner/"
            "type-decoder.parquet";
    scan_range.start_offset = 0;
    scan_range.format_type = TFileFormatType::FORMAT_PARQUET;
    scan_range.__isset.format_type = true;
    std::unordered_map<std::string, int> colname_to_slot_id;
    for (auto slot : tuple_desc->slots()) {
        TFileScanSlotInfo slot_info;
        slot_info.slot_id = slot->id();
        slot_info.is_file_slot = true;
        scan_params.required_slots.emplace_back(slot_info);
    }
    runtime_state._timezone = "CST";

    std::unique_ptr<RuntimeProfile> runtime_profile;
    runtime_profile = std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");

    auto vf = FileScanner::create_unique(&runtime_state, runtime_profile.get(), &scan_params,
                                         &colname_to_slot_id, tuple_desc);
    EXPECT_TRUE(vf->prepare_for_read_one_line(scan_range).ok());
    ExternalFileMappingInfo external_info(0, scan_range, false);
    int64_t init_reader_ms = 0;
    int64_t get_block_ms = 0;

    auto read_lines_tmp2 = read_lines;
    while (!read_lines_tmp2.empty()) {
        auto st = vf->read_one_line_from_range(scan_range, read_lines_tmp2.front(), block.get(),
                                               external_info, &init_reader_ms, &get_block_ms);
        std::cout << st.to_string() << "\n";
        EXPECT_TRUE(st.ok());

        read_lines_tmp2.pop_front();
    }
    EXPECT_EQ(block->dump_data(read_lines.size()), block_dump);
}

TEST_F(ParquetReadLinesTest, test0) {
    std::vector<std::string> numeric_types = {"boolean_col", "tinyint_col", "smallint_col",
                                              "int_col",     "bigint_col",  "float_col",
                                              "double_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::BOOLEAN,  TPrimitiveType::TINYINT,
                                               TPrimitiveType::SMALLINT, TPrimitiveType::INT,
                                               TPrimitiveType::BIGINT,   TPrimitiveType::FLOAT,
                                               TPrimitiveType::DOUBLE};
    std::list<int64_t> read_lines {1, 5, 7};
    std::string block_dump =
            "+----------------------------+---------------------------+----------------------------"
            "-+------------------------+---------------------------+----------------------------+--"
            "---------------------------+\n"
            "|boolean_col(Nullable(UInt8))|tinyint_col(Nullable(Int8))|smallint_col(Nullable(Int16)"
            ")|int_col(Nullable(Int32))|bigint_col(Nullable(Int64))|float_col(Nullable(Float32))|"
            "double_col(Nullable(Float64))|\n"
            "+----------------------------+---------------------------+----------------------------"
            "-+------------------------+---------------------------+----------------------------+--"
            "---------------------------+\n"
            "|                           1|                          2|                            "
            "2|                       2|                          2|                        2.14|  "
            "                       2.14|\n"
            "|                           0|                          6|                            "
            "6|                       6|                          6|                        6.14|  "
            "                       6.14|\n"
            "|                           0|                          8|                            "
            "8|                       8|                          8|                        8.14|  "
            "                       8.14|\n"
            "+----------------------------+---------------------------+----------------------------"
            "-+------------------------+---------------------------+----------------------------+--"
            "---------------------------+\n";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test1) {
    std::vector<std::string> numeric_types = {"boolean_col", "tinyint_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::BOOLEAN, TPrimitiveType::TINYINT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {2, 6};
    std::string block_dump =
            "+----------------------------+---------------------------+----------------------------"
            "+\n"
            "|boolean_col(Nullable(UInt8))|tinyint_col(Nullable(Int8))|float_col(Nullable(Float32))"
            "|\n"
            "+----------------------------+---------------------------+----------------------------"
            "+\n"
            "|                           0|                         -3|                       "
            "-3.14|\n"
            "|                           1|                         -7|                       "
            "-7.14|\n"
            "+----------------------------+---------------------------+----------------------------"
            "+\n";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test2) {
    std::vector<std::string> numeric_types = {"double_col", "int_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::DOUBLE, TPrimitiveType::INT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {1, 4, 9};
    string block_dump =
            "+-----------------------------+------------------------+----------------------------+"
            "\n"
            "|double_col(Nullable(Float64))|int_col(Nullable(Int32))|float_col(Nullable(Float32))|"
            "\n"
            "+-----------------------------+------------------------+----------------------------+"
            "\n"
            "|                         2.14|                       2|                        "
            "2.14|\n"
            "|                        -5.14|                      -5|                       "
            "-5.14|\n"
            "|                        10.14|                      10|                       "
            "10.14|\n"
            "+-----------------------------+------------------------+----------------------------+"
            "\n";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test3) {
    std::vector<std::string> numeric_types = {"double_col", "int_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::DOUBLE, TPrimitiveType::INT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {3, 6, 8};
    std::string block_dump =
            "+-----------------------------+------------------------+----------------------------+"
            "\n"
            "|double_col(Nullable(Float64))|int_col(Nullable(Int32))|float_col(Nullable(Float32))|"
            "\n"
            "+-----------------------------+------------------------+----------------------------+"
            "\n"
            "|                         4.14|                       4|                        "
            "4.14|\n"
            "|                        -7.14|                      -7|                       "
            "-7.14|\n"
            "|                        -9.14|                      -9|                       "
            "-9.14|\n"
            "+-----------------------------+------------------------+----------------------------+"
            "\n";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test4) {
    std::vector<std::string> numeric_types = {"string_col", "char_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::STRING, TPrimitiveType::STRING};
    std::list<int64_t> read_lines {3, 6, 8};
    std::string block_dump =
            "+----------------------------+--------------------------+\n"
            "|string_col(Nullable(String))|char_col(Nullable(String))|\n"
            "+----------------------------+--------------------------+\n"
            "|                        NULL|                    c-row3|\n"
            "|                      s-row6|                    c-row6|\n"
            "|                      s-row8|                    c-row8|\n"
            "+----------------------------+--------------------------+\n";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

} // namespace vectorized
} // namespace doris
