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
#include "format/table/iceberg_scan_semantics.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "orc/sargs/SearchArgument.hh"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/segment/column_reader.h"
#include "util/timezone_utils.h"

namespace doris {
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
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    std::vector<std::string> missing_column_names;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 100000;
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
    static_cast<void>(p_reader->read_by_rows(read_lines));

    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    static_cast<void>(p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr,
                                            nullptr, nullptr, nullptr, nullptr));
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

    auto data_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_VARCHAR, false);
    block->insert(ColumnWithTypeAndName(data_type->create_column()->assert_mutable(), data_type,
                                        "row_id"));

    bool eof = false;
    size_t read_row = 0;
    static_cast<void>(p_reader->get_next_block(block.get(), &read_row, &eof));
    auto row_id_string_column = static_cast<const ColumnString&>(
            *block->get_by_position(block->get_position_by_name("row_id")).column.get());
    auto read_lines_tmp = read_lines;
    for (auto i = 0; i < row_id_string_column.size(); i++) {
        GlobalRowLoacationV2 info =
                *((GlobalRowLoacationV2*)row_id_string_column.get_data_at(i).data);
        EXPECT_EQ(info.file_local.file_id, 10);
        EXPECT_EQ(info.file_local.row_id, read_lines_tmp.front());
        read_lines_tmp.pop_front();
        EXPECT_EQ(info.backend_id, BackendOptions::get_backend_id());
        EXPECT_EQ(info.version, IdManager::ID_VERSION);
    }
    block->erase(block->get_position_by_name("row_id"));

    EXPECT_EQ(block->dump_data(), block_dump);
    std::cout << block->dump_data();
    EXPECT_TRUE(eof);
    delete p_reader;

    scan_params.file_type = TFileType::FILE_LOCAL;
    scan_range.path =
            "./be/test/exec/test_data/parquet_scanner/"
            "type-decoder.parquet";
    scan_range.start_offset = 0;
    scan_range.size = 100000;
    scan_range.format_type = TFileFormatType::FORMAT_PARQUET;
    scan_range.__isset.format_type = true;
    scan_range.table_format_params.table_format_type = "hive";
    scan_range.__isset.table_format_params = true;
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
    EXPECT_TRUE(vf->prepare_for_read_lines(scan_range).ok());
    ExternalFileMappingInfo external_info(0, scan_range, true);
    int64_t init_reader_ms = 0;
    int64_t get_block_ms = 0;
    auto read_lines_tmp2 = read_lines;
    while (!read_lines_tmp2.empty()) {
        auto st = vf->read_lines_from_range(scan_range, {read_lines_tmp2.front()}, block.get(),
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
            R"__DORIS__(+---------------------------+------------------------------+--------------------------------+----------------------+----------------------------+--------------------------+----------------------------+
|boolean_col(Nullable(BOOL))|tinyint_col(Nullable(TINYINT))|smallint_col(Nullable(SMALLINT))|int_col(Nullable(INT))|bigint_col(Nullable(BIGINT))|float_col(Nullable(FLOAT))|double_col(Nullable(DOUBLE))|
+---------------------------+------------------------------+--------------------------------+----------------------+----------------------------+--------------------------+----------------------------+
|                          1|                             2|                               2|                     2|                           2|                      2.14|                        2.14|
|                          0|                             6|                               6|                     6|                           6|                      6.14|                        6.14|
|                          0|                             8|                               8|                     8|                           8|                      8.14|                        8.14|
+---------------------------+------------------------------+--------------------------------+----------------------+----------------------------+--------------------------+----------------------------+
)__DORIS__";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test1) {
    std::vector<std::string> numeric_types = {"boolean_col", "tinyint_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::BOOLEAN, TPrimitiveType::TINYINT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {2, 6};
    std::string block_dump =
            R"__DORIS__(+---------------------------+------------------------------+--------------------------+
|boolean_col(Nullable(BOOL))|tinyint_col(Nullable(TINYINT))|float_col(Nullable(FLOAT))|
+---------------------------+------------------------------+--------------------------+
|                          0|                            -3|                     -3.14|
|                          1|                            -7|                     -7.14|
+---------------------------+------------------------------+--------------------------+
)__DORIS__";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test2) {
    std::vector<std::string> numeric_types = {"double_col", "int_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::DOUBLE, TPrimitiveType::INT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {1, 4, 9};
    std::string block_dump =
            R"__DORIS__(+----------------------------+----------------------+--------------------------+
|double_col(Nullable(DOUBLE))|int_col(Nullable(INT))|float_col(Nullable(FLOAT))|
+----------------------------+----------------------+--------------------------+
|                        2.14|                     2|                      2.14|
|                       -5.14|                    -5|                     -5.14|
|                       10.14|                    10|                     10.14|
+----------------------------+----------------------+--------------------------+
)__DORIS__";
    read_parquet_lines(numeric_types, types, read_lines, block_dump);
}

TEST_F(ParquetReadLinesTest, test3) {
    std::vector<std::string> numeric_types = {"double_col", "int_col", "float_col"};
    std::vector<TPrimitiveType::type> types = {TPrimitiveType::DOUBLE, TPrimitiveType::INT,
                                               TPrimitiveType::FLOAT};
    std::list<int64_t> read_lines {3, 6, 8};
    std::string block_dump =
            R"__DORIS__(+----------------------------+----------------------+--------------------------+
|double_col(Nullable(DOUBLE))|int_col(Nullable(INT))|float_col(Nullable(FLOAT))|
+----------------------------+----------------------+--------------------------+
|                        4.14|                     4|                      4.14|
|                       -7.14|                    -7|                     -7.14|
|                       -9.14|                    -9|                     -9.14|
+----------------------------+----------------------+--------------------------+
)__DORIS__";
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

TEST_F(ParquetReadLinesTest, iceberg_row_id_fetch_materializes_readded_missing_column) {
    TDescriptorTable thrift_desc;
    TTableDescriptor table_desc;
    table_desc.__set_id(0);
    table_desc.__set_tableType(TTableType::OLAP_TABLE);
    table_desc.__set_numCols(0);
    table_desc.__set_numClusteringCols(0);
    thrift_desc.tableDescriptors.push_back(table_desc);
    thrift_desc.__isset.tableDescriptors = true;

    const std::vector<std::tuple<std::string, int32_t, TPrimitiveType::type>> columns {
            {"new_name", 2, TPrimitiveType::STRING},
            {"data", 3, TPrimitiveType::STRING},
            {"id", 4, TPrimitiveType::INT},
    };
    for (size_t index = 0; index < columns.size(); ++index) {
        const auto& [name, field_id, primitive_type] = columns[index];
        TTypeNode type_node;
        type_node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(primitive_type);
        type_node.__set_scalar_type(scalar_type);
        TTypeDesc type;
        type.types.push_back(type_node);

        TSlotDescriptor slot;
        slot.__set_id(cast_set<int32_t>(index));
        slot.__set_parent(0);
        slot.__set_slotType(type);
        slot.__set_columnPos(cast_set<int32_t>(index));
        slot.__set_byteOffset(0);
        slot.__set_nullIndicatorByte(0);
        slot.__set_nullIndicatorBit(cast_set<int32_t>(index));
        slot.__set_colName(name);
        slot.__set_slotIdx(cast_set<int32_t>(index));
        slot.__set_isMaterialized(true);
        slot.__set_col_unique_id(field_id);
        thrift_desc.slotDescriptors.push_back(slot);
    }
    thrift_desc.__isset.slotDescriptors = true;

    TTupleDescriptor tuple;
    tuple.__set_id(0);
    tuple.__set_byteSize(16);
    tuple.__set_numNullBytes(1);
    tuple.__set_tableId(0);
    tuple.__isset.tableId = true;
    thrift_desc.tupleDescriptors.push_back(tuple);

    ObjectPool object_pool;
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&object_pool, thrift_desc, &desc_tbl).ok());
    auto* tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    ASSERT_NE(tuple_desc, nullptr);

    const auto make_field = [](const std::string& name, int32_t field_id,
                               TPrimitiveType::type primitive_type, bool optional) {
        auto field = std::make_shared<schema::external::TField>();
        field->__set_name(name);
        field->__set_id(field_id);
        field->__set_is_optional(optional);
        TColumnType type;
        type.__set_type(primitive_type);
        field->__set_type(type);
        schema::external::TFieldPtr field_ptr;
        field_ptr.__set_field_ptr(std::move(field));
        return field_ptr;
    };
    schema::external::TStructField root;
    root.__set_fields({make_field("new_new_id", 1, TPrimitiveType::INT, false),
                       make_field("new_name", 2, TPrimitiveType::STRING, true),
                       make_field("data", 3, TPrimitiveType::STRING, false),
                       make_field("id", 4, TPrimitiveType::INT, true)});
    schema::external::TSchema schema;
    schema.__set_schema_id(4);
    schema.__set_root_field(root);

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    scan_params.__set_num_of_columns_from_file(cast_set<int32_t>(columns.size()));
    scan_params.__set_current_schema_id(4);
    scan_params.__set_history_schema_info({schema});
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    for (size_t index = 0; index < columns.size(); ++index) {
        const auto* slot = tuple_desc->slots()[index];
        TFileScanSlotInfo slot_info;
        slot_info.__set_slot_id(slot->id());
        slot_info.__set_is_file_slot(true);
        scan_params.required_slots.push_back(slot_info);
        scan_params.default_value_of_src_slot.emplace(slot->id(), TExpr {});
        scan_params.column_idxs.push_back(cast_set<int32_t>(index + 1));
        scan_params.slot_name_to_schema_pos.emplace(slot->col_name(), cast_set<int32_t>(index + 1));
    }
    scan_params.__isset.required_slots = true;
    scan_params.__isset.default_value_of_src_slot = true;
    scan_params.__isset.column_idxs = true;
    scan_params.__isset.slot_name_to_schema_pos = true;

    const std::string path =
            "./docker/thirdparties/docker-compose/iceberg/scripts/preinstalled_data/iceberg/"
            "equality_delete_par_1/data/"
            "00000-0-bd4d0a30-cdf6-48d7-933d-91e860870eb9-00001.parquet";
    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(io::global_local_filesystem()->open_file(path, &file_reader).ok());
    TFileRangeDesc range;
    range.__set_path(path);
    range.__set_start_offset(0);
    range.__set_size(file_reader->size());
    range.__set_file_size(file_reader->size());
    range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    TTableFormatFileDesc table_format;
    table_format.__set_table_format_type("iceberg");
    table_format.__set_iceberg_params(TIcebergFileDesc {});
    range.__set_table_format_params(table_format);

    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    runtime_state.set_desc_tbl(desc_tbl);
    std::unordered_map<std::string, int> colname_to_slot_id;
    Block block;
    for (const auto* slot : tuple_desc->slots()) {
        colname_to_slot_id.emplace(slot->col_name(), slot->id());
        block.insert(
                {slot->get_empty_mutable_column(), slot->get_data_type_ptr(), slot->col_name()});
    }
    RuntimeProfile profile("ExternalRowIDFetcher");
    auto scanner = FileScanner::create_unique(&runtime_state, &profile, &scan_params,
                                              &colname_to_slot_id, tuple_desc);
    ASSERT_TRUE(scanner->prepare_for_read_lines(range).ok());
    ExternalFileMappingInfo external_info(0, range, false);
    int64_t init_reader_ms = 0;
    int64_t get_block_ms = 0;
    const auto status = scanner->read_lines_from_range(range, {0}, &block, external_info,
                                                       &init_reader_ms, &get_block_ms);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(block.get_position_by_name("new_name"))
                      .column->get_data_at(0)
                      .to_string(),
              "bob");
    EXPECT_EQ(block.get_by_position(block.get_position_by_name("data"))
                      .column->get_data_at(0)
                      .to_string(),
              "e");
    EXPECT_TRUE(block.get_by_position(block.get_position_by_name("id")).column->is_null_at(0));
}

} // namespace doris
