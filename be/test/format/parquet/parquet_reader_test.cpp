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
#include "core/string_view.h"
#include "format/parquet/vparquet_reader.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/olap_scan_common.h"
#include "util/timezone_utils.h"

namespace doris {
class VExprContext;
static VExprContextSPtrs create_predicates(DescriptorTbl* desc_tbl, RuntimeState* runtime_state);
template <bool filter_all>
static VExprContextSPtrs create_partition_predicates(DescriptorTbl* desc_tbl,
                                                     RuntimeState* runtime_state);
static VExprContextSPtrs create_only_partition_predicates(DescriptorTbl* desc_tbl,
                                                          RuntimeState* runtime_state);
static void create_table_desc(TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
                              std::vector<std::string> table_column_names,
                              std::vector<TPrimitiveType::type> types);
class ParquetReaderTest : public testing::Test {
public:
    ParquetReaderTest() : cache(1024) {}

    FileMetaCache cache;

    template <bool filter_all, bool enable_lazy>
    void all_string_null_scan() {
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;
        std::vector<std::string> table_column_names = {"string_col", "value_col"};
        std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::STRING,
                                                                TPrimitiveType::INT};
        create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
        EXPECT_TRUE(st.ok()) << st;

        auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr reader;
        st = local_fs->open_file(
                "./be/test/exec/test_data/parquet_scanner/test_string_null.zst.parquet", &reader);
        EXPECT_TRUE(st.ok()) << st;

        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
        std::vector<std::string> column_names;
        std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
        for (int i = 0; i < slot_descs.size(); i++) {
            column_names.push_back(slot_descs[i]->col_name());
            col_name_to_block_idx[slot_descs[i]->col_name()] = i;
        }
        TFileScanRangeParams scan_params;
        TFileRangeDesc scan_range;
        {
            scan_range.start_offset = 0;
            scan_range.size = 1000;
        }
        RuntimeState runtime_state = RuntimeState();
        auto p_reader =
                std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                nullptr, &runtime_state, &cache, enable_lazy);
        p_reader->set_file_reader(reader);
        runtime_state.set_desc_tbl(desc_tbl);
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
        auto conjuncts = create_predicates(desc_tbl, &runtime_state);
        std::unordered_map<int, VExprContextSPtrs> slot_id_to_expr_ctxs;
        slot_id_to_expr_ctxs[0].emplace_back(conjuncts[0]);
        slot_id_to_expr_ctxs[1].emplace_back(conjuncts[1]);

        if constexpr (filter_all) {
            st = p_reader->init_reader(column_names, &col_name_to_block_idx, conjuncts, tmp,
                                       tuple_desc, nullptr, nullptr, nullptr,
                                       &slot_id_to_expr_ctxs);
        } else {
            st = p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr,
                                       nullptr, nullptr, nullptr, nullptr);
        }

        EXPECT_TRUE(st.ok()) << st;
        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        st = p_reader->set_fill_columns(partition_columns, missing_columns);
        EXPECT_TRUE(st.ok()) << st;
        bool eof = false;
        size_t total_rows = 0;
        bool all_null = true;
        while (!eof) {
            BlockUPtr block = Block::create_unique();
            for (const auto& slot_desc : tuple_desc->slots()) {
                auto data_type = make_nullable(slot_desc->type());
                MutableColumnPtr data_column = data_type->create_column();
                block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                    slot_desc->col_name()));
            }

            size_t read_row = 0;
            st = p_reader->get_next_block(block.get(), &read_row, &eof);
            EXPECT_TRUE(st.ok()) << st;
            auto col = block->safe_get_by_position(0).column;
            auto nullable_column = assert_cast<const ColumnNullable*>(col.get());
            const auto& null_map = nullable_column->get_null_map_data();
            for (UInt8 is_null : null_map) {
                all_null &= is_null != 0;
            }
            total_rows += col->size();
        }
        EXPECT_TRUE(all_null);
        if constexpr (filter_all) {
            EXPECT_EQ(total_rows, 0);
        } else {
            EXPECT_EQ(total_rows, 10000);
        }
    }

    template <bool filter_all, bool enable_lazy>
    void all_string_null_scan_with_predicate_partition_column() {
        TDescriptorTable t_desc_table;
        TTableDescriptor t_table_desc;
        std::vector<std::string> table_column_names = {"string_col", "value_col", "part_col"};
        std::vector<TPrimitiveType::type> table_column_types = {
                TPrimitiveType::STRING, TPrimitiveType::INT, TPrimitiveType::INT};
        create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
        DescriptorTbl* desc_tbl;
        ObjectPool obj_pool;
        auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
        EXPECT_TRUE(st.ok()) << st;

        auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
        auto local_fs = io::global_local_filesystem();
        io::FileReaderSPtr reader;
        st = local_fs->open_file(
                "./be/test/exec/test_data/parquet_scanner/test_string_null.zst.parquet", &reader);
        EXPECT_TRUE(st.ok()) << st;

        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
        auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
        std::vector<std::string> column_names;
        std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
        for (int i = 0; i < slot_descs.size(); i++) {
            column_names.push_back(slot_descs[i]->col_name());
            col_name_to_block_idx[slot_descs[i]->col_name()] = i;
        }

        TFileScanRangeParams scan_params;
        TFileRangeDesc scan_range;
        scan_range.start_offset = 0;
        scan_range.size = 1000;
        RuntimeState runtime_state = RuntimeState();
        auto p_reader =
                std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                nullptr, &runtime_state, &cache, enable_lazy);
        p_reader->set_file_reader(reader);
        runtime_state.set_desc_tbl(desc_tbl);

        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
        auto conjuncts = create_partition_predicates<filter_all>(desc_tbl, &runtime_state);
        std::unordered_map<int, VExprContextSPtrs> slot_id_to_expr_ctxs;
        slot_id_to_expr_ctxs[1].emplace_back(conjuncts[0]);
        slot_id_to_expr_ctxs[2].emplace_back(conjuncts[1]);

        st = p_reader->init_reader(column_names, &col_name_to_block_idx, conjuncts, tmp, tuple_desc,
                                   nullptr, nullptr, nullptr, &slot_id_to_expr_ctxs);
        EXPECT_TRUE(st.ok()) << st;

        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        partition_columns.emplace("part_col", std::make_tuple("1", tuple_desc->slots()[2]));
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        st = p_reader->set_fill_columns(partition_columns, missing_columns);
        EXPECT_TRUE(st.ok()) << st;

        bool eof = false;
        size_t total_rows = 0;
        bool partition_all_one = true;
        while (!eof) {
            BlockUPtr block = Block::create_unique();
            for (const auto& slot_desc : tuple_desc->slots()) {
                auto data_type = slot_desc->col_name() == "part_col"
                                         ? slot_desc->type()
                                         : make_nullable(slot_desc->type());
                MutableColumnPtr data_column = data_type->create_column();
                block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                    slot_desc->col_name()));
            }

            size_t read_row = 0;
            st = p_reader->get_next_block(block.get(), &read_row, &eof);
            EXPECT_TRUE(st.ok()) << st;

            auto partition_col = block->safe_get_by_position(2).column;
            const auto* partition_col_ptr = assert_cast<const ColumnInt32*>(partition_col.get());
            const auto& partition_data = partition_col_ptr->get_data();
            for (size_t i = 0; i < partition_data.size(); ++i) {
                partition_all_one &= (partition_data[i] == 1);
            }
            total_rows += partition_col_ptr->size();
        }

        EXPECT_TRUE(partition_all_one);
        if constexpr (filter_all) {
            EXPECT_EQ(total_rows, 0);
        } else {
            EXPECT_EQ(total_rows, 10000);
        }
    }
};

static void create_table_desc(TDescriptorTable& t_desc_table, TTableDescriptor& t_table_desc,
                              std::vector<std::string> table_column_names,
                              std::vector<TPrimitiveType::type> types) {
    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    // init boolean and numeric slot
    for (int i = 0; i < table_column_names.size(); i++) {
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
            tslot_desc.colName = table_column_names[i];
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
};

TEST_F(ParquetReaderTest, normal) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"boolean_col", "tinyint_col", "smallint_col",
                                                   "int_col",     "bigint_col",  "float_col",
                                                   "double_col"};
    std::vector<TPrimitiveType::type> table_column_types = {
            TPrimitiveType::BOOLEAN, TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT,
            TPrimitiveType::INT,     TPrimitiveType::BIGINT,  TPrimitiveType::FLOAT,
            TPrimitiveType::DOUBLE};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    static_cast<void>(DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl));

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    static_cast<void>(local_fs->open_file(
            "./be/test/exec/test_data/parquet_scanner/type-decoder.parquet", &reader));

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = new ParquetReader(nullptr, scan_params, scan_range, 992, &ctz, nullptr, nullptr,
                                      &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

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
    bool eof = false;
    size_t read_row = 0;
    auto st = p_reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 10);
    }
    EXPECT_TRUE(eof);
    delete p_reader;
}

TEST_F(ParquetReaderTest, uuid_varbinary) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"id", "col1"};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::INT,
                                                            TPrimitiveType::VARBINARY};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/test_uuid.parquet", &reader);
    EXPECT_TRUE(st.ok()) << st;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    scan_params.enable_mapping_varbinary = true;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                    nullptr, nullptr, &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    st = p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr, nullptr,
                               nullptr, nullptr, nullptr);
    EXPECT_TRUE(st.ok()) << st;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    st = p_reader->set_fill_columns(partition_columns, missing_columns);
    EXPECT_TRUE(st.ok()) << st;
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = make_nullable(slot_desc->type());
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    st = p_reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(eof);
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 3);
    }
    auto col = block->safe_get_by_position(1).column;
    auto nullable_column = assert_cast<const ColumnNullable*>(col.get());
    auto varbinary_column =
            assert_cast<const ColumnVarbinary*>(nullable_column->get_nested_column_ptr().get());
    auto& data = varbinary_column->get_data();
    EXPECT_EQ(data[0].dump_hex(), "0x550E8400E29B41D4A716446655440000");
    EXPECT_EQ(data[1].dump_hex(), "0x123E4567E89B12D3A456426614174000");
    EXPECT_EQ(data[2].dump_hex(), "0x00000000000000000000000000000000");
}

TEST_F(ParquetReaderTest, varbinary_varbinary) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"id", "col2"};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::INT,
                                                            TPrimitiveType::VARBINARY};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/test_uuid.parquet", &reader);
    EXPECT_TRUE(st.ok()) << st;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    scan_params.enable_mapping_varbinary = true;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                    nullptr, nullptr, &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    st = p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr, nullptr,
                               nullptr, nullptr, nullptr);
    EXPECT_TRUE(st.ok()) << st;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    st = p_reader->set_fill_columns(partition_columns, missing_columns);
    EXPECT_TRUE(st.ok()) << st;
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = make_nullable(slot_desc->type());
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    st = p_reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(eof);
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 3);
    }
    auto col = block->safe_get_by_position(1).column;
    auto nullable_column = assert_cast<const ColumnNullable*>(col.get());
    auto varbinary_column =
            assert_cast<const ColumnVarbinary*>(nullable_column->get_nested_column_ptr().get());
    auto& data = varbinary_column->get_data();
    EXPECT_EQ(data[0].dump_hex(), "0x0123456789ABCDEF");
    EXPECT_EQ(data[1].dump_hex(), "0xFEDCBA9876543210");
    EXPECT_EQ(data[2].dump_hex(), "0x00");
}

TEST_F(ParquetReaderTest, varbinary_string) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"id", "col2"};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::INT,
                                                            TPrimitiveType::VARBINARY};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/test_uuid.parquet", &reader);
    EXPECT_TRUE(st.ok()) << st;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    // use string to read parquet column, but dst type is varbinary
    // <VarBinaryConverter<TYPE_STRING, TYPE_VARBINARY>
    scan_params.enable_mapping_varbinary = false;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                    nullptr, nullptr, &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    st = p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr, nullptr,
                               nullptr, nullptr, nullptr);
    EXPECT_TRUE(st.ok()) << st;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    st = p_reader->set_fill_columns(partition_columns, missing_columns);
    EXPECT_TRUE(st.ok()) << st;
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = make_nullable(slot_desc->type());
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    st = p_reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(eof);
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 3);
    }
    auto col = block->safe_get_by_position(1).column;
    auto nullable_column = assert_cast<const ColumnNullable*>(col.get());
    auto varbinary_column =
            assert_cast<const ColumnVarbinary*>(nullable_column->get_nested_column_ptr().get());
    auto& data = varbinary_column->get_data();
    EXPECT_EQ(data[0].dump_hex(), "0x0123456789ABCDEF");
    EXPECT_EQ(data[1].dump_hex(), "0xFEDCBA9876543210");
    EXPECT_EQ(data[2].dump_hex(), "0x00");
}

TEST_F(ParquetReaderTest, varbinary_string2) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"id", "col2"};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::INT,
                                                            TPrimitiveType::STRING};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    st = local_fs->open_file("./be/test/exec/test_data/parquet_scanner/test_uuid.parquet", &reader);
    EXPECT_TRUE(st.ok()) << st;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }
    TFileScanRangeParams scan_params;
    // although want use binary column read, _cached_src_physical_type is string, so use string to read parquet column, but dst type is string
    // <VarBinaryConverter<TYPE_STRING, TYPE_STRING>>
    scan_params.enable_mapping_varbinary = true;
    TFileRangeDesc scan_range;
    {
        scan_range.start_offset = 0;
        scan_range.size = 1000;
    }
    auto p_reader = std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                    nullptr, nullptr, &cache);
    p_reader->set_file_reader(reader);
    RuntimeState runtime_state((TQueryGlobals()));
    runtime_state.set_desc_tbl(desc_tbl);

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    st = p_reader->init_reader(column_names, &col_name_to_block_idx, {}, tmp, nullptr, nullptr,
                               nullptr, nullptr, nullptr);
    EXPECT_TRUE(st.ok()) << st;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    st = p_reader->set_fill_columns(partition_columns, missing_columns);
    EXPECT_TRUE(st.ok()) << st;
    BlockUPtr block = Block::create_unique();
    for (const auto& slot_desc : tuple_desc->slots()) {
        auto data_type = make_nullable(slot_desc->type());
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    bool eof = false;
    size_t read_row = 0;
    st = p_reader->get_next_block(block.get(), &read_row, &eof);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_TRUE(eof);
    for (auto& col : block->get_columns_with_type_and_name()) {
        ASSERT_EQ(col.column->size(), 3);
    }
    auto col = block->safe_get_by_position(1).column;
    auto nullable_column = assert_cast<const ColumnNullable*>(col.get());
    auto string_column =
            assert_cast<const ColumnString*>(nullable_column->get_nested_column_ptr().get());
    EXPECT_EQ(StringView(string_column->get_data_at(0)).dump_hex(), "0x0123456789ABCDEF");
    EXPECT_EQ(StringView(string_column->get_data_at(1)).dump_hex(), "0xFEDCBA9876543210");
    EXPECT_EQ(StringView(string_column->get_data_at(2)).dump_hex(), "0x00");
}

static VExprContextSPtrs create_predicates(DescriptorTbl* desc_tbl, RuntimeState* runtime_state) {
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    VExprSPtr root;
    {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("eq");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_STRING));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_STRING));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        texpr_node.__set_opcode(TExprOpcode::EQ);
        texpr_node.__set_fn(fn);
        texpr_node.__set_num_children(2);
        texpr_node.__set_is_nullable(true);
        root = VectorizedFnCall::create_shared(texpr_node);
    }
    { root->add_child(VSlotRef::create_shared(tuple_desc->slots()[0])); }
    {
        TExprNode texpr_node;
        texpr_node.__set_node_type(TExprNodeType::STRING_LITERAL);
        texpr_node.__set_type(create_type_desc(TYPE_STRING));
        TStringLiteral string_literal;
        string_literal.__set_value("test");
        texpr_node.__set_string_literal(string_literal);
        texpr_node.__set_is_nullable(false);
        root->add_child(VLiteral::create_shared(texpr_node));
    }
    VExprContextSPtr ctx = VExprContext::create_shared(root);

    auto st = ctx->prepare(runtime_state, row_desc);
    EXPECT_TRUE(st.ok()) << st;
    auto st1 = ctx->open(runtime_state);
    EXPECT_TRUE(st1.ok()) << st1;

    VExprContextSPtrs res = {ctx};

    VExprSPtr root2;
    {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("eq");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        texpr_node.__set_opcode(TExprOpcode::EQ);
        texpr_node.__set_fn(fn);
        texpr_node.__set_num_children(2);
        texpr_node.__set_is_nullable(true);
        root2 = VectorizedFnCall::create_shared(texpr_node);
    }
    { root2->add_child(VSlotRef::create_shared(tuple_desc->slots()[1])); }
    {
        TExprNode texpr_node;
        texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
        texpr_node.__set_type(create_type_desc(TYPE_INT));
        TIntLiteral int_literal;
        int_literal.__set_value(1);
        texpr_node.__set_int_literal(int_literal);
        texpr_node.__set_is_nullable(false);
        root2->add_child(VLiteral::create_shared(texpr_node));
    }
    VExprContextSPtr ctx2 = VExprContext::create_shared(root2);

    auto st2 = ctx2->prepare(runtime_state, row_desc);
    EXPECT_TRUE(st2.ok()) << st2;
    auto st3 = ctx2->open(runtime_state);
    EXPECT_TRUE(st3.ok()) << st3;

    res.push_back(ctx2);

    return res;
}

template <bool filter_all>
static VExprContextSPtrs create_partition_predicates(DescriptorTbl* desc_tbl,
                                                     RuntimeState* runtime_state) {
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    VExprContextSPtrs res;

    VExprSPtr value_eq_root;
    {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("eq");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        texpr_node.__set_opcode(TExprOpcode::EQ);
        texpr_node.__set_fn(fn);
        texpr_node.__set_num_children(2);
        texpr_node.__set_is_nullable(true);
        value_eq_root = VectorizedFnCall::create_shared(texpr_node);
    }
    value_eq_root->add_child(VSlotRef::create_shared(tuple_desc->slots()[1]));
    {
        TExprNode texpr_node;
        texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
        texpr_node.__set_type(create_type_desc(TYPE_INT));
        TIntLiteral int_literal;
        if constexpr (filter_all) {
            int_literal.__set_value(0);
        } else {
            int_literal.__set_value(1);
        }
        texpr_node.__set_int_literal(int_literal);
        texpr_node.__set_is_nullable(false);
        value_eq_root->add_child(VLiteral::create_shared(texpr_node));
    }
    VExprContextSPtr value_eq_ctx = VExprContext::create_shared(value_eq_root);
    auto st = value_eq_ctx->prepare(runtime_state, row_desc);
    EXPECT_TRUE(st.ok()) << st;
    st = value_eq_ctx->open(runtime_state);
    EXPECT_TRUE(st.ok()) << st;
    res.push_back(value_eq_ctx);

    VExprSPtr partition_eq_root;
    {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("eq");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        texpr_node.__set_opcode(TExprOpcode::EQ);
        texpr_node.__set_fn(fn);
        texpr_node.__set_num_children(2);
        texpr_node.__set_is_nullable(true);
        partition_eq_root = VectorizedFnCall::create_shared(texpr_node);
    }
    partition_eq_root->add_child(VSlotRef::create_shared(tuple_desc->slots()[2]));
    {
        TExprNode texpr_node;
        texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
        texpr_node.__set_type(create_type_desc(TYPE_INT));
        TIntLiteral int_literal;
        int_literal.__set_value(1);
        texpr_node.__set_int_literal(int_literal);
        texpr_node.__set_is_nullable(false);
        partition_eq_root->add_child(VLiteral::create_shared(texpr_node));
    }
    VExprContextSPtr partition_eq_ctx = VExprContext::create_shared(partition_eq_root);
    st = partition_eq_ctx->prepare(runtime_state, row_desc);
    EXPECT_TRUE(st.ok()) << st;
    st = partition_eq_ctx->open(runtime_state);
    EXPECT_TRUE(st.ok()) << st;
    res.push_back(partition_eq_ctx);

    return res;
}

static VExprContextSPtrs create_only_partition_predicates(DescriptorTbl* desc_tbl,
                                                          RuntimeState* runtime_state) {
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    VExprContextSPtrs res;

    VExprSPtr partition_eq_root;
    {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("eq");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        texpr_node.__set_opcode(TExprOpcode::EQ);
        texpr_node.__set_fn(fn);
        texpr_node.__set_num_children(2);
        texpr_node.__set_is_nullable(true);
        partition_eq_root = VectorizedFnCall::create_shared(texpr_node);
    }
    partition_eq_root->add_child(VSlotRef::create_shared(tuple_desc->slots()[0]));
    {
        TExprNode texpr_node;
        texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
        texpr_node.__set_type(create_type_desc(TYPE_INT));
        TIntLiteral int_literal;
        int_literal.__set_value(1);
        texpr_node.__set_int_literal(int_literal);
        texpr_node.__set_is_nullable(false);
        partition_eq_root->add_child(VLiteral::create_shared(texpr_node));
    }
    VExprContextSPtr partition_eq_ctx = VExprContext::create_shared(partition_eq_root);
    auto st = partition_eq_ctx->prepare(runtime_state, row_desc);
    EXPECT_TRUE(st.ok()) << st;
    st = partition_eq_ctx->open(runtime_state);
    EXPECT_TRUE(st.ok()) << st;
    res.push_back(partition_eq_ctx);

    return res;
}

TEST_F(ParquetReaderTest, all_string_null) {
    all_string_null_scan<true, true>();
    all_string_null_scan<true, false>();
    all_string_null_scan<false, true>();
    all_string_null_scan<false, false>();
}

TEST_F(ParquetReaderTest, all_string_null_with_predicate_partition_column) {
    all_string_null_scan_with_predicate_partition_column<true, true>();
    all_string_null_scan_with_predicate_partition_column<true, false>();
    all_string_null_scan_with_predicate_partition_column<false, true>();
    all_string_null_scan_with_predicate_partition_column<false, false>();
}

TEST_F(ParquetReaderTest, only_partition_column) {
    TDescriptorTable t_desc_table;
    TTableDescriptor t_table_desc;
    std::vector<std::string> table_column_names = {"part_col"};
    std::vector<TPrimitiveType::type> table_column_types = {TPrimitiveType::INT};
    create_table_desc(t_desc_table, t_table_desc, table_column_names, table_column_types);
    DescriptorTbl* desc_tbl;
    ObjectPool obj_pool;
    auto st = DescriptorTbl::create(&obj_pool, t_desc_table, &desc_tbl);
    EXPECT_TRUE(st.ok()) << st;

    auto slot_descs = desc_tbl->get_tuple_descriptor(0)->slots();
    auto local_fs = io::global_local_filesystem();
    io::FileReaderSPtr reader;
    st = local_fs->open_file(
            "./be/test/exec/test_data/parquet_scanner/test_string_null.zst.parquet", &reader);
    EXPECT_TRUE(st.ok()) << st;

    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    std::vector<std::string> column_names;
    std::unordered_map<std::string, uint32_t> col_name_to_block_idx;
    for (int i = 0; i < slot_descs.size(); i++) {
        column_names.push_back(slot_descs[i]->col_name());
        col_name_to_block_idx[slot_descs[i]->col_name()] = i;
    }

    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    scan_range.start_offset = 0;
    scan_range.size = 1000;
    RuntimeState runtime_state = RuntimeState();
    auto p_reader = std::make_unique<ParquetReader>(nullptr, scan_params, scan_range, 992, &ctz,
                                                    nullptr, &runtime_state, &cache);
    p_reader->set_file_reader(reader);
    runtime_state.set_desc_tbl(desc_tbl);

    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> tmp;
    auto conjuncts = create_only_partition_predicates(desc_tbl, &runtime_state);
    std::unordered_map<int, VExprContextSPtrs> slot_id_to_expr_ctxs;
    slot_id_to_expr_ctxs[0].emplace_back(conjuncts[0]);

    st = p_reader->init_reader(column_names, &col_name_to_block_idx, conjuncts, tmp, tuple_desc,
                               nullptr, nullptr, nullptr, &slot_id_to_expr_ctxs);
    EXPECT_TRUE(st.ok()) << st;

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    partition_columns.emplace("part_col", std::make_tuple("1", tuple_desc->slots()[0]));
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    st = p_reader->set_fill_columns(partition_columns, missing_columns);
    EXPECT_TRUE(st.ok()) << st;

    bool eof = false;
    size_t total_rows = 0;
    bool partition_all_one = true;
    while (!eof) {
        BlockUPtr block = Block::create_unique();
        for (const auto& slot_desc : tuple_desc->slots()) {
            auto data_type = slot_desc->type();
            MutableColumnPtr data_column = data_type->create_column();
            block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                slot_desc->col_name()));
        }

        size_t read_row = 0;
        st = p_reader->get_next_block(block.get(), &read_row, &eof);
        EXPECT_TRUE(st.ok()) << st;

        auto partition_col = block->safe_get_by_position(0).column;
        const auto* partition_col_ptr = assert_cast<const ColumnInt32*>(partition_col.get());
        const auto& partition_data = partition_col_ptr->get_data();
        for (size_t i = 0; i < partition_data.size(); ++i) {
            partition_all_one &= (partition_data[i] == 1);
        }
        total_rows += partition_col_ptr->size();
    }

    EXPECT_TRUE(partition_all_one);
    EXPECT_EQ(total_rows, 10000);
}

} // namespace doris
