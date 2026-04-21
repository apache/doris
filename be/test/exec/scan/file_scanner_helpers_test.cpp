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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif

#define private public
#define protected public
#include "exec/scan/file_scanner.h"
#undef private
#undef protected

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include "format/column_descriptor.h"
#include "format/table/table_schema_change_helper.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

class FileScannerHelpersTest : public ::testing::Test {
protected:
    void SetUp() override {
        _obj_pool = std::make_unique<ObjectPool>();
        _runtime_state = std::make_unique<RuntimeState>();
        _profile = std::make_unique<RuntimeProfile>("test_profile");

        TTupleDescriptor tuple_desc;
        tuple_desc.__set_id(0);
        tuple_desc.__set_byteSize(16);
        tuple_desc.__set_numNullBytes(0);
        _tuple_desc = _obj_pool->add(new TupleDescriptor(tuple_desc));

        _params = std::make_unique<TFileScanRangeParams>();
        _scanner = std::make_unique<FileScanner>(_runtime_state.get(), _profile.get(),
                                                 _params.get(), &_col_name_to_slot_id, _tuple_desc);
    }

    SlotDescriptor* add_slot(TSlotId slot_id, const std::string& col_name) {
        TSlotDescriptor tslot_desc;
        tslot_desc.id = slot_id;
        tslot_desc.parent = _tuple_desc->id();

        TTypeDesc type_desc;
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(node);

        tslot_desc.slotType = type_desc;
        tslot_desc.columnPos = -1;
        tslot_desc.colName = col_name;
        tslot_desc.byteOffset = 0;
        tslot_desc.nullIndicatorByte = 0;
        tslot_desc.nullIndicatorBit = -1;
        tslot_desc.slotIdx = static_cast<int>(slot_id);
        tslot_desc.isMaterialized = true;

        auto* slot_desc = _obj_pool->add(new SlotDescriptor(tslot_desc));
        _tuple_desc->add_slot(slot_desc);
        return slot_desc;
    }

    void add_column_desc(TSlotId slot_id, const std::string& col_name, ColumnCategory category) {
        ColumnDescriptor col_desc;
        col_desc.name = col_name;
        col_desc.slot_desc = add_slot(slot_id, col_name);
        col_desc.category = category;
        _scanner->_column_descs.push_back(col_desc);
    }

    std::unique_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _profile;
    TupleDescriptor* _tuple_desc = nullptr;
    std::unique_ptr<TFileScanRangeParams> _params;
    std::unique_ptr<FileScanner> _scanner;
    std::unordered_map<std::string, int> _col_name_to_slot_id;
};

TEST_F(FileScannerHelpersTest, FillBaseInitContextUsesScannerState) {
    add_column_desc(1, "id", ColumnCategory::REGULAR);
    add_column_desc(2, "year", ColumnCategory::PARTITION_KEY);
    _scanner->_src_block_name_to_idx["id"] = 0;
    _scanner->_current_range.path = "/warehouse/data.parquet";
    _scanner->_default_val_row_desc = std::make_unique<RowDescriptor>(_tuple_desc);

    ReaderInitContext ctx;
    _scanner->_fill_base_init_context(&ctx);

    EXPECT_EQ(&_scanner->_column_descs, ctx.column_descs);
    EXPECT_EQ(&_scanner->_src_block_name_to_idx, ctx.col_name_to_block_idx);
    EXPECT_EQ(_runtime_state.get(), ctx.state);
    EXPECT_EQ(_tuple_desc, ctx.tuple_descriptor);
    EXPECT_EQ(_scanner->_default_val_row_desc.get(), ctx.row_descriptor);
    EXPECT_EQ(_params.get(), ctx.params);
    EXPECT_EQ(&_scanner->_current_range, ctx.range);
    EXPECT_EQ(TableSchemaChangeHelper::ConstNode::get_instance(), ctx.table_info_node);
    EXPECT_EQ(TPushAggOp::type::NONE, ctx.push_down_agg_type);
}

TEST_F(FileScannerHelpersTest, GeneratePartitionColumnsUsesRealHelper) {
    _scanner->_current_range.__set_columns_from_path_keys({"year", "month", "day"});
    _scanner->_current_range.__set_columns_from_path({"2024", "__HIVE_DEFAULT_PARTITION__"});
    _scanner->_current_range.__set_columns_from_path_is_null({false, true});

    add_column_desc(1, "id", ColumnCategory::REGULAR);
    add_column_desc(2, "year", ColumnCategory::PARTITION_KEY);
    add_column_desc(3, "month", ColumnCategory::PARTITION_KEY);
    add_column_desc(4, "data", ColumnCategory::REGULAR);

    Status st = _scanner->_generate_partition_columns();

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(2, _scanner->_partition_col_descs.size());
    ASSERT_EQ(2, _scanner->_partition_value_is_null.size());

    auto year_it = _scanner->_partition_col_descs.find("year");
    ASSERT_NE(year_it, _scanner->_partition_col_descs.end());
    EXPECT_EQ("2024", std::get<0>(year_it->second));
    EXPECT_EQ("year", std::get<1>(year_it->second)->col_name());
    EXPECT_FALSE(_scanner->_partition_value_is_null["year"]);

    auto month_it = _scanner->_partition_col_descs.find("month");
    ASSERT_NE(month_it, _scanner->_partition_col_descs.end());
    EXPECT_EQ("__HIVE_DEFAULT_PARTITION__", std::get<0>(month_it->second));
    EXPECT_EQ("month", std::get<1>(month_it->second)->col_name());
    EXPECT_TRUE(_scanner->_partition_value_is_null["month"]);

    EXPECT_EQ(_scanner->_partition_col_descs.end(), _scanner->_partition_col_descs.find("day"));
    EXPECT_EQ(_scanner->_partition_col_descs.end(), _scanner->_partition_col_descs.find("id"));
}

TEST_F(FileScannerHelpersTest, GeneratePartitionColumnsWithoutPathKeysClearsState) {
    add_column_desc(1, "year", ColumnCategory::PARTITION_KEY);
    _scanner->_partition_col_descs.emplace("stale",
                                           std::make_tuple("value", _tuple_desc->slots()[0]));
    _scanner->_partition_value_is_null.emplace("stale", true);

    Status st = _scanner->_generate_partition_columns();

    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(_scanner->_partition_col_descs.empty());
    EXPECT_TRUE(_scanner->_partition_value_is_null.empty());
}

TEST_F(FileScannerHelpersTest, ConfigureFileScanHandlersUsesRealHelper) {
    _scanner->_is_load = true;
    _scanner->_configure_file_scan_handlers();
    EXPECT_EQ(&FileScanner::_init_src_block_for_load, _scanner->_init_src_block_handler);
    EXPECT_EQ(&FileScanner::_process_src_block_after_read_for_load,
              _scanner->_process_src_block_after_read_handler);
    EXPECT_EQ(&FileScanner::_should_push_down_predicates_for_load,
              _scanner->_should_push_down_predicates_handler);
    EXPECT_EQ(&FileScanner::_should_enable_condition_cache_for_load,
              _scanner->_should_enable_condition_cache_handler);

    _scanner->_is_load = false;
    _scanner->_configure_file_scan_handlers();
    EXPECT_EQ(&FileScanner::_init_src_block_for_query, _scanner->_init_src_block_handler);
    EXPECT_EQ(&FileScanner::_process_src_block_after_read_for_query,
              _scanner->_process_src_block_after_read_handler);
    EXPECT_EQ(&FileScanner::_should_push_down_predicates_for_query,
              _scanner->_should_push_down_predicates_handler);
    EXPECT_EQ(&FileScanner::_should_enable_condition_cache_for_query,
              _scanner->_should_enable_condition_cache_handler);
}

} // namespace doris
