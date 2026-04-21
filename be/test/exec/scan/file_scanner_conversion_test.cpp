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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

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

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {

class FileScannerConversionTest : public ::testing::Test {
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
        _params->num_of_columns_from_file = 2;
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

    void add_required_slot(TSlotId slot_id, bool is_file_slot,
                           std::optional<TColumnCategory::type> category = std::nullopt) {
        TFileScanSlotInfo slot_info;
        slot_info.slot_id = slot_id;
        slot_info.is_file_slot = is_file_slot;
        if (category.has_value()) {
            slot_info.__set_category(*category);
        }
        _params->required_slots.push_back(slot_info);
    }

    const ColumnDescriptor* find_column_desc(const std::string& col_name) const {
        for (const auto& col_desc : _scanner->_column_descs) {
            if (col_desc.name == col_name) {
                return &col_desc;
            }
        }
        return nullptr;
    }

    std::unique_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _profile;
    TupleDescriptor* _tuple_desc = nullptr;
    std::unique_ptr<TFileScanRangeParams> _params;
    std::unique_ptr<FileScanner> _scanner;
    std::unordered_map<std::string, int> _col_name_to_slot_id;
};

TEST_F(FileScannerConversionTest, InitExprCtxesUsesThriftCategoryAndIsFileSlotDerivation) {
    add_slot(1, "id");
    add_slot(2, "year");
    add_slot(3, "row_id");
    add_slot(4, "gen_col");

    _scanner->_current_range.__set_columns_from_path_keys({"year"});
    add_required_slot(1, true, TColumnCategory::REGULAR);
    add_required_slot(2, false, TColumnCategory::PARTITION_KEY);
    add_required_slot(3, false, TColumnCategory::SYNTHESIZED);
    add_required_slot(4, true, TColumnCategory::GENERATED);

    Status st = _scanner->_init_expr_ctxes();

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(4, _scanner->_column_descs.size());
    auto* id_col = find_column_desc("id");
    auto* year_col = find_column_desc("year");
    auto* row_id_col = find_column_desc("row_id");
    auto* gen_col = find_column_desc("gen_col");
    ASSERT_NE(nullptr, id_col);
    ASSERT_NE(nullptr, year_col);
    ASSERT_NE(nullptr, row_id_col);
    ASSERT_NE(nullptr, gen_col);
    EXPECT_EQ(ColumnCategory::REGULAR, id_col->category);
    EXPECT_EQ(ColumnCategory::PARTITION_KEY, year_col->category);
    EXPECT_EQ(ColumnCategory::SYNTHESIZED, row_id_col->category);
    EXPECT_EQ(ColumnCategory::GENERATED, gen_col->category);

    ASSERT_EQ(2, _scanner->_file_slot_descs.size());
    EXPECT_EQ("id", _scanner->_file_slot_descs[0]->col_name());
    EXPECT_EQ("gen_col", _scanner->_file_slot_descs[1]->col_name());
    EXPECT_EQ(std::vector<std::string>({"id", "gen_col"}), _scanner->_file_col_names);

    EXPECT_TRUE(_scanner->_is_file_slot.contains(1));
    EXPECT_TRUE(_scanner->_is_file_slot.contains(4));
    EXPECT_FALSE(_scanner->_is_file_slot.contains(2));
    EXPECT_FALSE(_scanner->_is_file_slot.contains(3));

    ASSERT_TRUE(_scanner->_partition_slot_index_map.contains(2));
    EXPECT_EQ(0, _scanner->_partition_slot_index_map[2]);
}

TEST_F(FileScannerConversionTest, InitExprCtxesFallsBackToPartitionKeysForOldFe) {
    add_slot(1, "data");
    add_slot(2, "month");

    _scanner->_current_range.__set_columns_from_path_keys({"month"});
    add_required_slot(1, true);
    add_required_slot(2, false);

    Status st = _scanner->_init_expr_ctxes();

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(2, _scanner->_column_descs.size());
    auto* data_col = find_column_desc("data");
    auto* month_col = find_column_desc("month");
    ASSERT_NE(nullptr, data_col);
    ASSERT_NE(nullptr, month_col);
    EXPECT_EQ(ColumnCategory::REGULAR, data_col->category);
    EXPECT_EQ(ColumnCategory::PARTITION_KEY, month_col->category);

    ASSERT_EQ(1, _scanner->_file_slot_descs.size());
    EXPECT_EQ("data", _scanner->_file_slot_descs[0]->col_name());
    EXPECT_TRUE(_scanner->_is_file_slot.contains(1));
    EXPECT_FALSE(_scanner->_is_file_slot.contains(2));
    ASSERT_TRUE(_scanner->_partition_slot_index_map.contains(2));
    EXPECT_EQ(0, _scanner->_partition_slot_index_map[2]);
}

} // namespace doris
