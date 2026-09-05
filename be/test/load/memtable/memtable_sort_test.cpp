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
#include <tuple>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class MemTableSortTest : public ::testing::Test {};

TEST_F(MemTableSortTest, Tie) {
    auto t0 = Tie {0, 0};
    EXPECT_FALSE(t0.iter().next());

    auto tie = Tie {0, 1};
    EXPECT_FALSE(tie.iter().next());

    auto t = Tie {10, 30};
    for (int i = 10; i < 30; i++) {
        EXPECT_EQ(t[i], 1);
    }

    auto it1 = t.iter();
    EXPECT_TRUE(it1.next());
    EXPECT_EQ(it1.left(), 10);
    EXPECT_EQ(it1.right(), 30);

    EXPECT_FALSE(it1.next());

    t[13] = t[14] = t[22] = t[29] = 0;
    auto it2 = t.iter();

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 10);
    EXPECT_EQ(it2.right(), 13);

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 14);
    EXPECT_EQ(it2.right(), 22);

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 22);
    EXPECT_EQ(it2.right(), 29);

    EXPECT_FALSE(it2.next());
    EXPECT_FALSE(it2.next());

    // 100000000...
    for (int i = 11; i < 30; i++) {
        t[i] = 0;
    }
    EXPECT_FALSE(t.iter().next());

    // 000000000...
    t[10] = 0;
    EXPECT_FALSE(t.iter().next());

    // 000000000...001
    t[29] = 1;
    auto it3 = t.iter();
    EXPECT_TRUE(it3.next());
    EXPECT_EQ(it3.left(), 28);
    EXPECT_EQ(it3.right(), 30);

    EXPECT_FALSE(it3.next());
}

class DuplicateKeyMemTableTest : public testing::Test {
protected:
    using TestRow = std::tuple<int16_t, std::string, Array>;

    void SetUp() override {
        _smallint_type = std::make_shared<DataTypeInt16>();
        _string_type = std::make_shared<DataTypeString>();
        _array_type = std::make_shared<DataTypeArray>(_string_type);

        DescriptorTblBuilder builder(&_object_pool);
        builder.declare_tuple() << std::make_tuple(_smallint_type, "shard_num")
                                << std::make_tuple(_string_type, "label_name")
                                << std::make_tuple(_array_type, "label_value");
        _desc_tbl = builder.build();
        _tuple_desc = _desc_tbl->get_tuple_descriptor(0);

        _tablet_schema = create_tablet_schema(true);
        _resource_ctx = thread_context()->resource_ctx();
        _memtable = create_memtable(false);
    }

    std::unique_ptr<MemTable> create_memtable(bool need_lsn) const {
        return create_memtable(_tablet_schema, need_lsn);
    }

    std::unique_ptr<MemTable> create_memtable(const TabletSchemaSPtr& schema, bool need_lsn) const {
        return std::make_unique<MemTable>(1, schema, &_tuple_desc->slots(), _tuple_desc, false,
                                          nullptr, _resource_ctx, need_lsn);
    }

    TabletSchemaSPtr create_tablet_schema(bool with_keys) const {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_num_short_key_columns(with_keys ? 2 : 0);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_next_column_unique_id(4);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        schema->append_column(
                create_column(1, "shard_num", FieldType::OLAP_FIELD_TYPE_SMALLINT, with_keys, 2));
        schema->append_column(
                create_column(2, "label_name", FieldType::OLAP_FIELD_TYPE_VARCHAR, with_keys, 64));
        auto array_column =
                create_column(3, "label_value", FieldType::OLAP_FIELD_TYPE_ARRAY, false, 0);
        auto array_item_column =
                create_column(-1, "item", FieldType::OLAP_FIELD_TYPE_VARCHAR, false, 128);
        array_column.add_sub_column(array_item_column);
        schema->append_column(std::move(array_column));
        return schema;
    }

    static TabletColumn create_column(int32_t unique_id, std::string name, FieldType type,
                                      bool is_key, int32_t length) {
        TabletColumn column;
        column.set_unique_id(unique_id);
        column.set_name(std::move(name));
        column.set_type(type);
        column.set_is_key(is_key);
        column.set_is_nullable(false);
        column.set_length(length);
        column.set_index_length(length);
        return column;
    }

    Block create_block(const std::vector<TestRow>& rows) const {
        Block block;
        for (const auto* slot : _tuple_desc->slots()) {
            block.insert({slot->get_empty_mutable_column(), slot->type(), slot->col_name()});
        }
        auto columns_guard = block.mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const auto& [shard_num, label_name, label_value] : rows) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&shard_num), sizeof(shard_num));
            columns[1]->insert_data(label_name.data(), label_name.size());
            columns[2]->insert(Field::create_field<TYPE_ARRAY>(label_value));
        }
        return block;
    }

    static Array make_array(std::initializer_list<std::string> values) {
        Array array;
        array.reserve(values.size());
        for (const auto& value : values) {
            array.emplace_back(Field::create_field<TYPE_STRING>(value));
        }
        return array;
    }

    void expect_row(const Block& block, size_t row, int16_t shard_num, std::string_view label_name,
                    const Array& label_value) const {
        Field actual_shard_num;
        Field actual_label_name;
        Field actual_label_value;
        block.get_by_position(0).column->get(row, actual_shard_num);
        block.get_by_position(1).column->get(row, actual_label_name);
        block.get_by_position(2).column->get(row, actual_label_value);
        EXPECT_EQ(actual_shard_num, Field::create_field<TYPE_SMALLINT>(shard_num));
        EXPECT_EQ(actual_label_name, Field::create_field<TYPE_STRING>(std::string(label_name)));
        EXPECT_EQ(actual_label_value, Field::create_field<TYPE_ARRAY>(label_value));
    }

    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    DataTypePtr _smallint_type;
    DataTypePtr _string_type;
    DataTypePtr _array_type;
    TabletSchemaSPtr _tablet_schema;
    std::shared_ptr<ResourceContext> _resource_ctx;
    std::unique_ptr<MemTable> _memtable;
};

// Verify incremental sorting across insert batches, including the newest-first tie order for equal
// duplicate keys and correct movement of a non-POD ARRAY value column.
TEST_F(DuplicateKeyMemTableTest, SortMultipleBatchesWithDuplicateKeysAndArrays) {
    auto first = create_block({{2, "b", make_array({"first"})},
                               {1, "a", make_array({"old"})},
                               {1, "z", make_array({})}});
    ASSERT_TRUE(_memtable->insert(&first, TabletAddRowsPayload {.row_idxs = {0, 1, 2}}).ok());

    auto second = create_block({{1, "a", make_array({"new", "value"})},
                                {0, "z", make_array({"lowest"})},
                                {2, "b", make_array({"latest"})}});
    ASSERT_TRUE(_memtable->insert(&second, TabletAddRowsPayload {.row_idxs = {0, 1, 2}}).ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 6);
    expect_row(*output, 0, 0, "z", make_array({"lowest"}));
    expect_row(*output, 1, 1, "a", make_array({"new", "value"}));
    expect_row(*output, 2, 1, "a", make_array({"old"}));
    expect_row(*output, 3, 1, "z", make_array({}));
    expect_row(*output, 4, 2, "b", make_array({"latest"}));
    expect_row(*output, 5, 2, "b", make_array({"first"}));
}

// An empty insert must not create row metadata or disturb the next non-empty batch.
TEST_F(DuplicateKeyMemTableTest, EmptyBatchDoesNotAffectSort) {
    auto empty = create_block({});
    ASSERT_TRUE(_memtable->insert(&empty, TabletAddRowsPayload {}).ok());
    EXPECT_TRUE(_memtable->empty());

    auto data = create_block({{2, "b", make_array({"second"})}, {1, "a", make_array({"first"})}});
    ASSERT_TRUE(_memtable->insert(&data, TabletAddRowsPayload {.row_idxs = {0, 1}}).ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 2);
    expect_row(*output, 0, 1, "a", make_array({"first"}));
    expect_row(*output, 1, 2, "b", make_array({"second"}));
}

// Exercise the offset-sensitive append path with LSNs split across two batches, then verify that
// the row permutation and LSN permutation remain identical after sorting.
TEST_F(DuplicateKeyMemTableTest, PreserveAllocatedLsnsAfterSort) {
    auto memtable = create_memtable(true);
    auto first = create_block({{2, "b", make_array({"second"})}, {1, "a", make_array({"first"})}});
    ASSERT_TRUE(memtable->insert(&first, TabletAddRowsPayload {.row_idxs = {0, 1},
                                                               .allocated_lsns = {20, 10}})
                        .ok());

    auto second = create_block({{2, "b", make_array({"latest"})}});
    ASSERT_TRUE(memtable->insert(&second,
                                 TabletAddRowsPayload {.row_idxs = {0}, .allocated_lsns = {21}})
                        .ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 3);
    EXPECT_EQ(*memtable->allocated_lsns(), (std::vector<int64_t> {10, 21, 20}));
}

// A zero-key duplicate table bypasses sorting and swaps the input block directly. Its LSN sidecar
// must therefore remain in physical insertion order, and the sort counter must stay unchanged.
TEST_F(DuplicateKeyMemTableTest, PreserveAllocatedLsnsWithoutKeyColumns) {
    auto memtable = create_memtable(create_tablet_schema(false), true);
    auto data = create_block({{2, "b", make_array({"second"})},
                              {1, "a", make_array({"first"})},
                              {2, "b", make_array({"latest"})}});
    ASSERT_TRUE(memtable->insert(&data, TabletAddRowsPayload {.row_idxs = {0, 1, 2},
                                                              .allocated_lsns = {20, 10, 21}})
                        .ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 3);
    expect_row(*output, 0, 2, "b", make_array({"second"}));
    expect_row(*output, 1, 1, "a", make_array({"first"}));
    expect_row(*output, 2, 2, "b", make_array({"latest"}));
    EXPECT_EQ(*memtable->allocated_lsns(), (std::vector<int64_t> {20, 10, 21}));
    EXPECT_EQ(memtable->stat().sort_times.load(), 0);
}

// Check the production insert path, rather than a test-only helper: DUP_KEYS populates the compact
// contiguous permutation and never allocates RowInBlock objects.
TEST_F(DuplicateKeyMemTableTest, InsertUsesCompactContiguousRowPositions) {
    auto data = create_block({{2, "b", make_array({"second"})},
                              {1, "a", make_array({"first"})},
                              {2, "b", make_array({"latest"})}});
    ASSERT_TRUE(_memtable->insert(&data, TabletAddRowsPayload {.row_idxs = {0, 1, 2}}).ok());

    EXPECT_EQ(*_memtable->_duplicate_key_row_positions, (DorisVector<uint32_t> {0, 1, 2}));
    EXPECT_TRUE(_memtable->_row_in_blocks->empty());
    EXPECT_LT(sizeof(uint32_t) * 8, sizeof(std::shared_ptr<RowInBlock>) + sizeof(RowInBlock));
}

} // namespace doris
