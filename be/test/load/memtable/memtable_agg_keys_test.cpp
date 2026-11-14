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
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/creators.h"

namespace doris {

// AGG_KEYS resolves its aggregate functions through AggregatingMemTable::_make_agg_function, which
// the table-model split turned into a virtual hook. These tests pin the resulting behaviour end to
// end: equal keys must collapse into one row whose value columns hold the merged aggregate state.
class AggKeysMemTableTest : public testing::Test {
protected:
    void SetUp() override {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::AGG_KEYS);
        testutil::add_column_pb(&schema_pb, 0, "k", "INT", true, false);
        testutil::add_column_pb(&schema_pb, 1, "v", "INT", false, false)->set_aggregation("SUM");
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(schema_pb);

        auto tdesc = testutil::create_descriptor_table(
                {{.type = TYPE_INT, .column_name = "k", .nullable = false},
                 {.type = TYPE_INT, .column_name = "v", .nullable = false}});
        DescriptorTbl* desc_tbl = nullptr;
        ASSERT_TRUE(DescriptorTbl::create(&_pool, tdesc, &desc_tbl).ok());
        _tuple_desc = desc_tbl->get_tuple_descriptor(0);
        _memtable = MemTable::create(1, _schema, &_tuple_desc->slots(), _tuple_desc, false, nullptr,
                                     thread_context()->resource_ctx(), false);
    }

    // Builds an input block for the given (key, value) pairs. Every row is inserted, so the caller
    // controls how the rows are split across batches.
    Block create_block(const std::vector<std::pair<int32_t, int32_t>>& rows) const {
        Block block;
        for (const auto* slot : _tuple_desc->slots()) {
            block.insert({slot->get_empty_mutable_column(), slot->type(), slot->col_name()});
        }
        auto columns_guard = block.mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (const auto& [key, value] : rows) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
            columns[1]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        return block;
    }

    static TabletAddRowsPayload all_rows(size_t num_rows) {
        TabletAddRowsPayload rows;
        rows.row_idxs.reserve(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            rows.row_idxs.push_back(static_cast<uint32_t>(i));
        }
        return rows;
    }

    ObjectPool _pool;
    TabletSchemaSPtr _schema;
    TupleDescriptor* _tuple_desc = nullptr;
    std::shared_ptr<MemTable> _memtable;
};

// Two rows share key 1, so the SUM state must accumulate both values before the row is emitted.
TEST_F(AggKeysMemTableTest, MergesEqualKeysWithSumInOneBatch) {
    auto input = create_block({{2, 30}, {1, 10}, {1, 20}});
    ASSERT_TRUE(_memtable->insert(&input, all_rows(3)).ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 2);
    // AGG_KEYS sorts by key, so key 1 is emitted before key 2.
    EXPECT_EQ(output->get_by_position(0).column->get_int(0), 1);
    EXPECT_EQ(output->get_by_position(1).column->get_int(0), 30);
    EXPECT_EQ(output->get_by_position(0).column->get_int(1), 2);
    EXPECT_EQ(output->get_by_position(1).column->get_int(1), 30);
}

// The merge must also work across batches: the second batch is sorted and merged into the rows the
// first batch already aggregated.
TEST_F(AggKeysMemTableTest, MergesEqualKeysWithSumAcrossBatches) {
    auto first = create_block({{1, 10}, {2, 30}});
    ASSERT_TRUE(_memtable->insert(&first, all_rows(2)).ok());
    auto second = create_block({{1, 20}});
    ASSERT_TRUE(_memtable->insert(&second, all_rows(1)).ok());

    std::unique_ptr<Block> output;
    ASSERT_TRUE(_memtable->to_block(&output).ok());
    ASSERT_EQ(output->rows(), 2);
    EXPECT_EQ(output->get_by_position(0).column->get_int(0), 1);
    EXPECT_EQ(output->get_by_position(1).column->get_int(0), 30);
    EXPECT_EQ(output->get_by_position(0).column->get_int(1), 2);
    EXPECT_EQ(output->get_by_position(1).column->get_int(1), 30);
}

} // namespace doris
