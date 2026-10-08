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

#include "storage/mow/historical_row_fetcher.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "cpp/sync_point.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"

namespace doris {

class HistoricalRowFetcherTest : public MowTransformTestBase {
protected:
    // A fixed partial update that carries only the key column, so `v` is the missing column the
    // fetcher has to fill.
    std::shared_ptr<PartialUpdateInfo> key_only_partial_update(const TabletSchemaSPtr& schema) {
        auto info = std::make_shared<PartialUpdateInfo>();
        EXPECT_TRUE(info->init(kTabletId, /*txn_id=*/1, *schema,
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {"k"}, /*is_strict_mode=*/false,
                               /*timestamp_ms=*/0, /*nano_seconds=*/0, "UTC", "")
                            .ok());
        return info;
    }

    // A narrow input block holding just the key column.
    static Block key_block(const TabletSchemaSPtr& schema, const std::vector<int32_t>& keys) {
        Block block = schema->create_storage_block({0});
        auto* column = block.get_by_position(0).column->assert_mutable().get();
        for (int32_t k : keys) {
            column->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        }
        return block;
    }
};

// The raw read path: whatever rows were planned come back, addressed through read_index by the
// destination position the caller planned them under.
TEST_F(HistoricalRowFetcherTest, ReadColumnsReturnsThePlannedRows) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 5001, 2, {{1, 11}, {2, 22}, {3, 33}}, &tablet);

    HistoricalRowFetcher fetcher {make_fetcher_ctx(schema, tablet)};
    fetcher.pin_rowset(rowset);
    // planned out of segment order, the way a load's incoming rows arrive
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 2}, /*dst_pos=*/0);
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 0}, /*dst_pos=*/1);
    ASSERT_EQ(fetcher.pinned_rowsets().size(), 1);

    std::vector<uint32_t> cids {1};
    Block old_values = schema->create_storage_block(cids);
    std::map<uint32_t, uint32_t> read_index;
    auto st = fetcher.read_columns(*schema, cids, old_values, &read_index,
                                   /*force_read_old_delete_signs=*/false);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(old_values.rows(), 2);
    ASSERT_EQ(read_index.size(), 2);
    EXPECT_EQ(read_int(old_values, 0, read_index[0]), 33); // dst 0 <- row 2
    EXPECT_EQ(read_int(old_values, 0, read_index[1]), 11); // dst 1 <- row 0
}

// A full row-store schema can still read a narrow projection directly from physical columns.
// Both sources must preserve planned row order and delete-sign semantics.
TEST_F(HistoricalRowFetcherTest, FixedPlanColumnStoreReadMatchesRowStore) {
    auto schema = create_row_store_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 5002, 2, {{1, 11, 0, 0}, {2, 22, 0, 1}}, &tablet);
    std::map<RowsetId, RowsetSharedPtr> rowsets {{rowset->rowset_id(), rowset}};

    FixedReadPlan read_plan;
    read_plan.prepare_to_read(RowLocation {rowset->rowset_id(), 0, 1}, /*dst_pos=*/0);
    read_plan.prepare_to_read(RowLocation {rowset->rowset_id(), 0, 0}, /*dst_pos=*/1);

    const std::vector<uint32_t> cids {0, 1};
    auto column_store_block = schema->create_storage_block(cids);
    auto row_store_block = schema->create_storage_block(cids);
    std::map<uint32_t, uint32_t> column_store_read_index;
    std::map<uint32_t, uint32_t> row_store_read_index;

    ASSERT_TRUE(read_plan
                        .read_columns_by_plan(*schema, cids, rowsets, column_store_block,
                                              &column_store_read_index,
                                              FixedReadPlan::ReadStrategy::COLUMN_STORE,
                                              /*force_read_old_delete_signs=*/true)
                        .ok());
    ASSERT_TRUE(read_plan
                        .read_columns_by_plan(*schema, cids, rowsets, row_store_block,
                                              &row_store_read_index,
                                              FixedReadPlan::ReadStrategy::PREFER_ROW_STORE,
                                              /*force_read_old_delete_signs=*/true)
                        .ok());

    EXPECT_EQ(column_store_read_index, row_store_read_index);
    EXPECT_EQ(column_store_block.dump_data(), row_store_block.dump_data());
    ASSERT_EQ(column_store_block.rows(), 2);
    EXPECT_EQ(read_int(column_store_block, 0, 0), 2);
    EXPECT_EQ(read_int(column_store_block, 0, 1), 1);
    EXPECT_EQ(read_int(column_store_block, 1, 0), 22);
    EXPECT_EQ(read_int(column_store_block, 1, 1), 11);
    EXPECT_EQ(read_tinyint(column_store_block, 2, 0), 1);
    EXPECT_EQ(read_tinyint(column_store_block, 2, 1), 0);
}

// The production publish-conflict rebuild must read the current rowset's update projection from
// physical columns, while the historical missing projection still uses the row store. The batch
// physical read must load/locate each planned segment once, not once per update column.
TEST_F(HistoricalRowFetcherTest, PublishConflictUsesBatchedColumnStoreForCurrentProjection) {
    auto schema = create_row_store_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto current_rowset = write_rowset(schema, 5003, 3, {{1, 101, 5, 0}}, &tablet);
    auto historical_rowset = write_rowset(schema, 5004, 2, {{1, 11, 3, 0}}, &tablet);

    auto partial_update_info = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(partial_update_info
                        ->init(kTabletId, /*txn_id=*/1, *schema,
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {"k", "v"},
                               /*is_strict_mode=*/false, /*timestamp_ms=*/0,
                               /*nano_seconds=*/0, "UTC", "")
                        .ok());

    FixedReadPlan read_plan_update;
    read_plan_update.prepare_to_read(
            RowLocation {current_rowset->rowset_id(), /*segment_id=*/0, /*row_id=*/0},
            /*dst_pos=*/0);
    FixedReadPlan read_plan_historical;
    read_plan_historical.prepare_to_read(
            RowLocation {historical_rowset->rowset_id(), /*segment_id=*/0, /*row_id=*/0},
            /*dst_pos=*/0);
    std::map<RowsetId, RowsetSharedPtr> rowsets {
            {current_rowset->rowset_id(), current_rowset},
            {historical_rowset->rowset_id(), historical_rowset}};

    int current_segment_loads = 0;
    int historical_segment_loads = 0;
    int current_batch_reads = 0;
    int current_row_store_reads = 0;
    int historical_row_store_reads = 0;
    size_t current_batch_column_count = 0;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard load_segment_guard;
    SyncPoint::CallbackGuard batch_read_guard;
    SyncPoint::CallbackGuard row_store_read_guard;
    sync_point->set_call_back(
            "BaseTablet::_load_segment",
            [&](auto&& args) {
                auto* rowset = try_any_cast<BetaRowset*>(args[0]);
                if (rowset->rowset_id() == current_rowset->rowset_id()) {
                    ++current_segment_loads;
                } else if (rowset->rowset_id() == historical_rowset->rowset_id()) {
                    ++historical_segment_loads;
                }
            },
            &load_segment_guard);
    sync_point->set_call_back(
            "BaseTablet::fetch_values_by_rowids",
            [&](auto&& args) {
                auto* rowset = try_any_cast<BetaRowset*>(args[0]);
                if (rowset->rowset_id() == current_rowset->rowset_id()) {
                    ++current_batch_reads;
                    current_batch_column_count =
                            try_any_cast<const std::vector<uint32_t>*>(args[1])->size();
                }
            },
            &batch_read_guard);
    sync_point->set_call_back(
            "BaseTablet::fetch_value_through_row_column",
            [&](auto&& args) {
                auto* rowset = try_any_cast<BetaRowset*>(args[0]);
                if (rowset->rowset_id() == current_rowset->rowset_id()) {
                    ++current_row_store_reads;
                } else if (rowset->rowset_id() == historical_rowset->rowset_id()) {
                    ++historical_row_store_reads;
                }
            },
            &row_store_read_guard);
    sync_point->enable_processing();

    auto output_block = schema->create_storage_block();
    auto st = BaseTablet::generate_new_block_for_partial_update(
            schema, partial_update_info.get(), read_plan_historical, read_plan_update, rowsets,
            &output_block);
    sync_point->disable_processing();

    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(output_block.rows(), 1);
    EXPECT_EQ(read_int(output_block, 0, 0), 1);
    EXPECT_EQ(read_int(output_block, 1, 0), 101);
    EXPECT_EQ(read_int(output_block, 2, 0), 3);
    EXPECT_EQ(read_tinyint(output_block, 3, 0), 0);
    EXPECT_EQ(current_batch_reads, 1);
    EXPECT_EQ(current_batch_column_count, 2);
    EXPECT_EQ(current_row_store_reads, 0);
    EXPECT_EQ(historical_row_store_reads, 1);
    EXPECT_EQ(current_segment_loads, 1);
    EXPECT_EQ(historical_segment_loads, 1);
}

// The fixed partial update fill: rows flagged for a historical read take the old value, rows
// flagged use-default take the column default.
TEST_F(HistoricalRowFetcherTest, FillMissingColumnsMixesHistoryAndDefaults) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 5011, 2, {{1, 11}, {2, 22}, {3, 33}}, &tablet);

    HistoricalRowFetcher fetcher {
            make_fetcher_ctx(schema, tablet, key_only_partial_update(schema))};
    fetcher.pin_rowset(rowset);
    // input rows are keys 1, 3 and the brand-new 99
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 0}, /*dst_pos=*/0);
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 2}, /*dst_pos=*/1);

    Block input = key_block(schema, {1, 3, 99});
    Block full_block = schema->create_storage_block();
    full_block.replace_by_position(0, input.get_by_position(0).column);
    std::vector<bool> use_default_or_null_flag {false, false, true};

    auto st = fetcher.fill_missing_columns(*schema, full_block, use_default_or_null_flag,
                                           /*has_default_or_nullable=*/true,
                                           /*segment_start_pos=*/0, &input);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(full_block.rows(), 3);
    EXPECT_EQ(read_int(full_block, 1, 0), 11);
    EXPECT_EQ(read_int(full_block, 1, 1), 33);
    EXPECT_EQ(read_int(full_block, 1, 2), 0); // column default, not NULL
    EXPECT_FALSE(read_is_null(full_block, 1, 2));
}

// A row whose historical value sits behind a delete sign gets the default instead: the old row is
// gone, there is nothing to carry forward.
TEST_F(HistoricalRowFetcherTest, FillMissingColumnsSkipsDeletedHistory) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 5021, 2, {{1, 11, 0, 0}, {2, 22, 0, 1}}, &tablet);

    HistoricalRowFetcher fetcher {
            make_fetcher_ctx(schema, tablet, key_only_partial_update(schema))};
    fetcher.pin_rowset(rowset);
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 0}, /*dst_pos=*/0);
    fetcher.plan_fixed_read(RowLocation {rowset->rowset_id(), 0, 1}, /*dst_pos=*/1);

    Block input = key_block(schema, {1, 2});
    Block full_block = schema->create_storage_block();
    full_block.replace_by_position(0, input.get_by_position(0).column);
    std::vector<bool> use_default_or_null_flag {false, false};

    auto st = fetcher.fill_missing_columns(*schema, full_block, use_default_or_null_flag,
                                           /*has_default_or_nullable=*/true,
                                           /*segment_start_pos=*/0, &input);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(full_block.rows(), 2);
    EXPECT_EQ(read_int(full_block, 1, 0), 11); // live old row
    EXPECT_EQ(read_int(full_block, 1, 1), 0);  // old row was delete-signed
}

} // namespace doris
