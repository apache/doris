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
        Block block = schema->create_block_by_cids({0});
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
    Block old_values = schema->create_block_by_cids(cids);
    std::map<uint32_t, uint32_t> read_index;
    auto st = fetcher.read_columns(*schema, cids, old_values, &read_index,
                                   /*force_read_old_delete_signs=*/false);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(old_values.rows(), 2);
    ASSERT_EQ(read_index.size(), 2);
    EXPECT_EQ(read_int(old_values, 0, read_index[0]), 33); // dst 0 <- row 2
    EXPECT_EQ(read_int(old_values, 0, read_index[1]), 11); // dst 1 <- row 0
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
    Block full_block = schema->create_block();
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
    Block full_block = schema->create_block();
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
