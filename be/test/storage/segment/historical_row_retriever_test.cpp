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

#include "storage/segment/historical_row_retriever.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/binlog.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_writer_context.h"

namespace doris {

using segment_v2::PrimaryKeyModelRowRetriever;

// The row-binlog retriever probes the primary key index for every incoming row to decide the binlog
// operator and, when needed, to read the old row.
class HistoricalRowRetrieverTest : public MowTransformTestBase {
protected:
    // Keeps the converted key column accessors alive for as long as the retriever needs them.
    class KeyAccessors {
    public:
        // `block` holds the key column, and the sequence column too when `with_seq` is set (the
        // retriever then probes with a seq suffix).
        KeyAccessors(const TabletSchemaSPtr& schema, Block* block, size_t num_rows,
                     bool with_seq = false) {
            _convertor.add_column_data_convertor(schema->column(0));
            if (with_seq) {
                _convertor.add_column_data_convertor(
                        schema->column(static_cast<uint32_t>(schema->sequence_col_idx())));
            }
            _convertor.set_source_content(block, 0, num_rows);
            auto [st, accessor] = _convertor.convert_column_data(0);
            EXPECT_TRUE(st.ok()) << st;
            _accessors.push_back(accessor);
            if (with_seq) {
                auto [seq_st, seq] = _convertor.convert_column_data(1);
                EXPECT_TRUE(seq_st.ok()) << seq_st;
                _seq_column = seq;
            }
        }

        const std::vector<IOlapColumnDataAccessor*>& get() const { return _accessors; }
        IOlapColumnDataAccessor* seq_column() const { return _seq_column; }

    private:
        OlapBlockDataConvertor _convertor;
        std::vector<IOlapColumnDataAccessor*> _accessors;
        IOlapColumnDataAccessor* _seq_column = nullptr;
    };

    std::shared_ptr<PartialUpdateInfo> key_only_partial_update(const TabletSchemaSPtr& schema) {
        auto info = std::make_shared<PartialUpdateInfo>();
        EXPECT_TRUE(info->init(kTabletId, /*txn_id=*/1, *schema,
                               UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {"k"}, /*is_strict_mode=*/false,
                               /*timestamp_ms=*/0, /*nano_seconds=*/0, "UTC", "")
                            .ok());
        return info;
    }

    void fill_rowset_ctx(RowsetWriterContext* ctx, const TabletSchemaSPtr& schema,
                         const TabletSharedPtr& tablet,
                         const std::shared_ptr<PartialUpdateInfo>& info, bool need_before) {
        ctx->tablet_id = kTabletId;
        ctx->tablet = tablet;
        ctx->tablet_schema = schema;
        ctx->partial_update_info = info;
        ctx->write_type = DataWriteType::TYPE_DIRECT;
        ctx->write_binlog_opt().enable = true;
        ctx->write_binlog_opt().set_need_before(need_before);
    }

    // A block holding the key column and the sequence column, in that order.
    static Block key_seq_block(const TabletSchemaSPtr& schema, int32_t key, int32_t seq) {
        Block block = schema->create_block_by_cids(
                {0, static_cast<uint32_t>(schema->sequence_col_idx())});
        block.get_by_position(0).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&key), sizeof(int32_t));
        block.get_by_position(1).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&seq), sizeof(int32_t));
        return block;
    }

    // A block holding only the key column, plus the full-width block the AFTER image is built into.
    static Block key_block(const TabletSchemaSPtr& schema, const std::vector<int32_t>& keys) {
        Block block = schema->create_block_by_cids({0});
        auto* column = block.get_by_position(0).column->assert_mutable().get();
        for (int32_t k : keys) {
            column->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        }
        return block;
    }
};

// An existing key becomes an update whose old value is read, a missing key becomes an append that
// takes the column default.
TEST_F(HistoricalRowRetrieverTest, UpdateReadsHistoryAndAppendTakesDefault) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 6001, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto info = key_only_partial_update(schema);

    RowsetWriterContext rowset_ctx;
    fill_rowset_ctx(&rowset_ctx, schema, tablet, info, /*need_before=*/false);

    Block input = key_block(schema, {1, 99});
    KeyAccessors keys {schema, &input, 2};

    PrimaryKeyModelRowRetriever retriever;
    ASSERT_TRUE(retriever.init(rowset_ctx.make_historical_row_retriever_context()).ok());
    ASSERT_TRUE(retriever.prepare_lookup_plan_from_source_columns(keys.get(), nullptr, mow).ok());
    auto st = retriever.retrieve_historical_row(/*delete_sign_column_data=*/nullptr, 0, 2);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(retriever.get_operators().size(), 2);
    EXPECT_EQ(retriever.get_operators()[0], ROW_BINLOG_UPDATE);
    EXPECT_EQ(retriever.get_operators()[1], ROW_BINLOG_APPEND);
    // a read-only probe: the load's delete bitmap must stay untouched
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);

    Block after_block = schema->create_block();
    after_block.replace_by_position(0, input.get_by_position(0).column);
    st = retriever.build_after_block(&after_block, 0, 2);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(read_int(after_block, 1, 0), 11); // old value of key 1
    EXPECT_EQ(read_int(after_block, 1, 1), 0);  // brand-new key 99 -> default
}

// A delete-signed row: without the BEFORE image there is nothing to read back, with it the old row
// must still be fetched so __BEFORE__* can be filled.
TEST_F(HistoricalRowRetrieverTest, DeleteReadsHistoryOnlyWhenBeforeImageIsWanted) {
    for (bool need_before : {false, true}) {
        auto schema = create_mow_schema(/*has_seq=*/false);
        TabletSharedPtr tablet;
        auto rowset = write_rowset(schema, need_before ? 6011 : 6012, 2, {{1, 11}}, &tablet);
        auto mow = make_mow_context(100, {rowset});
        auto info = key_only_partial_update(schema);

        RowsetWriterContext rowset_ctx;
        fill_rowset_ctx(&rowset_ctx, schema, tablet, info, need_before);

        Block input = key_block(schema, {1});
        KeyAccessors keys {schema, &input, 1};
        std::vector<Int8> delete_signs {1};

        PrimaryKeyModelRowRetriever retriever;
        ASSERT_TRUE(retriever.init(rowset_ctx.make_historical_row_retriever_context()).ok());
        ASSERT_TRUE(
                retriever.prepare_lookup_plan_from_source_columns(keys.get(), nullptr, mow).ok());
        auto st = retriever.retrieve_historical_row(delete_signs.data(), 0, 1);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_EQ(retriever.get_operators().size(), 1);
        EXPECT_EQ(retriever.get_operators()[0], ROW_BINLOG_DELETE);

        Block before_block = schema->create_block_by_cids({1});
        st = retriever.build_before_block(&before_block, {1}, 0, 1);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(before_block.rows(), 1);
        EXPECT_EQ(read_is_null(before_block, 0, 0), !need_before) << "need_before=" << need_before;
        if (need_before) {
            EXPECT_EQ(read_int(before_block, 0, 0), 11);
        }
    }
}

// A row that loses on sequence: the writers would stop reading the old row here, the retriever must
// not — the surviving old row is what the binlog reports. This is the retriever's
// `use_defaults_for_seq_loser = false`.
TEST_F(HistoricalRowRetrieverTest, RowLosingOnSequenceStillReadsTheStoredRow) {
    auto schema = create_mow_schema(/*has_seq=*/true); // k(0) v(1) seq(2) delete_sign(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 6031, 2, {{1, 11, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto info = key_only_partial_update(schema);

    RowsetWriterContext rowset_ctx;
    fill_rowset_ctx(&rowset_ctx, schema, tablet, info, /*need_before=*/false);

    // incoming sequence 5 < stored 10
    Block input = key_seq_block(schema, /*key=*/1, /*seq=*/5);
    KeyAccessors keys {schema, &input, 1, /*with_seq=*/true};

    PrimaryKeyModelRowRetriever retriever;
    ASSERT_TRUE(retriever.init(rowset_ctx.make_historical_row_retriever_context()).ok());
    ASSERT_TRUE(
            retriever.prepare_lookup_plan_from_source_columns(keys.get(), keys.seq_column(), mow)
                    .ok());
    auto st = retriever.retrieve_historical_row(nullptr, 0, 1);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(retriever.get_operators().size(), 1);
    EXPECT_EQ(retriever.get_operators()[0], ROW_BINLOG_UPDATE);
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U); // MarkDeleted::NONE: nothing is marked

    Block before_block = schema->create_block_by_cids({1});
    ASSERT_TRUE(retriever.build_before_block(&before_block, {1}, 0, 1).ok());
    ASSERT_EQ(before_block.rows(), 1);
    EXPECT_FALSE(read_is_null(before_block, 0, 0));
    EXPECT_EQ(read_int(before_block, 0, 0), 11); // the row that survives
}

// clear() runs between two appended blocks of the same load. The first block goes through the
// production order -- AFTER then BEFORE, both served by the same read plan -- and the second block
// is a miss, so a plan that survived clear() is observable: its stale entry would be read back into
// the BEFORE image of a row that has no history at all.
TEST_F(HistoricalRowRetrieverTest, ClearResetsThePerBlockState) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 6021, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto info = key_only_partial_update(schema);

    RowsetWriterContext rowset_ctx;
    fill_rowset_ctx(&rowset_ctx, schema, tablet, info, /*need_before=*/true);

    PrimaryKeyModelRowRetriever retriever;
    ASSERT_TRUE(retriever.init(rowset_ctx.make_historical_row_retriever_context()).ok());

    Block first = key_block(schema, {1});
    {
        KeyAccessors keys {schema, &first, 1};
        ASSERT_TRUE(
                retriever.prepare_lookup_plan_from_source_columns(keys.get(), nullptr, mow).ok());
        ASSERT_TRUE(retriever.retrieve_historical_row(nullptr, 0, 1).ok());
        ASSERT_EQ(retriever.get_operators().size(), 1);
        EXPECT_EQ(retriever.get_operators()[0], ROW_BINLOG_UPDATE);

        Block after_block = schema->create_block();
        after_block.replace_by_position(0, first.get_by_position(0).column);
        ASSERT_TRUE(retriever.build_after_block(&after_block, 0, 1).ok());
        EXPECT_EQ(read_int(after_block, 1, 0), 11);
        // the same plan still serves the BEFORE image after the AFTER image consumed it
        Block before_block = schema->create_block_by_cids({1});
        ASSERT_TRUE(retriever.build_before_block(&before_block, {1}, 0, 1).ok());
        ASSERT_EQ(before_block.rows(), 1);
        EXPECT_FALSE(read_is_null(before_block, 0, 0));
        EXPECT_EQ(read_int(before_block, 0, 0), 11);
    }
    retriever.clear();

    // key 99 is in no rowset, so nothing may be planned for it
    Block second = key_block(schema, {99});
    KeyAccessors keys {schema, &second, 1};
    ASSERT_TRUE(retriever.prepare_lookup_plan_from_source_columns(keys.get(), nullptr, mow).ok());
    ASSERT_TRUE(retriever.retrieve_historical_row(nullptr, 0, 1).ok());
    ASSERT_EQ(retriever.get_operators().size(), 1);
    EXPECT_EQ(retriever.get_operators()[0], ROW_BINLOG_APPEND);

    Block before_block = schema->create_block_by_cids({1});
    ASSERT_TRUE(retriever.build_before_block(&before_block, {1}, 0, 1).ok());
    ASSERT_EQ(before_block.rows(), 1);
    // key 1's retained plan entry would land here, since both blocks plan destination position 0
    EXPECT_TRUE(read_is_null(before_block, 0, 0));

    Block after_block = schema->create_block();
    after_block.replace_by_position(0, second.get_by_position(0).column);
    ASSERT_TRUE(retriever.build_after_block(&after_block, 0, 1).ok());
    ASSERT_EQ(after_block.rows(), 1);
    EXPECT_EQ(read_int(after_block, 1, 0), 0); // brand-new key -> column default
}

// Insert -> delete -> insert inside one load: the lookup still finds the tombstone row, so the
// operator this retriever first reported as UPDATE has to be corrected to APPEND. Rows the read
// plan never reached keep a zero old delete sign and their operator.
TEST_F(HistoricalRowRetrieverTest, ReviseOperatorWithOldDeleteSign) {
    auto delete_sign_column = ColumnInt8::create();
    delete_sign_column->insert_value(1);
    delete_sign_column->insert_value(0);
    delete_sign_column->insert_value(1);

    Block old_value_block;
    old_value_block.insert(
            {std::move(delete_sign_column), std::make_shared<DataTypeInt8>(), DELETE_SIGN});

    // row 2 of the batch has no old row, so it is absent from the read index
    std::map<uint32_t, uint32_t> read_index {
            {0, 0},
            {1, 1},
            {3, 2},
    };

    // init() is what hands the retriever its fetcher; only the schema is read here
    segment_v2::HistoricalRowRetrieverContext context;
    context.tablet_schema = create_mow_schema(/*has_seq=*/false);
    PrimaryKeyModelRowRetriever retriever;
    ASSERT_TRUE(retriever.init(context).ok());

    ASSERT_TRUE(retriever._fill_old_delete_signs(old_value_block, read_index, 4).ok());
    ASSERT_EQ((std::vector<signed char> {1, 0, 0, 1}), retriever._old_delete_signs);

    retriever._operators = {ROW_BINLOG_UPDATE, ROW_BINLOG_UPDATE, ROW_BINLOG_UPDATE,
                            ROW_BINLOG_DELETE};
    // a planned read is what tells revise() that old rows were looked up at all
    RowsetId rowset_id;
    rowset_id.init(1);
    retriever._row_fetcher->plan_fixed_read(RowLocation(rowset_id, 0, 0), 0);
    ASSERT_TRUE(retriever.revise_operators_by_old_delete_sign(4).ok());

    EXPECT_EQ(ROW_BINLOG_APPEND, retriever._operators[0]);
    EXPECT_EQ(ROW_BINLOG_UPDATE, retriever._operators[1]);
    EXPECT_EQ(ROW_BINLOG_UPDATE, retriever._operators[2]);
    EXPECT_EQ(ROW_BINLOG_DELETE, retriever._operators[3]);
}

} // namespace doris
