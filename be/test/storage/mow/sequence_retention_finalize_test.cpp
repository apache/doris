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

#include "storage/mow/sequence_retention_finalize.h"

#include <gtest/gtest.h>

#include <atomic>
#include <numeric>

#include "core/column/column_complex.h"
#include "storage/changelog.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/sequence_retention.h"
#include "storage/txn/txn_manager.h"

namespace doris {
class SequenceRetentionFinalizerTest : public MowTransformTestBase {
protected:
    static int64_t next_id() {
        static std::atomic<int64_t> id {2000000000};
        return id.fetch_add(1);
    }

    TabletSchemaSPtr schema() {
        TabletSchemaPB pb;
        create_mow_schema(true)->to_schema_pb(&pb);
        pb.set_delete_sign_idx(3);
        auto* version = pb.add_column();
        version->set_name(VERSION_COL);
        version->set_unique_id(4);
        version->set_type("BIGINT");
        version->set_is_key(false);
        version->set_is_nullable(false);
        version->set_length(8);
        version->set_aggregation("NONE");
        version->set_default_value("0");
        pb.set_version_col_idx(4);
        auto result = std::make_shared<TabletSchema>();
        result->init_from_pb(pb);
        return result;
    }

    RowsetSharedPtr add_history(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                                int64_t start, int64_t end, const std::vector<MowRow>& rows) {
        TabletSharedPtr unused;
        auto rowset = write_rowset(schema, next_id(), end, rows, &unused);
        rowset->set_version({start, end});
        EXPECT_TRUE(tablet->add_rowset(rowset).ok());
        return rowset;
    }

    TabletTxnInfo input(const TabletSchemaSPtr& schema, int64_t plan, int64_t commit,
                        int32_t sequence, int8_t sign = 0) {
        TabletSharedPtr unused;
        TabletTxnInfo transaction;
        transaction.rowset =
                write_rowset(schema, next_id(), commit, {{1, sequence, sequence, sign}}, &unused);
        auto& meta = transaction.rowset->rowset_meta();
        meta->set_requires_sequence_retention(true);
        auto* retained = meta->mutable_sequence_retention_input();
        retained->set_plan_version(plan);
        retained->set_original_segment_count(meta->num_segments());
        transaction.delete_bitmap = std::make_shared<DeleteBitmap>(kTabletId);
        return transaction;
    }

    TCreateTabletReq replay_request(bool flexible = false, bool changelog = false) {
        auto request =
                testutil::create_tablet_request(next_id(), 1, 10, 1, TKeysType::UNIQUE_KEYS,
                                                {{"k", TPrimitiveType::INT, true},
                                                 {"v", TPrimitiveType::INT, false},
                                                 {SEQUENCE_COL, TPrimitiveType::INT, false},
                                                 {DELETE_SIGN, TPrimitiveType::TINYINT, false},
                                                 {VERSION_COL, TPrimitiveType::BIGINT, false}});
        request.__set_enable_unique_key_merge_on_write(true);
        request.tablet_schema.__set_sequence_col_idx(2);
        request.tablet_schema.__set_delete_sign_idx(3);
        request.tablet_schema.__set_version_col_idx(4);
        if (flexible) {
            request.tablet_schema.columns.push_back(testutil::create_tablet_column(
                    {SKIP_BITMAP_COL, TPrimitiveType::BITMAP, false}));
        }
        for (size_t cid = 0; cid < request.tablet_schema.columns.size(); ++cid) {
            auto& column = request.tablet_schema.columns[cid];
            if (cid != 5) {
                column.__set_default_value(cid == 1 ? "7" : "0");
            }
            column.__set_col_unique_id(cid);
            column.__set_visible(cid < 2);
        }
        if (changelog) {
            testutil::enable_changelog(&request);
            request.binlog_config.__set_need_historical_value(true);
        }
        return request;
    }

    TabletSharedPtr register_tablet(const TCreateTabletReq& request) {
        RuntimeProfile profile("sequence_replay");
        auto status = _engine->create_tablet(request, &profile);
        EXPECT_TRUE(status.ok()) << status;
        return _engine->tablet_manager()->get_tablet(request.tablet_id);
    }

    TabletTxnInfo retained_input(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                                 const std::shared_ptr<PartialUpdateInfo>& partial, Block* input,
                                 const TabletSharedPtr& changelog = nullptr) {
        auto retained = std::make_shared<SequenceRetentionContext>();
        retained->gap = 100;
        retained->plan_version = 620;
        RowsetWriterContext context;
        context.rowset_id.init(next_id());
        context.tablet = tablet;
        context.tablet_id = tablet->tablet_id();
        context.tablet_path = tablet->tablet_path();
        context.tablet_schema = schema;
        context.rowset_state = PREPARED;
        context.enable_unique_key_merge_on_write = true;
        context.write_type = DataWriteType::TYPE_DIRECT;
        context.partial_update_info = partial;
        context.mow_context = std::make_shared<MowContext>(
                620, 1, std::make_shared<RowsetIdUnorderedSet>(), std::vector<RowsetSharedPtr>(),
                std::make_shared<DeleteBitmap>(tablet->tablet_id()));
        context.mow_context->sequence_retention = retained;
        context.sequence_retention = retained;
        context.sequence_retention_enabled = true;
        auto created = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(created.has_value()) << created.error();
        auto data_writer = std::move(created).value();
        TabletTxnInfo transaction;
        if (changelog) {
            auto history_context = context;
            history_context.rowset_id.init(next_id());
            history_context.tablet = changelog;
            history_context.tablet_id = changelog->tablet_id();
            history_context.tablet_path = changelog->tablet_path();
            history_context.tablet_schema = changelog->tablet_schema();
            history_context.enable_unique_key_merge_on_write = false;
            history_context.mow_context = nullptr;
            history_context.write_changelog_opt().enable = true;
            auto history = RowsetFactory::create_rowset_writer(*_engine, history_context, false);
            EXPECT_TRUE(history.has_value()) << history.error();
            GroupRowsetWriter group;
            group.set_data_writer(RowsetWriterSharedPtr(std::move(data_writer)));
            group.set_changelog_writer(RowsetWriterSharedPtr(std::move(history).value()));
            EXPECT_TRUE(group.init(context).ok());
            auto& shared = const_cast<RowsetWriterContext&>(group.context());
            std::vector<int64_t> lsns(input->rows());
            std::iota(lsns.begin(), lsns.end(), 42);
            shared.insert_segment_allocated_lsns(
                    0, std::make_shared<const std::vector<int64_t>>(std::move(lsns)));
            EXPECT_TRUE(group.flush_single_block(input, 0).ok());
            EXPECT_TRUE(group.flush().ok());
            std::vector<RowsetSharedPtr> built;
            EXPECT_TRUE(group.build_rowsets(built).ok());
            transaction.rowset = built.at(0);
            transaction.attach_changelog.rowset = built.at(1);
            transaction.attach_changelog.rowset->set_version({621, 621});
            transaction.attach_changelog.tablet = changelog;
            transaction.attach_changelog.delete_bitmap =
                    std::make_shared<DeleteBitmap>(changelog->tablet_id());
            transaction.attach_changelog.rowset->rowset_meta()
                    ->mutable_sequence_retention_input()
                    ->set_original_segment_count(
                            transaction.attach_changelog.rowset->num_segments());
        } else {
            EXPECT_TRUE(data_writer->flush_single_block(input).ok());
            EXPECT_TRUE(data_writer->flush().ok());
            EXPECT_TRUE(data_writer->build(transaction.rowset).ok());
        }
        transaction.rowset->set_version({621, 621});
        auto* kept = transaction.rowset->rowset_meta()->mutable_sequence_retention_input();
        kept->set_original_segment_count(transaction.rowset->num_segments());
        kept->set_raw_partial_update(partial && partial->is_partial_update());
        transaction.partial_update_info = partial;
        transaction.delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        return transaction;
    }

    void calculate_early_bitmap(const TabletSharedPtr& tablet, TabletTxnInfo& transaction,
                                int64_t early_version) {
        std::vector<RowsetSharedPtr> rowsets;
        {
            std::shared_lock lock(tablet->get_header_lock());
            ASSERT_TRUE(
                    tablet->get_all_rs_id_unlocked(early_version, &transaction.rowset_ids).ok());
            rowsets = tablet->get_rowset_by_ids(&transaction.rowset_ids);
        }
        std::vector<segment_v2::SegmentSharedPtr> segments;
        ASSERT_TRUE(std::static_pointer_cast<BetaRowset>(transaction.rowset)
                            ->load_segments(&segments)
                            .ok());
        auto options = std::make_shared<SequenceRetentionBitmapOptions>();
        options->retention = std::make_shared<const SequenceRetentionContext>(
                SequenceRetentionContext {.gap = tablet->tablet_meta()->sequence_retention_gap(),
                                          .plan_version = transaction.rowset->rowset_meta()
                                                                  ->sequence_retention_input()
                                                                  .plan_version()});
        auto intra = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        ASSERT_TRUE(tablet->calc_delete_bitmap_between_segments(transaction.rowset->tablet_schema(),
                                                                transaction.rowset->rowset_id(),
                                                                segments, intra)
                            .ok());
        transaction.delete_bitmap->merge(*intra);
        transaction.sequence_retention_baseline =
                std::make_shared<SequenceRetentionBitmapBaseline>(SequenceRetentionBitmapBaseline {
                        transaction.rowset->rowset_id(), early_version, intra});
        ASSERT_TRUE(BaseTablet::calc_delete_bitmap(tablet, transaction.rowset, segments, rowsets,
                                                   transaction.delete_bitmap, early_version,
                                                   nullptr, nullptr, nullptr, options)
                            .ok());
    }

    void activate(const TabletSharedPtr& tablet) {
        ASSERT_TRUE(tablet->tablet_meta()->init_sequence_retention_gap(100).ok());
    }
};

TEST_F(SequenceRetentionFinalizerTest, EarlyLoserWinsAgainstFinalLowerSequenceLiveRow) {
    auto s = schema();
    auto tablet = make_tablet(s, kTabletId);
    add_history(s, tablet, 0, 499, {});
    auto old = add_history(s, tablet, 500, 500, {{1, 10, 100, 1}});
    add_history(s, tablet, 501, 620, {});
    activate(tablet);
    auto transaction = input(s, 550, 622, 90);
    calculate_early_bitmap(tablet, transaction, 620);
    ASSERT_TRUE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    auto live = add_history(s, tablet, 621, 621, {{1, 80, 80, 0}});
    tablet->tablet_meta()->delete_bitmap().add({old->rowset_id(), 0, 621}, 0);
    auto status = BaseTablet::update_delete_bitmap(tablet, &transaction, 1);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_TRUE(tablet->tablet_meta()->delete_bitmap().contains({live->rowset_id(), 0, 622}, 0));
    EXPECT_EQ(transaction.rowset->rowset_meta()->sequence_retention_input().finalized_version(),
              622);
    // A repeated attempt still starts from original candidates and the fixed snapshot.
    status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
}

TEST_F(SequenceRetentionFinalizerTest, DeltaMissKeepsExternalLoser) {
    auto s = schema();
    auto tablet = make_tablet(s, kTabletId);
    add_history(s, tablet, 0, 499, {});
    add_history(s, tablet, 500, 500, {{1, 10, 100, 1}});
    add_history(s, tablet, 501, 620, {});
    activate(tablet);
    auto transaction = input(s, 550, 622, 90);
    calculate_early_bitmap(tablet, transaction, 620);
    add_history(s, tablet, 621, 621, {{2, 80, 80, 0}});
    auto status = BaseTablet::update_delete_bitmap(tablet, &transaction, 1);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
}

TEST_F(SequenceRetentionFinalizerTest, StaleBaselineCanMissACleanedSuccessor) {
    auto s = schema();
    auto tablet = make_tablet(s, kTabletId);
    add_history(s, tablet, 0, 2, {});
    auto old = add_history(s, tablet, 3, 3, {{1, 10, 100, 0}});
    activate(tablet);
    auto transaction = input(s, 309, 310, 90);
    calculate_early_bitmap(tablet, transaction, 3);
    ASSERT_TRUE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    // The delete110 from version 4 removed live100. A compaction at 309 can remove it.
    tablet->tablet_meta()->delete_bitmap().add({old->rowset_id(), 0, 4}, 0);
    add_history(s, tablet, 4, 309, {});
    auto status = BaseTablet::update_delete_bitmap(tablet, &transaction, 1);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
}

TEST_F(SequenceRetentionFinalizerTest, ProtectedDeleteRejectsAndExpiredDeleteAcceptsSameInput) {
    for (int64_t plan : {550, 620}) {
        auto s = schema();
        auto tablet = make_tablet(s, kTabletId);
        add_history(s, tablet, 0, 499, {});
        auto old = add_history(s, tablet, 500, 500, {{1, 10, 100, 1}});
        add_history(s, tablet, 501, 620, {});
        activate(tablet);
        auto transaction = input(s, plan, 621, 80);
        auto status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(
                transaction.delete_bitmap->contains(
                        {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0),
                plan == 550);
        EXPECT_EQ(transaction.delete_bitmap->contains(
                          {old->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0),
                  plan == 620);
    }
}

TEST_F(SequenceRetentionFinalizerTest, OverlongTransactionDoesNotInstallBitmap) {
    auto s = schema();
    auto tablet = make_tablet(s, kTabletId);
    activate(tablet);
    auto transaction = input(s, 550, 651, 90);
    const auto before = tablet->tablet_meta()->delete_bitmap().cardinality();
    auto status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(before, tablet->tablet_meta()->delete_bitmap().cardinality());
    EXPECT_EQ(transaction.rowset->rowset_meta()->sequence_retention_input().finalized_version(), 0);
}
TEST_F(SequenceRetentionFinalizerTest, RawPartialInputUsesNewKeyDefaultsAndDiscardsPriorReplay) {
    auto request = testutil::create_tablet_request(next_id(), 1, 10, 1, TKeysType::UNIQUE_KEYS,
                                                   {{"k", TPrimitiveType::INT, true},
                                                    {"v", TPrimitiveType::INT, false},
                                                    {SEQUENCE_COL, TPrimitiveType::INT, false},
                                                    {DELETE_SIGN, TPrimitiveType::TINYINT, false},
                                                    {VERSION_COL, TPrimitiveType::BIGINT, false}});
    request.__set_enable_unique_key_merge_on_write(true);
    request.tablet_schema.__set_sequence_col_idx(2);
    request.tablet_schema.__set_version_col_idx(4);
    request.tablet_schema.__set_delete_sign_idx(3);
    for (size_t cid = 0; cid < request.tablet_schema.columns.size(); ++cid) {
        request.tablet_schema.columns[cid].__set_default_value(cid == 1 ? "7" : "0");
        request.tablet_schema.columns[cid].__set_col_unique_id(cid);
    }
    RuntimeProfile profile("sequence_replay");
    auto status = _engine->create_tablet(request, &profile);
    ASSERT_TRUE(status.ok()) << status;
    auto tablet = _engine->tablet_manager()->get_tablet(request.tablet_id);
    ASSERT_NE(tablet, nullptr);
    auto s = tablet->tablet_schema();
    ASSERT_GE(s->version_col_idx(), 0);
    add_history(s, tablet, 2, 499, {});
    auto old = add_history(s, tablet, 500, 500, {{1, 999, 100, 1}});
    add_history(s, tablet, 501, 620, {});
    activate(tablet);
    auto partial = std::make_shared<PartialUpdateInfo>();
    status = partial->init(tablet->tablet_id(), 1, *s, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                           PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL}, false, 0, 0,
                           "UTC", "");
    ASSERT_TRUE(status.ok()) << status;
    auto retained = std::make_shared<SequenceRetentionContext>();
    retained->gap = 100;
    retained->plan_version = 620;
    RowsetWriterContext writer_context;
    writer_context.rowset_id.init(next_id());
    writer_context.tablet = tablet;
    writer_context.tablet_id = tablet->tablet_id();
    writer_context.tablet_path = tablet->tablet_path();
    writer_context.tablet_schema = s;
    writer_context.rowset_state = PREPARED;
    writer_context.enable_unique_key_merge_on_write = true;
    writer_context.write_type = DataWriteType::TYPE_DIRECT;
    writer_context.partial_update_info = partial;
    writer_context.mow_context = make_mow_context(620, {old});
    writer_context.mow_context->sequence_retention = retained;
    writer_context.sequence_retention = retained;
    writer_context.sequence_retention_enabled = true;
    auto created = RowsetFactory::create_rowset_writer(*_engine, writer_context, false);
    ASSERT_TRUE(created.has_value()) << created.error();
    auto writer = std::move(created).value();
    Block original = s->create_storage_block(partial->update_cids);
    {
        auto guard = original.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        assert_cast<ColumnInt32&>(*columns[0]).insert_value(1);
        assert_cast<ColumnInt32&>(*columns[1]).insert_value(80);
    }
    status = writer->flush_single_block(&original);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(writer->flush().ok());
    TabletTxnInfo transaction;
    ASSERT_TRUE(writer->build(transaction.rowset).ok());
    transaction.rowset->set_version({621, 621});
    auto* input = transaction.rowset->rowset_meta()->mutable_sequence_retention_input();
    input->set_original_segment_count(1);
    input->set_raw_partial_update(true);
    transaction.partial_update_info = partial;
    transaction.delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
    status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(transaction.rowset->num_segments(), 2);
    EXPECT_TRUE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    MutableColumns values;
    values.push_back(ColumnInt32::create());
    values.push_back(ColumnInt32::create());
    status = BaseTablet::fetch_values_by_rowids(transaction.rowset, *s, 1, {0}, {1, 2}, values);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[0]).get_data()[0], 7);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[1]).get_data()[0], 80);
    status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(transaction.rowset->num_segments(), 3);
    EXPECT_TRUE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 1, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_FALSE(transaction.delete_bitmap->contains(
            {transaction.rowset->rowset_id(), 2, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    // Ordinary bitmap-aware reading must not expose the retained raw short-key segment.
    auto read_schema = std::make_shared<ReadSchema>(s->columns());
    ASSERT_TRUE(read_schema->init_from_tablet_schema(*s, false, false).ok());
    RowsetReaderContext read_context;
    read_context.read_schema = read_schema;
    read_context.version = {621, 621};
    read_context.enable_unique_key_merge_on_write = true;
    read_context.delete_bitmap = tablet->tablet_meta()->delete_bitmap_ptr();
    OlapReaderStatistics statistics;
    read_context.stats = &statistics;
    RowsetReaderSharedPtr reader;
    ASSERT_TRUE(transaction.rowset->create_reader(&reader).ok());
    ASSERT_TRUE(reader->init(&read_context).ok());
    Block rows = read_schema->create_read_block();
    status = reader->next_batch(&rows);
    ASSERT_TRUE(status.ok() || status.is<ErrorCode::END_OF_FILE>()) << status;
    ASSERT_EQ(rows.rows(), 1) << status << " bitmap-filtered=" << statistics.rows_del_by_bitmap;
    EXPECT_EQ(read_int(rows, 1, 0), 7);
}

TEST_F(SequenceRetentionFinalizerTest,
       FlexibleRawDuplicatesAndMissingSequenceReplayAgainstFinalState) {
    auto tablet = register_tablet(replay_request(true));
    ASSERT_NE(tablet, nullptr);
    TabletSchemaPB pb;
    tablet->tablet_schema()->to_schema_pb(&pb);
    pb.set_skip_bitmap_col_idx(
            5); // The load's request-level schema resolves hidden column positions.
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    add_history(schema, tablet, 2, 499, {});
    add_history(schema, tablet, 500, 500, {{1, 999, 100, 1}});
    add_history(schema, tablet, 501, 620, {});
    activate(tablet);
    auto partial = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(partial->init(tablet->tablet_id(), 1, *schema,
                              UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        for (int row = 0; row < 3; ++row) {
            assert_cast<ColumnInt32&>(*columns[0]).insert_value(row < 2 ? 1 : 2);
            assert_cast<ColumnInt32&>(*columns[1]).insert_value(row == 1 ? 123 : 0);
            assert_cast<ColumnInt32&>(*columns[2]).insert_value(row == 1 ? 80 : 0);
            assert_cast<ColumnInt8&>(*columns[3]).insert_value(0);
            assert_cast<ColumnInt64&>(*columns[4]).insert_value(0);
            BitmapValue skip;
            if (row != 1) {
                skip.add(1);
                skip.add(2);
            }
            assert_cast<ColumnBitmap&>(*columns[5]).insert_value(std::move(skip));
        }
    }
    auto transaction = retained_input(tablet, schema, partial, &block);
    ASSERT_EQ(transaction.rowset->num_rows(), 3); // The duplicate input was not aggregated early.
    auto status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    MutableColumns values;
    values.push_back(ColumnInt32::create());
    values.push_back(ColumnInt32::create());
    status = BaseTablet::fetch_values_by_rowids(transaction.rowset, *schema, 1, {0, 1}, {1, 2},
                                                values);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[0]).get_data()[0], 123);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[1]).get_data()[0], 80);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[0]).get_data()[1], 7);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*values[1]).get_data()[1], 0);
}

TEST_F(SequenceRetentionFinalizerTest, ChangelogDerivesFinalImagesAndKeepsOriginalLsn) {
    auto request = replay_request(false, true);
    auto tablet = register_tablet(request);
    ASSERT_NE(tablet, nullptr);
    auto history_request = request;
    history_request.tablet_id = next_id();
    history_request.tablet_schema =
            testutil::create_changelog_tablet_schema(request.tablet_schema, 2);
    history_request.tablet_schema.__set_sequence_col_idx(-1);
    history_request.tablet_schema.__set_delete_sign_idx(-1);
    history_request.tablet_schema.__set_version_col_idx(-1);
    history_request.__set_enable_unique_key_merge_on_write(false);
    history_request.__set_tablet_role(TTabletRole::TABLET_ROLE_CHANGELOG);
    history_request.tablet_schema.columns.insert(
            history_request.tablet_schema.columns.begin() + 2,
            testutil::create_tablet_column({changelog::build_before_column_name("v"),
                                            TPrimitiveType::INT, false, true,
                                            TAggregationType::NONE, true}));
    history_request.tablet_schema.columns[1].__set_is_allow_null(true);
    auto history = register_tablet(history_request);
    ASSERT_NE(history, nullptr);
    auto schema = tablet->tablet_schema();
    add_history(schema, tablet, 2, 499, {});
    add_history(schema, tablet, 500, 500, {{1, 999, 100, 1}});
    add_history(schema, tablet, 501, 620, {});
    activate(tablet);
    activate(history);
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        assert_cast<ColumnInt32&>(*columns[0]).insert_value(1);
        assert_cast<ColumnInt32&>(*columns[1]).insert_value(80);
        assert_cast<ColumnInt32&>(*columns[2]).insert_value(80);
        assert_cast<ColumnInt8&>(*columns[3]).insert_value(0);
        assert_cast<ColumnInt64&>(*columns[4]).insert_value(0);
    }
    auto transaction = retained_input(tablet, schema, nullptr, &block, history);
    auto status = finalize_sequence_retention(tablet, &transaction, 1, 0, nullptr);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(transaction.attach_changelog.rowset->num_segments(), 2);
    // Model the commit metadata installed after final decisions; the TSO column is virtual.
    transaction.attach_changelog.rowset->rowset_meta()->set_commit_tso(9001);
    const auto& history_schema = *history->tablet_schema();
    Block images = history_schema.create_storage_block();
    std::vector<uint32_t> cids(images.columns());
    std::iota(cids.begin(), cids.end(), 0);
    {
        auto guard = images.mutate_columns_scoped();
        status = BaseTablet::fetch_values_by_rowids(transaction.attach_changelog.rowset,
                                                    history_schema, 1, {0}, cids,
                                                    guard.mutable_columns());
        ASSERT_TRUE(status.ok()) << status;
    }
    EXPECT_EQ(assert_cast<const ColumnInt64&>(
                      *images.get_by_position(history_schema.changelog_lsn_col_idx()).column)
                      .get_data()[0],
              42);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(
                      *images.get_by_position(history_schema.changelog_op_col_idx()).column)
                      .get_data()[0],
              static_cast<int64_t>(ChangelogOp::APPEND));
    EXPECT_EQ(read_int(images, 1, 0), 80);
    EXPECT_TRUE(
            assert_cast<const ColumnNullable&>(*images.get_by_position(2).column).is_null_at(0));
    EXPECT_TRUE(transaction.attach_changelog.delete_bitmap->contains(
            {transaction.attach_changelog.rowset->rowset_id(), 0, 621}, 0));
}

} // namespace doris
