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

#include "storage/mow/key_probe.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "service/point_query_executor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

using segment_v2::KeyProbeResult;
using segment_v2::MowKeyProbe;

class KeyProbeTest : public MowTransformTestBase {
protected:
    static constexpr int64_t kWritingRowsetId = 4002;

    MowKeyProbe make_probe(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                           const std::shared_ptr<MowContext>& mow, MowKeyProbe::Policy policy,
                           uint32_t writing_segment_id = 0) {
        RowsetId writing_rowset_id;
        writing_rowset_id.init(kWritingRowsetId);
        return MowKeyProbe {tablet.get(), schema.get(),      schema->has_sequence_col(),
                            mow,          writing_rowset_id, writing_segment_id,
                            policy};
    }

    // Built through the factory both segment writers call, so a policy field drifting away from
    // the fill paths fails these tests rather than silently changing production behavior.
    MowKeyProbe make_fill_probe(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                                const std::shared_ptr<MowContext>& mow, bool flexible = false,
                                uint32_t writing_segment_id = 0) {
        return MowKeyProbe::for_partial_update(tablet.get(), schema.get(),
                                               schema->has_sequence_col(), mow, writing_rowset_id(),
                                               writing_segment_id, flexible);
    }

    // The policy both partial update fill paths use, for the tests that then flip one field to
    // show what that field alone controls.
    static MowKeyProbe::Policy fill_policy() {
        return MowKeyProbe::Policy {
                .mark_deleted = MowKeyProbe::MarkDeleted::OLD_AND_LOSING_ROW,
                .use_defaults_for_delete_signed = true,
                .use_defaults_for_seq_loser = true,
                .use_defaults_for_in_load_deleted = false,
        };
    }

    // True iff the delete bitmap has `row_id` marked under the TEMP version for {rowset_id,
    // segment_id}.
    static bool marked(const std::shared_ptr<MowContext>& mow, const RowsetId& rsid,
                       uint32_t segment_id, uint32_t row_id) {
        return mow->delete_bitmap->contains({rsid, segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                                            row_id);
    }

    static RowsetId writing_rowset_id() {
        RowsetId rsid;
        rsid.init(kWritingRowsetId);
        return rsid;
    }
};

// A key that no rowset holds: brand-new row, nothing marked, and the caller is told to fill
// defaults.
TEST_F(KeyProbeTest, NotFoundReportsNewRow) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4001, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto probe = make_fill_probe(schema, tablet, mow);

    auto result = probe.probe(encode_key(schema, encoder, 99), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/false, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::NOT_FOUND);
    EXPECT_TRUE(result->use_default_or_null);
    EXPECT_EQ(result->rowset, nullptr);
    EXPECT_EQ(stats.num_rows_new_added, 1);
    EXPECT_EQ(stats.num_rows_updated, 0);
    EXPECT_EQ(stats.num_rows_deleted, 0);
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
}

// An existing key: the old row is located and marked deleted, its rowset is handed back so the
// caller can pin it, and the old values must be read.
TEST_F(KeyProbeTest, FoundMarksOldRowAndReturnsItsRowset) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4011, 2, {{1, 11}, {2, 22}, {3, 33}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto probe = make_fill_probe(schema, tablet, mow);

    auto result = probe.probe(encode_key(schema, encoder, 2), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/false, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND);
    EXPECT_FALSE(result->use_default_or_null);
    ASSERT_NE(result->rowset, nullptr);
    EXPECT_EQ(result->rowset->rowset_id(), rowset->rowset_id());
    EXPECT_EQ(result->loc.rowset_id, rowset->rowset_id());
    EXPECT_EQ(result->loc.segment_id, 0);
    EXPECT_EQ(result->loc.row_id, 1U); // key 2 is the second row of the segment

    EXPECT_EQ(stats.num_rows_updated, 1);
    EXPECT_EQ(stats.num_rows_new_added, 0);
    EXPECT_EQ(stats.num_rows_deleted, 0);
    // only the old row is marked; the row being written stays alive
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 1));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// The row being written carries the larger sequence value, so it wins: the old row is marked and
// its values are still read for the missing columns.
TEST_F(KeyProbeTest, HigherSequenceWins) {
    auto schema = create_mow_schema(/*has_seq=*/true); // k(0) v(1) seq(2) delete_sign(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4101, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto probe = make_fill_probe(schema, tablet, mow);

    auto result = probe.probe(encode_key_with_seq(schema, encoder, 1, 10), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND);
    EXPECT_FALSE(result->use_default_or_null);
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_FALSE(marked(mow, writing_rowset_id(), 0, 0));
    EXPECT_EQ(stats.num_rows_updated, 1);
    EXPECT_EQ(stats.num_rows_deleted, 0);
}

// Equal sequence values are not a loss: the incoming row still replaces the old one. This pins the
// boundary of the "old row is newer" comparison.
TEST_F(KeyProbeTest, EqualSequenceIsFoundNotFoundNewer) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4111, 2, {{2, 22, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto probe = make_fill_probe(schema, tablet, mow);

    auto result = probe.probe(encode_key_with_seq(schema, encoder, 2, 10), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND);
    EXPECT_FALSE(result->use_default_or_null);
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_EQ(stats.num_rows_updated, 1);
}

// A delete of a key that does not exist yet: still a brand-new row. The probe counts it, and
// deciding whether to reject it (handle_new_key) stays with the caller, which skips that call for
// delete-signed rows.
TEST_F(KeyProbeTest, DeleteSignOnMissingKeyIsStillANewRow) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4121, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto probe = make_fill_probe(schema, tablet, mow);

    auto result = probe.probe(encode_key(schema, encoder, 99), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/false, /*have_delete_sign=*/true, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::NOT_FOUND);
    EXPECT_EQ(result->rowset, nullptr);
    EXPECT_EQ(stats.num_rows_new_added, 1);
    EXPECT_EQ(stats.num_rows_updated, 0);
    EXPECT_EQ(stats.num_rows_deleted, 0);
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
}

// The old row has the larger sequence value, so the row being written loses: it is the one marked
// deleted (in the segment under construction), and with use_defaults_for_seq_loser the caller does
// not read the old values.
TEST_F(KeyProbeTest, FoundNewerMarksTheIncomingRow) {
    auto schema = create_mow_schema(/*has_seq=*/true); // k(0) v(1) seq(2) delete_sign(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4021, 2, {{1, 11, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    // a segment id other than 0, so the mark has to come from the probe's writing_segment_id
    auto probe = make_fill_probe(schema, tablet, mow, /*flexible=*/false, /*writing_segment_id=*/3);

    // incoming seq 5 < the old row seq 10
    auto result = probe.probe(encode_key_with_seq(schema, encoder, 1, 5), /*segment_pos=*/7,
                              /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND_NEWER);
    EXPECT_TRUE(result->use_default_or_null);
    EXPECT_EQ(stats.num_rows_deleted, 1);
    EXPECT_EQ(stats.num_rows_updated, 0);
    // the losing row of the segment being written is marked, the old row is not
    EXPECT_TRUE(marked(mow, writing_rowset_id(), 3, 7));
    EXPECT_FALSE(marked(mow, writing_rowset_id(), 0, 7));
    EXPECT_FALSE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// Same losing row, but with use_defaults_for_seq_loser off (the row binlog policy): the surviving
// old row still has to be read, so its rowset comes back.
TEST_F(KeyProbeTest, FoundNewerReadsOldRowWhenDefaultsForSeqLoserIsOff) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4031, 2, {{1, 11, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto policy = fill_policy();
    policy.use_defaults_for_seq_loser = false;
    auto probe = make_probe(schema, tablet, mow, policy);

    auto result = probe.probe(encode_key_with_seq(schema, encoder, 1, 5), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND_NEWER);
    EXPECT_FALSE(result->use_default_or_null);
    ASSERT_NE(result->rowset, nullptr);
    EXPECT_EQ(result->loc.row_id, 0U);
}

// A delete-signed row needs no old values -- but only when the schema has no sequence column, since
// the sequence value must survive for merge-on-read.
TEST_F(KeyProbeTest, DeleteSignTakesDefaultsOnlyWithoutSequenceColumn) {
    for (bool has_seq : {false, true}) {
        auto schema = create_mow_schema(has_seq);
        TabletSharedPtr tablet;
        auto rowset = write_rowset(schema, has_seq ? 4041 : 4042, 2, {{1, 11, 3, 0}}, &tablet);
        auto mow = make_mow_context(100, {rowset});
        RowKeyEncoder encoder {*schema, /*mow=*/true};

        std::vector<RowsetSharedPtr> rowsets {rowset};
        std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
        PartialUpdateStats stats;
        auto probe = make_fill_probe(schema, tablet, mow);

        auto result = probe.probe(encode_key(schema, encoder, 1), /*segment_pos=*/0,
                                  /*key_has_seq_suffix=*/false, /*have_delete_sign=*/true, rowsets,
                                  caches, stats);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->result, KeyProbeResult::FOUND);
        EXPECT_EQ(result->use_default_or_null, !has_seq) << "has_seq=" << has_seq;
        // either way the old row is replaced by the delete
        EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
        EXPECT_EQ(stats.num_rows_updated, 1);
    }
}

// The row binlog retriever keeps the old values of a delete-signed row so it can emit the
// __BEFORE__ image: use_defaults_for_delete_signed off flips that same case.
TEST_F(KeyProbeTest, DeleteSignReadsHistoryWhenDefaultsForDeleteSignedIsOff) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4051, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    auto policy = fill_policy();
    policy.use_defaults_for_delete_signed = false;
    auto probe = make_probe(schema, tablet, mow, policy);

    auto result = probe.probe(encode_key(schema, encoder, 1), /*segment_pos=*/0,
                              /*key_has_seq_suffix=*/false, /*have_delete_sign=*/true, rowsets,
                              caches, stats);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->result, KeyProbeResult::FOUND);
    EXPECT_FALSE(result->use_default_or_null);
    ASSERT_NE(result->rowset, nullptr);
}

// MarkDeleted::NONE is a pure lookup: no delete bitmap writes, no delete counters.
TEST_F(KeyProbeTest, MarkNoneLeavesTheDeleteBitmapAlone) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4061, 2, {{1, 11, 10, 0}, {2, 22, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    // through the factory the row binlog retriever calls, so its MarkDeleted::NONE is pinned here
    auto probe = MowKeyProbe::for_row_binlog(tablet.get(), schema.get(), schema->has_sequence_col(),
                                             mow, /*write_before=*/true);

    // a replaced row ...
    auto found = probe.probe(encode_key_with_seq(schema, encoder, 1, 20), /*segment_pos=*/0,
                             /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                             caches, stats);
    ASSERT_TRUE(found.has_value()) << found.error();
    EXPECT_EQ(found->result, KeyProbeResult::FOUND);
    // ... and a row that loses on sequence
    auto loser = probe.probe(encode_key_with_seq(schema, encoder, 2, 1), /*segment_pos=*/1,
                             /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                             caches, stats);
    ASSERT_TRUE(loser.has_value()) << loser.error();
    EXPECT_EQ(loser->result, KeyProbeResult::FOUND_NEWER);

    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
    EXPECT_EQ(stats.num_rows_updated, 0);
    EXPECT_EQ(stats.num_rows_deleted, 0);
}

// MarkDeleted::OLD_ROW marks the old row but never the row being written: the caller of this mode
// (the block aggregator) drops the losing row itself.
TEST_F(KeyProbeTest, OldRowModeNeverMarksTheIncomingRow) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4071, 2, {{1, 11, 10, 0}, {2, 22, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    PartialUpdateStats stats;
    MowKeyProbe::Policy policy {
            .mark_deleted = MowKeyProbe::MarkDeleted::OLD_ROW,
            .use_defaults_for_delete_signed = true,
            .use_defaults_for_seq_loser = true,
            .use_defaults_for_in_load_deleted = false,
    };
    auto probe = make_probe(schema, tablet, mow, policy);

    auto found = probe.probe(encode_key_with_seq(schema, encoder, 1, 20), /*segment_pos=*/0,
                             /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                             caches, stats);
    ASSERT_TRUE(found.has_value()) << found.error();
    EXPECT_EQ(found->result, KeyProbeResult::FOUND);
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_EQ(stats.num_rows_updated, 1);

    auto loser = probe.probe(encode_key_with_seq(schema, encoder, 2, 1), /*segment_pos=*/1,
                             /*key_has_seq_suffix=*/true, /*have_delete_sign=*/false, rowsets,
                             caches, stats);
    ASSERT_TRUE(loser.has_value()) << loser.error();
    EXPECT_EQ(loser->result, KeyProbeResult::FOUND_NEWER);
    EXPECT_FALSE(marked(mow, writing_rowset_id(), 0, 1));
    EXPECT_EQ(stats.num_rows_deleted, 0);
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// Flexible partial update, insert after delete in one load: the old row was already deleted earlier
// in this same load, so the insert counts as brand new and its old values must not be read back.
TEST_F(KeyProbeTest, InLoadDeletedTreatsReinsertAsNewRow) {
    for (bool use_defaults_for_in_load_deleted : {false, true}) {
        auto schema = create_mow_schema(/*has_seq=*/false);
        TabletSharedPtr tablet;
        auto rowset = write_rowset(schema, use_defaults_for_in_load_deleted ? 4081 : 4082, 2,
                                   {{1, 11}}, &tablet);
        auto mow = make_mow_context(100, {rowset});
        RowKeyEncoder encoder {*schema, /*mow=*/true};
        // an earlier row of this load already deleted the old row
        mow->delete_bitmap->add({rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0);

        std::vector<RowsetSharedPtr> rowsets {rowset};
        std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
        PartialUpdateStats stats;
        auto probe =
                make_fill_probe(schema, tablet, mow, /*flexible=*/use_defaults_for_in_load_deleted);

        auto result = probe.probe(encode_key(schema, encoder, 1), /*segment_pos=*/0,
                                  /*key_has_seq_suffix=*/false, /*have_delete_sign=*/false, rowsets,
                                  caches, stats);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->result, KeyProbeResult::FOUND);
        EXPECT_EQ(result->use_default_or_null, use_defaults_for_in_load_deleted)
                << "use_defaults_for_in_load_deleted=" << use_defaults_for_in_load_deleted;
    }
}

// The aggregator's lookup: no sequence suffix on the probe key, the old row's sequence value comes
// back encoded, and nothing is marked deleted.
TEST_F(KeyProbeTest, ProbePreviousSeqValueReturnsTheOldRowSequence) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 4091, 2, {{1, 11, 7, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    std::vector<RowsetSharedPtr> rowsets {rowset};
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    auto probe = make_fill_probe(schema, tablet, mow);

    auto hit = probe.probe_previous_seq_value(encode_key(schema, encoder, 1), rowsets, caches);
    ASSERT_TRUE(hit.has_value()) << hit.error();
    EXPECT_EQ(hit->outcome.result, KeyProbeResult::FOUND);
    EXPECT_FALSE(hit->outcome.use_default_or_null);
    ASSERT_NE(hit->outcome.rowset, nullptr);
    // the encoded suffix of the old row: marker byte + the value bytes of seq=7
    std::string expected = encode_key(schema, encoder, 1);
    std::string with_suffix = encode_key_with_seq(schema, encoder, 1, 7);
    EXPECT_EQ(hit->encoded_seq_value, with_suffix.substr(expected.size()));

    auto miss = probe.probe_previous_seq_value(encode_key(schema, encoder, 42), rowsets, caches);
    ASSERT_TRUE(miss.has_value()) << miss.error();
    EXPECT_EQ(miss->outcome.result, KeyProbeResult::NOT_FOUND);
    EXPECT_TRUE(miss->outcome.use_default_or_null);

    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
}

// Row cache invalidation. The cache is keyed by the encoded key *without* the sequence suffix, so
// encode_mow_key_invalidate_cache must invalidate before it appends the suffix -- and it must still
// return the key with the suffix.
class RowCacheProbeTest : public KeyProbeTest {
protected:
    void SetUp() override {
        KeyProbeTest::SetUp();
        _saved_cache = ExecEnv::GetInstance()->get_row_cache();
        _cache = new RowCache(1024 * 1024, 1);
        ExecEnv::GetInstance()->_row_cache = _cache;
        _saved_disable_row_cache = config::disable_storage_row_cache;
        config::disable_storage_row_cache = false;
    }

    void TearDown() override {
        config::disable_storage_row_cache = _saved_disable_row_cache;
        ExecEnv::GetInstance()->_row_cache = _saved_cache;
        delete _cache;
        KeyProbeTest::TearDown();
    }

    static bool cached(int64_t tablet_id, const std::string& key) {
        RowCache::CacheHandle handle;
        return RowCache::instance()->lookup({tablet_id, Slice {key}}, &handle);
    }

    static void cache_row(int64_t tablet_id, const std::string& key) {
        RowCache::instance()->insert({tablet_id, Slice {key}}, Slice {"row"});
    }

    // encode_mow_key_invalidate_cache the way the fill loops call it: the column accessors must
    // outlive the call, so the convertor stays alive here.
    std::string encode_and_invalidate(const TabletSchemaSPtr& schema, const RowKeyEncoder& encoder,
                                      int32_t k, bool row_has_seq, int32_t seq,
                                      DataWriteType write_type) {
        const auto seq_idx = static_cast<uint32_t>(schema->sequence_col_idx());
        Block block = row_has_seq ? schema->create_storage_block({0, seq_idx})
                                  : schema->create_storage_block({0});
        block.get_by_position(0).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&k), sizeof(int32_t));
        OlapBlockDataConvertor convertor;
        convertor.add_column_data_convertor(schema->column(0));
        if (row_has_seq) {
            block.get_by_position(1).column->assert_mutable()->insert_data(
                    reinterpret_cast<const char*>(&seq), sizeof(int32_t));
            convertor.add_column_data_convertor(schema->column(seq_idx));
        }
        convertor.set_source_content(&block, 0, 1);
        auto [key_st, key_accessor] = convertor.convert_column_data(0);
        EXPECT_TRUE(key_st.ok()) << key_st;
        IOlapColumnDataAccessor* seq_accessor = nullptr;
        if (row_has_seq) {
            auto [seq_st, accessor] = convertor.convert_column_data(1);
            EXPECT_TRUE(seq_st.ok()) << seq_st;
            seq_accessor = accessor;
        }
        std::vector<IOlapColumnDataAccessor*> key_columns {key_accessor};
        return segment_v2::encode_mow_key_invalidate_cache(
                encoder, key_columns, seq_accessor, 0, row_has_seq, kTabletId, *schema, write_type);
    }

    RowCache* _cache = nullptr;
    RowCache* _saved_cache = nullptr;
    bool _saved_disable_row_cache = false;
};

TEST_F(RowCacheProbeTest, InvalidatesTheSeqlessKeyAndReturnsTheSuffixedKey) {
    auto schema = create_row_store_schema(/*has_seq=*/true);
    RowKeyEncoder encoder {*schema, /*mow=*/true};

    const std::string seqless_key = encode_key(schema, encoder, 1);
    const std::string suffixed_key = encode_key_with_seq(schema, encoder, 1, 9);
    ASSERT_GT(suffixed_key.size(), seqless_key.size());

    cache_row(kTabletId, seqless_key);
    cache_row(kTabletId, suffixed_key);
    ASSERT_TRUE(cached(kTabletId, seqless_key));

    std::string key = encode_and_invalidate(schema, encoder, 1, /*row_has_seq=*/true, 9,
                                            DataWriteType::TYPE_DIRECT);
    EXPECT_EQ(key, suffixed_key);
    // the cache entry of the row itself is gone ...
    EXPECT_FALSE(cached(kTabletId, seqless_key));
    // ... and the suffixed key was never used as a cache key
    EXPECT_TRUE(cached(kTabletId, suffixed_key));
}

TEST_F(RowCacheProbeTest, KeepsTheCacheForNonDirectWritesAndNonRowStoreSchemas) {
    auto row_store_schema = create_row_store_schema();
    RowKeyEncoder row_store_encoder {*row_store_schema, /*mow=*/true};
    const std::string key = encode_key(row_store_schema, row_store_encoder, 1);

    // compaction output is not visible through the row cache, so it must not invalidate anything
    cache_row(kTabletId, key);
    static_cast<void>(encode_and_invalidate(row_store_schema, row_store_encoder, 1,
                                            /*row_has_seq=*/false, 0,
                                            DataWriteType::TYPE_COMPACTION));
    EXPECT_TRUE(cached(kTabletId, key));

    // a schema without the row store column never populates the cache either. Key 2, so this
    // sub-case owns a cache entry of its own: both schemas start with the same INT key column, so
    // key 1 would encode to the exact bytes the sub-case above cached.
    auto plain_schema = create_mow_schema(/*has_seq=*/false);
    RowKeyEncoder plain_encoder {*plain_schema, /*mow=*/true};
    const std::string plain_key = encode_key(plain_schema, plain_encoder, 2);
    ASSERT_NE(plain_key, key);
    cache_row(kTabletId, plain_key);
    static_cast<void>(encode_and_invalidate(plain_schema, plain_encoder, 2, /*row_has_seq=*/false,
                                            0, DataWriteType::TYPE_DIRECT));
    EXPECT_TRUE(cached(kTabletId, plain_key));

    // the direct write of a row-store schema still invalidates
    static_cast<void>(encode_and_invalidate(row_store_schema, row_store_encoder, 1,
                                            /*row_has_seq=*/false, 0, DataWriteType::TYPE_DIRECT));
    EXPECT_FALSE(cached(kTabletId, key));
}

} // namespace doris
