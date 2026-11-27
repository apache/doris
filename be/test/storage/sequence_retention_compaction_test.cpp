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

#include <atomic>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/compaction/base_compaction.h"
#include "storage/merger.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/row_ttl.h"
#include "storage/rowid_conversion.h"
#include "storage/sequence_retention.h"

namespace doris {
namespace {

Block delete_block(std::initializer_list<int8_t> signs, std::initializer_list<int64_t> versions) {
    auto deletes = ColumnInt8::create();
    for (auto sign : signs) {
        deletes->insert_value(sign);
    }
    auto originals = ColumnInt64::create();
    for (auto version : versions) {
        originals->insert_value(version);
    }
    return Block {{std::move(deletes), std::make_shared<DataTypeInt8>(), DELETE_SIGN},
                  {std::move(originals), std::make_shared<DataTypeInt64>(), VERSION_COL}};
}

TEST(SequenceRetentionFilterTest, InclusiveCutoffAndComposedMask) {
    auto block = delete_block({1, 1, 1, 0}, {519, 520, 521, 0});
    RowVisibilityFilter filter;
    filter.selection.resize_fill(4, 1);
    filter.selection[0] = 0;
    filter.rows_deleted = 1;
    ASSERT_TRUE(apply_sequence_retention_delete_filter(block, 520, &filter).ok());
    EXPECT_EQ(filter.selection[0], 0);
    EXPECT_EQ(filter.selection[1], 0);
    EXPECT_EQ(filter.selection[2], 1);
    EXPECT_EQ(filter.selection[3], 1);
    EXPECT_EQ(filter.rows_deleted, 2);
}

TEST(SequenceRetentionFilterTest, CumulativeRetainsDeletesAndUnknownPublishedVersionFails) {
    auto block = delete_block({1}, {0});
    RowVisibilityFilter filter;
    filter.selection.resize_fill(1, 1);
    ASSERT_TRUE(apply_sequence_retention_delete_filter(block, -1, &filter).ok());
    EXPECT_EQ(filter.selection[0], 1);
    EXPECT_FALSE(apply_sequence_retention_delete_filter(block, 520, &filter).ok());
    block.erase(block.get_position_by_name(VERSION_COL));
    EXPECT_FALSE(apply_sequence_retention_delete_filter(block, 520, &filter).ok());
}

class SequenceRetentionCompactionTest : public MowTransformTestBase,
                                        public ::testing::WithParamInterface<bool> {
protected:
    static int64_t next_id() {
        // Segment/Page caches outlive each StorageEngine fixture; never overwrite the same
        // (rowset, segment) identity with a horizontal/vertical output from another test.
        static std::atomic<int64_t> counter {1000000000};
        return counter.fetch_add(1);
    }

    TabletSchemaSPtr schema(bool clustered = false) {
        TabletSchemaPB pb;
        create_mow_schema(true)->to_schema_pb(&pb);
        pb.set_delete_sign_idx(3);
        auto* version = pb.add_column();
        version->set_unique_id(4);
        version->set_name(VERSION_COL);
        version->set_type("BIGINT");
        version->set_is_key(false);
        version->set_is_nullable(false);
        version->set_length(8);
        version->set_aggregation("NONE");
        version->set_default_value("0");
        pb.set_version_col_idx(4);
        if (clustered) {
            pb.add_cluster_key_uids(1);
        }
        auto result = std::make_shared<TabletSchema>();
        result->init_from_pb(pb);
        return result;
    }

    Status merge(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                 const std::vector<RowsetSharedPtr>& inputs, ReaderType type, int64_t cutoff,
                 RowsetSharedPtr* output, RowIdConversion* rowid_conversion = nullptr) {
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& input : inputs) {
            RowsetReaderSharedPtr reader;
            RETURN_IF_ERROR(input->create_reader(&reader));
            readers.push_back(std::move(reader));
        }
        RowsetWriterContext context;
        TabletSharedPtr unused;
        make_rowset_ctx(schema, next_id(), inputs.back()->end_version(), &context, &unused);
        context.version = {inputs.front()->start_version(), inputs.back()->end_version()};
        context.tablet = tablet;
        context.write_type = DataWriteType::TYPE_COMPACTION;
        context.sequence_retention_enabled = true;
        context.sequence_delete_prune_version = cutoff;
        auto result = RowsetFactory::create_rowset_writer(*_engine, context, GetParam());
        if (!result.has_value()) {
            return result.error();
        }
        auto writer = std::move(result).value();
        Merger::Statistics statistics;
        statistics.rowid_conversion = rowid_conversion;
        if (GetParam()) {
            RETURN_IF_ERROR(Merger::vertical_merge_rowsets(tablet, type, *schema, readers,
                                                           writer.get(), 1024, inputs.size(),
                                                           &statistics));
        } else {
            RETURN_IF_ERROR(Merger::vmerge_rowsets(tablet, type, *schema, readers, writer.get(),
                                                   &statistics));
        }
        return writer->build(*output);
    }
};

TEST_P(SequenceRetentionCompactionTest, BitmapPrecedesSequenceDeduplication) {
    auto s = schema();
    TabletSharedPtr tablet;
    auto old = write_rowset(s, next_id(), 500, {{1, 11, 100, 1}}, &tablet);
    TabletSharedPtr unused;
    auto live = write_rowset(s, next_id(), 621, {{1, 80, 80, 0}}, &unused);
    tablet->tablet_meta()->delete_bitmap().add({old->rowset_id(), 0, 621}, 0);
    RowsetSharedPtr output;
    ASSERT_TRUE(
            merge(s, tablet, {old, live}, ReaderType::READER_BASE_COMPACTION, 421, &output).ok());
    Block rows;
    auto read_status = read_rowset(output, s, &rows);
    ASSERT_TRUE(read_status.ok()) << read_status;
    ASSERT_EQ(rows.rows(), 1);
    EXPECT_EQ(read_int(rows, 1, 0), 80);
    EXPECT_EQ(read_int(rows, 2, 0), 80);
    EXPECT_EQ(read_tinyint(rows, 3, 0), 0);
}

TEST_P(SequenceRetentionCompactionTest, BasePrunesByOriginalVersionAndCumulativeRetains) {
    auto s = schema();
    TabletSharedPtr tablet;
    auto old = write_rowset(s, next_id(), 500, {{1, 11, 100, 1}}, &tablet);
    TabletSharedPtr unused;
    auto young = write_rowset(s, next_id(), 620, {{2, 22, 100, 1}}, &unused);
    RowsetSharedPtr output;
    RowIdConversion conversion;
    conversion.enable_visibility_filter_tracking();
    ASSERT_TRUE(merge(s, tablet, {old, young}, ReaderType::READER_BASE_COMPACTION, 521, &output,
                      &conversion)
                        .ok());
    EXPECT_TRUE(conversion.is_visibility_filtered({old->rowset_id(), 0, 0}));
    EXPECT_FALSE(conversion.is_visibility_filtered({young->rowset_id(), 0, 0}));
    RowIdConversion::DestinationRowId destination;
    EXPECT_NE(conversion.get({old->rowset_id(), 0, 0}, &destination), 0);
    EXPECT_EQ(conversion.get({young->rowset_id(), 0, 0}, &destination), 0);
    Block rows;
    auto read_status = read_rowset(output, s, &rows);
    ASSERT_TRUE(read_status.ok()) << read_status;
    ASSERT_EQ(rows.rows(), 1);
    EXPECT_EQ(read_int(rows, 0, 0), 2);
    EXPECT_EQ(read_int(rows, 1, 0), 22);
    EXPECT_EQ(read_tinyint(rows, 3, 0), 1);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*rows.get_by_position(4).column).get_data()[0], 620);
}

TEST_P(SequenceRetentionCompactionTest, CumulativePreservesSignsAndMaterializesSourceVersions) {
    auto s = schema();
    TabletSharedPtr tablet;
    auto old = write_rowset(s, next_id(), 500, {{1, 11, 100, 1}}, &tablet);
    TabletSharedPtr unused;
    auto young = write_rowset(s, next_id(), 620, {{2, 22, 100, 1}}, &unused);
    RowsetSharedPtr output;
    ASSERT_TRUE(
            merge(s, tablet, {old, young}, ReaderType::READER_CUMULATIVE_COMPACTION, -1, &output)
                    .ok());
    Block rows;
    auto read_status = read_rowset(output, s, &rows);
    ASSERT_TRUE(read_status.ok()) << read_status;
    ASSERT_EQ(rows.rows(), 2);
    const auto& versions = assert_cast<const ColumnInt64&>(*rows.get_by_position(4).column);
    EXPECT_EQ(versions.get_data()[0], 500);
    EXPECT_EQ(versions.get_data()[1], 620);
    EXPECT_EQ(read_int(rows, 1, 0), 11);
    EXPECT_EQ(read_int(rows, 1, 1), 22);
    EXPECT_EQ(read_tinyint(rows, 3, 0), 1);
    EXPECT_EQ(read_tinyint(rows, 3, 1), 1);
}

TEST_P(SequenceRetentionCompactionTest, VersionIsInDecisionGroupWithoutBecomingSortKey) {
    for (bool clustered : {false, true}) {
        std::vector<std::vector<uint32_t>> groups;
        std::vector<uint32_t> cluster_positions;
        auto s = schema(clustered);
        Merger::vertical_split_columns(*s, &groups, &cluster_positions, 1, true);
        if (clustered) {
            EXPECT_EQ(groups.front(), (std::vector<uint32_t> {0, 2, 4, 3, 1}));
            EXPECT_EQ(cluster_positions, (std::vector<uint32_t> {4}));
        } else {
            EXPECT_EQ(groups.front(), (std::vector<uint32_t> {0, 2, 4, 3}));
            ASSERT_EQ(groups.size(), 2);
            EXPECT_EQ(groups.back(), (std::vector<uint32_t> {1}));
        }
    }
}

TEST_P(SequenceRetentionCompactionTest, TaskFreezesCleanupLineBeforeReading) {
    auto s = schema();
    TabletSharedPtr tablet;
    auto prefix = write_rowset(s, next_id(), 620, {}, &tablet);
    prefix->set_version({0, 620});
    ASSERT_TRUE(tablet->add_rowset(prefix).ok());
    {
        std::unique_lock lock(tablet->get_header_lock());
        ASSERT_TRUE(tablet->tablet_meta()->init_sequence_retention_gap(100).ok());
    }
    BaseCompaction task(*_engine, tablet);
    task._input_rowsets = {prefix};
    ASSERT_TRUE(task.snapshot_sequence_retention().ok());
    EXPECT_TRUE(task._sequence_retention_enabled);
    EXPECT_EQ(task._sequence_delete_prune_version, 420);
    TabletSharedPtr unused;
    auto next = write_rowset(s, next_id(), 621, {}, &unused);
    ASSERT_TRUE(tablet->add_rowset(next).ok());
    ASSERT_TRUE(task.snapshot_sequence_retention().ok());
    EXPECT_EQ(task._sequence_delete_prune_version, 420);
}

TEST_P(SequenceRetentionCompactionTest, TaskCannotAdvanceCleanupAcrossVersionHole) {
    auto s = schema();
    TabletSharedPtr tablet;
    auto prefix = write_rowset(s, next_id(), 618, {}, &tablet);
    prefix->set_version({0, 618});
    ASSERT_TRUE(tablet->add_rowset(prefix).ok());
    TabletSharedPtr unused;
    auto received = write_rowset(s, next_id(), 620, {}, &unused);
    ASSERT_TRUE(tablet->add_rowset(received).ok());
    {
        std::unique_lock lock(tablet->get_header_lock());
        ASSERT_TRUE(tablet->tablet_meta()->init_sequence_retention_gap(100).ok());
    }
    BaseCompaction task(*_engine, tablet);
    task._input_rowsets = {prefix, received};
    EXPECT_FALSE(task.snapshot_sequence_retention().ok());
    EXPECT_FALSE(task._sequence_retention_snapshot_initialized);
}

INSTANTIATE_TEST_SUITE_P(HorizontalAndVertical, SequenceRetentionCompactionTest, ::testing::Bool());

} // namespace
} // namespace doris
