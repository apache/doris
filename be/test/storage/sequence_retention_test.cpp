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

#include "storage/sequence_retention.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "cloud/pb_convert.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {
namespace {

TEST(SequenceRetentionTest, LegacySchemaResolvesVersionColumnWithoutPersistedIndex) {
    TabletSchemaPB stored;
    stored.set_keys_type(UNIQUE_KEYS);
    auto* version = stored.add_column();
    version->set_name(VERSION_COL);
    version->set_type("BIGINT");
    version->set_unique_id(0);
    version->set_is_key(false);
    version->set_length(8);
    version->set_aggregation("NONE");
    TabletSchema schema;
    schema.init_from_pb(stored);
    EXPECT_EQ(schema.version_col_idx(), 0);
    stored.mutable_column(0)->set_name("ordinary_value");
    schema.init_from_pb(stored);
    EXPECT_EQ(schema.version_col_idx(), -1);
    stored.mutable_column(0)->set_name(VERSION_COL);
    stored.set_version_col_idx(-1);
    schema.init_from_pb(stored);
    EXPECT_EQ(schema.version_col_idx(), -1); // An explicit persisted index remains authoritative.
}

TEST(SequenceRetentionTest, PlanVersionAndTabletGapSurviveCloudMetadataRoundTrips) {
    RowsetMetaPB rowset;
    rowset.set_rowset_id(0);
    rowset.set_requires_sequence_retention(true);
    rowset.mutable_sequence_retention_input()->set_plan_version(620);
    auto restored = cloud::cloud_rowset_meta_to_doris(cloud::doris_rowset_meta_to_cloud(rowset));
    EXPECT_TRUE(restored.requires_sequence_retention());
    EXPECT_EQ(620, restored.sequence_retention_input().plan_version());

    TabletMetaPB tablet;
    tablet.set_sequence_retention_gap(100);
    auto restored_tablet =
            cloud::cloud_tablet_meta_to_doris(cloud::doris_tablet_meta_to_cloud(tablet));
    EXPECT_EQ(100, restored_tablet.sequence_retention_gap());
    EXPECT_FALSE(cloud::doris_tablet_meta_to_cloud(TabletMetaPB {}).has_sequence_retention_gap());
}

TEST(SequenceRetentionTest, DeleteExpiryIncludesItsBoundary) {
    SequenceRetentionContext context {.gap = 100, .plan_version = 620};
    ASSERT_TRUE(context.validate().ok());
    EXPECT_EQ(520, context.expiry_version());
    EXPECT_TRUE(context.expired_delete(519).value());
    EXPECT_TRUE(context.expired_delete(520).value());
    EXPECT_FALSE(context.expired_delete(521).value());
    EXPECT_FALSE(context.expired_delete(0).has_value());
}

TEST(SequenceRetentionTest, CommitWindowIsInclusiveAndCannotMoveBackwards) {
    SequenceRetentionContext context {.gap = 100, .plan_version = 620};
    EXPECT_TRUE(context.check_commit(621).ok());
    EXPECT_TRUE(context.check_commit(720).ok());
    EXPECT_FALSE(context.check_commit(721).ok());
    EXPECT_FALSE(context.check_commit(620).ok());
    EXPECT_FALSE(context.check_commit(619).ok());
}

TEST(SequenceRetentionTest, InvalidGapAndPlanningVersionFailClosed) {
    ASSERT_FALSE((SequenceRetentionContext {.gap = -1, .plan_version = 620}).validate().ok());
    ASSERT_FALSE((SequenceRetentionContext {.gap = 100, .plan_version = 0}).validate().ok());
}

TEST(SequenceRetentionTest, LargeVersionGapsNeverOverflow) {
    const int64_t gap = std::numeric_limits<int64_t>::max();
    EXPECT_EQ(0, sequence_retention_prune_version(gap, gap));
    EXPECT_EQ(0, sequence_retention_prune_version(1, gap));
    SequenceRetentionContext context {.gap = gap, .plan_version = 1};
    EXPECT_EQ(0, context.expiry_version());
    EXPECT_TRUE(context.check_commit(gap).ok());
}

TEST(SequenceRetentionTest, CleanupNeverCrossesAnySuccessfulTransactionSnapshot) {
    for (int64_t gap = 1; gap <= 16; ++gap) {
        for (int64_t plan = 1; plan < 32; ++plan) {
            SequenceRetentionContext context {.gap = gap, .plan_version = plan};
            ASSERT_TRUE(context.validate().ok());
            for (int64_t delta = 1; delta <= gap; ++delta) {
                const int64_t commit = plan + delta;
                ASSERT_TRUE(context.check_commit(commit).ok());
                for (int64_t published = 0; published < commit; ++published) {
                    ASSERT_LE(sequence_retention_prune_version(published, gap),
                              context.expiry_version());
                }
            }
        }
    }
}

} // namespace
} // namespace doris
