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

#include "storage/rowset/rowset_meta.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <new>
#include <string>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/olap_common.h"
#include "storage/olap_meta.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/storage/test_data/rowset.json";

class RowsetMetaTest : public testing::Test {
public:
    virtual void SetUp() {
        std::string meta_path = "./meta";
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
        EXPECT_TRUE(std::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
    }

    virtual void TearDown() {
        delete _meta;
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
};

void do_check(const RowsetMeta& rowset_meta) {
    RowsetId rowset_id;
    rowset_id.init(540081);
    EXPECT_EQ(rowset_id, rowset_meta.rowset_id());
    EXPECT_EQ(15673, rowset_meta.tablet_id());
    EXPECT_EQ(4042, rowset_meta.txn_id());
    EXPECT_EQ(567997577, rowset_meta.tablet_schema_hash());
    EXPECT_EQ(BETA_ROWSET, rowset_meta.rowset_type());
    EXPECT_EQ(VISIBLE, rowset_meta.rowset_state());
    EXPECT_EQ(2, rowset_meta.start_version());
    EXPECT_EQ(2, rowset_meta.end_version());
    EXPECT_EQ(3929, rowset_meta.num_rows());
    EXPECT_EQ(84699, rowset_meta.total_disk_size());
    EXPECT_EQ(84464, rowset_meta.data_disk_size());
    EXPECT_EQ(235, rowset_meta.index_disk_size());
    EXPECT_EQ(false, rowset_meta.empty());
    EXPECT_EQ(1553765670, rowset_meta.creation_time());
}

TEST_F(RowsetMetaTest, TestInit) {
    RowsetMeta rowset_meta;
    EXPECT_TRUE(rowset_meta.init_from_json(_json_rowset_meta));
    do_check(rowset_meta);
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    RowsetMeta rowset_meta_2;
    rowset_meta_2.init_from_pb(rowset_meta_pb);
    do_check(rowset_meta_2);
    std::string value = "";
    rowset_meta_pb.SerializeToString(&value);
    RowsetMeta rowset_meta_3;
    rowset_meta_3.init(value);
    do_check(rowset_meta_3);
}

TEST_F(RowsetMetaTest, TestInitWithInvalidData) {
    RowsetMeta rowset_meta;
    EXPECT_FALSE(rowset_meta.init_from_json("invalid json meta data"));
    EXPECT_FALSE(rowset_meta.init("invalid pb meta data"));
}

TEST_F(RowsetMetaTest, TestRowsetIdInit) {
    RowsetId id {};
    config::force_regenerate_rowsetid_on_start_error = true;
    std::string_view rowset_id_str = "test";
    id.init(rowset_id_str);
    // 0x100000000000000 - 0x01
    EXPECT_EQ(id.to_string(), "72057594037927935");
}

TEST_F(RowsetMetaTest, TestNumSegmentRowsSetAndGet) {
    RowsetMeta rowset_meta;
    EXPECT_TRUE(rowset_meta.init_from_json(_json_rowset_meta));

    // Test set_num_segment_rows and get_num_segment_rows
    std::vector<uint32_t> num_segment_rows = {100, 200, 300};
    rowset_meta.set_num_segment_rows(num_segment_rows);

    std::vector<uint32_t> retrieved_rows;
    rowset_meta.get_num_segment_rows(&retrieved_rows);

    EXPECT_EQ(retrieved_rows.size(), 3);
    EXPECT_EQ(retrieved_rows[0], 100);
    EXPECT_EQ(retrieved_rows[1], 200);
    EXPECT_EQ(retrieved_rows[2], 300);

    // Test get_num_segment_rows() const reference
    const auto& num_segment_rows_ref = rowset_meta.get_num_segment_rows();
    EXPECT_EQ(num_segment_rows_ref.size(), 3);
    EXPECT_EQ(num_segment_rows_ref.Get(0), 100);
    EXPECT_EQ(num_segment_rows_ref.Get(1), 200);
    EXPECT_EQ(num_segment_rows_ref.Get(2), 300);

    // Test serialization and deserialization
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    EXPECT_EQ(rowset_meta_pb.num_segment_rows_size(), 3);
    EXPECT_EQ(rowset_meta_pb.num_segment_rows(0), 100);
    EXPECT_EQ(rowset_meta_pb.num_segment_rows(1), 200);
    EXPECT_EQ(rowset_meta_pb.num_segment_rows(2), 300);

    RowsetMeta rowset_meta_2;
    rowset_meta_2.init_from_pb(rowset_meta_pb);
    std::vector<uint32_t> retrieved_rows_2;
    rowset_meta_2.get_num_segment_rows(&retrieved_rows_2);
    EXPECT_EQ(retrieved_rows_2.size(), 3);
    EXPECT_EQ(retrieved_rows_2[0], 100);
    EXPECT_EQ(retrieved_rows_2[1], 200);
    EXPECT_EQ(retrieved_rows_2[2], 300);
}

TEST_F(RowsetMetaTest, TestNumSegmentRowsEmpty) {
    RowsetMeta rowset_meta;
    EXPECT_TRUE(rowset_meta.init_from_json(_json_rowset_meta));

    // By default, num_segment_rows should be empty
    std::vector<uint32_t> retrieved_rows;
    rowset_meta.get_num_segment_rows(&retrieved_rows);
    EXPECT_EQ(retrieved_rows.size(), 0);

    const auto& num_segment_rows_ref = rowset_meta.get_num_segment_rows();
    EXPECT_EQ(num_segment_rows_ref.size(), 0);
}

TEST_F(RowsetMetaTest, TestMergeRowsetMetaWithNumSegmentRows) {
    RowsetMeta rowset_meta_1;
    EXPECT_TRUE(rowset_meta_1.init_from_json(_json_rowset_meta));
    std::vector<uint32_t> num_segment_rows_1 = {100, 200};
    rowset_meta_1.set_num_segment_rows(num_segment_rows_1);
    rowset_meta_1.set_num_segments(2);
    rowset_meta_1.set_total_disk_size(1000);
    rowset_meta_1.set_data_disk_size(800);
    rowset_meta_1.set_index_disk_size(200);

    RowsetMeta rowset_meta_2;
    EXPECT_TRUE(rowset_meta_2.init_from_json(_json_rowset_meta));
    std::vector<uint32_t> num_segment_rows_2 = {300, 400, 500};
    rowset_meta_2.set_num_segment_rows(num_segment_rows_2);
    rowset_meta_2.set_num_segments(3);
    rowset_meta_2.set_total_disk_size(2000);
    rowset_meta_2.set_data_disk_size(1600);
    rowset_meta_2.set_index_disk_size(400);

    // Use sync point to skip schema merge logic
    auto sp = SyncPoint::get_instance();
    bool skip_called = false;
    sp->set_call_back("RowsetMeta::merge_rowset_meta:skip_schema_merge", [&](auto&& args) {
        skip_called = true;
        // Set the return flag to skip the schema merge logic
        auto pred = try_any_cast<bool*>(args.back());
        *pred = true;
    });
    sp->enable_processing();

    // Merge rowset_meta_2 into rowset_meta_1
    rowset_meta_1.merge_rowset_meta(rowset_meta_2);

    EXPECT_TRUE(skip_called);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();

    // Check merged num_segment_rows
    std::vector<uint32_t> merged_rows;
    rowset_meta_1.get_num_segment_rows(&merged_rows);
    EXPECT_EQ(merged_rows.size(), 5);
    EXPECT_EQ(merged_rows[0], 100);
    EXPECT_EQ(merged_rows[1], 200);
    EXPECT_EQ(merged_rows[2], 300);
    EXPECT_EQ(merged_rows[3], 400);
    EXPECT_EQ(merged_rows[4], 500);

    // Check merged num_segments
    EXPECT_EQ(rowset_meta_1.num_segments(), 5);

    // Check merged disk sizes
    EXPECT_EQ(rowset_meta_1.total_disk_size(), 3000);
}

TEST_F(RowsetMetaTest, TestMergeRowsetMetaWithPartialNumSegmentRows) {
    RowsetMeta rowset_meta_1;
    EXPECT_TRUE(rowset_meta_1.init_from_json(_json_rowset_meta));
    std::vector<uint32_t> num_segment_rows_1 = {100, 200};
    rowset_meta_1.set_num_segment_rows(num_segment_rows_1);
    rowset_meta_1.set_num_segments(2);

    RowsetMeta rowset_meta_2;
    EXPECT_TRUE(rowset_meta_2.init_from_json(_json_rowset_meta));
    // rowset_meta_2 has no num_segment_rows (simulating old version data)
    rowset_meta_2.set_num_segments(3);

    // Use sync point to skip schema merge logic
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("RowsetMeta::merge_rowset_meta:skip_schema_merge", [&](auto&& args) {
        auto pred = try_any_cast<bool*>(args.back());
        *pred = true;
    });
    sp->enable_processing();

    // Merge rowset_meta_2 into rowset_meta_1
    rowset_meta_1.merge_rowset_meta(rowset_meta_2);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();

    // num_segment_rows should be cleared when one of them is empty
    std::vector<uint32_t> merged_rows;
    rowset_meta_1.get_num_segment_rows(&merged_rows);
    EXPECT_EQ(merged_rows.size(), 0);

    // num_segments should still be merged
    EXPECT_EQ(rowset_meta_1.num_segments(), 5);
}

TEST_F(RowsetMetaTest, TestMergeRowsetMetaBothEmpty) {
    RowsetMeta rowset_meta_1;
    EXPECT_TRUE(rowset_meta_1.init_from_json(_json_rowset_meta));
    rowset_meta_1.set_num_segments(2);

    RowsetMeta rowset_meta_2;
    EXPECT_TRUE(rowset_meta_2.init_from_json(_json_rowset_meta));
    rowset_meta_2.set_num_segments(3);

    // Use sync point to skip schema merge logic
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("RowsetMeta::merge_rowset_meta:skip_schema_merge", [&](auto&& args) {
        auto pred = try_any_cast<bool*>(args.back());
        *pred = true;
    });
    sp->enable_processing();

    // Merge rowset_meta_2 into rowset_meta_1
    rowset_meta_1.merge_rowset_meta(rowset_meta_2);

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();

    // num_segment_rows should remain empty
    std::vector<uint32_t> merged_rows;
    rowset_meta_1.get_num_segment_rows(&merged_rows);
    EXPECT_EQ(merged_rows.size(), 0);

    // num_segments should still be merged
    EXPECT_EQ(rowset_meta_1.num_segments(), 5);
}

TEST_F(RowsetMetaTest, TestSegmentsKeyBoundsAggregation) {
    auto make_bounds = [](std::string min_key, std::string max_key) {
        KeyBoundsPB kb;
        kb.set_min_key(std::move(min_key));
        kb.set_max_key(std::move(max_key));
        return kb;
    };

    // Prepare three per-segment bounds whose overall min is "a01" and overall max is "z99".
    // Intentionally unordered so that the aggregation must scan all entries.
    std::vector<KeyBoundsPB> per_segment;
    per_segment.push_back(make_bounds("m50", "z99"));
    per_segment.push_back(make_bounds("a01", "k10"));
    per_segment.push_back(make_bounds("f20", "r80"));

    // Save and restore truncation config to keep the test deterministic.
    int32_t saved_truncation = config::segments_key_bounds_truncation_threshold;
    config::segments_key_bounds_truncation_threshold = -1;
    auto restore = std::shared_ptr<void>(nullptr, [&](void*) {
        config::segments_key_bounds_truncation_threshold = saved_truncation;
    });

    // 1. aggregate=true -> single [overall_min, overall_max] entry, flag set.
    {
        RowsetMeta rs_meta;
        rs_meta.set_num_segments(per_segment.size());
        rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/true);

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        ASSERT_EQ(out.size(), 1);
        EXPECT_EQ(out[0].min_key(), "a01");
        EXPECT_EQ(out[0].max_key(), "z99");
        EXPECT_TRUE(rs_meta.is_segments_key_bounds_aggregated());

        // first_key/last_key must still return the global min/max.
        KeyBoundsPB first;
        KeyBoundsPB last;
        ASSERT_TRUE(rs_meta.get_first_segment_key_bound(&first));
        ASSERT_TRUE(rs_meta.get_last_segment_key_bound(&last));
        EXPECT_EQ(first.min_key(), "a01");
        EXPECT_EQ(last.max_key(), "z99");
    }

    // 2. aggregate=false (default) -> per-segment entries preserved, flag unset.
    {
        RowsetMeta rs_meta;
        rs_meta.set_num_segments(per_segment.size());
        rs_meta.set_segments_key_bounds(per_segment);

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        ASSERT_EQ(out.size(), per_segment.size());
        EXPECT_FALSE(rs_meta.is_segments_key_bounds_aggregated());
        for (size_t i = 0; i < per_segment.size(); ++i) {
            EXPECT_EQ(out[i].min_key(), per_segment[i].min_key());
            EXPECT_EQ(out[i].max_key(), per_segment[i].max_key());
        }
    }

    // 3. aggregate=true with empty input -> nothing written, flag untouched.
    {
        RowsetMeta rs_meta;
        rs_meta.set_segments_key_bounds({}, /*aggregate_into_single=*/true);

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        EXPECT_EQ(out.size(), 0);
        EXPECT_FALSE(rs_meta.is_segments_key_bounds_aggregated());
    }

    // 4. aggregate=true called twice -> result reflects the latest call only.
    {
        RowsetMeta rs_meta;
        rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/true);
        std::vector<KeyBoundsPB> second;
        second.push_back(make_bounds("b00", "c00"));
        rs_meta.set_segments_key_bounds(second, /*aggregate_into_single=*/true);

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        ASSERT_EQ(out.size(), 1);
        EXPECT_EQ(out[0].min_key(), "b00");
        EXPECT_EQ(out[0].max_key(), "c00");
        EXPECT_TRUE(rs_meta.is_segments_key_bounds_aggregated());
    }

    // 5. aggregated flag must be reset when switching from aggregate=true to
    //    aggregate=false on the same instance.
    {
        RowsetMeta rs_meta;
        rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/true);
        ASSERT_TRUE(rs_meta.is_segments_key_bounds_aggregated());

        rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/false);
        EXPECT_FALSE(rs_meta.is_segments_key_bounds_aggregated());

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        EXPECT_EQ(out.size(), per_segment.size());
    }

    // 6. aggregated flag must be reset when calling with aggregate=true but an
    //    empty input after a prior aggregated call.
    {
        RowsetMeta rs_meta;
        rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/true);
        ASSERT_TRUE(rs_meta.is_segments_key_bounds_aggregated());

        rs_meta.set_segments_key_bounds({}, /*aggregate_into_single=*/true);
        EXPECT_FALSE(rs_meta.is_segments_key_bounds_aggregated());

        std::vector<KeyBoundsPB> out;
        rs_meta.get_segments_key_bounds(&out);
        EXPECT_TRUE(out.empty());
    }
}

TEST_F(RowsetMetaTest, TestSegmentsKeyBoundsAggregationTruncation) {
    // Aggregated entry is still subject to truncation.
    int32_t saved_truncation = config::segments_key_bounds_truncation_threshold;
    bool saved_random = config::random_segments_key_bounds_truncation;
    config::segments_key_bounds_truncation_threshold = 4;
    config::random_segments_key_bounds_truncation = false;
    auto restore = std::shared_ptr<void>(nullptr, [&](void*) {
        config::segments_key_bounds_truncation_threshold = saved_truncation;
        config::random_segments_key_bounds_truncation = saved_random;
    });

    auto make_bounds = [](std::string min_key, std::string max_key) {
        KeyBoundsPB kb;
        kb.set_min_key(std::move(min_key));
        kb.set_max_key(std::move(max_key));
        return kb;
    };

    std::vector<KeyBoundsPB> per_segment;
    per_segment.push_back(make_bounds("aaaaaaa", "bbbbbbb"));
    per_segment.push_back(make_bounds("ccccccc", "ddddddd"));

    RowsetMeta rs_meta;
    rs_meta.set_segments_key_bounds(per_segment, /*aggregate_into_single=*/true);

    std::vector<KeyBoundsPB> out;
    rs_meta.get_segments_key_bounds(&out);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].min_key(), std::string("aaaa"));
    EXPECT_EQ(out[0].max_key(), std::string("dddd"));
    EXPECT_TRUE(rs_meta.is_segments_key_bounds_aggregated());
    EXPECT_TRUE(rs_meta.is_segments_key_bounds_truncated());
}

} // namespace doris
