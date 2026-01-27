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

#include "olap/rowset/rowset_meta.h"

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
#include "olap/olap_common.h"
#include "olap/olap_meta.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset.json";

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

} // namespace doris
