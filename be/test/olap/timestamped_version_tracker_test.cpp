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

#include <cctz/time_zone.h>
#include <fmt/format.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdint.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/version_graph.h"

namespace doris {

using RowsetMetaSharedContainerPtr = std::shared_ptr<std::vector<RowsetMetaSharedPtr>>;

class TestTimestampedVersionTracker : public testing::Test {
public:
    void SetUp() override {
        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "ALPHA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670
        })";
    }
    void TearDown() override {}

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
    }

    void init_all_rs_meta(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 0, 0);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 1, 1);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 2, 5);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 6, 9);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 10, 11);
        rs_metas->push_back(ptr5);
    }

    void init_expired_row_rs_meta(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 2, 3);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 4, 5);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 6, 6);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 7, 8);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 8);
        rs_metas->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 9, 9);
        rs_metas->push_back(ptr6);

        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 10, 10);
        rs_metas->push_back(ptr7);
    }

    void init_expired_row_rs_meta_with_same_rowset(std::vector<RowsetMetaSharedPtr>* rs_metas) {
        RowsetMetaSharedPtr ptr0(new RowsetMeta());
        init_rs_meta(ptr0, 1, 1);
        rs_metas->push_back(ptr0);

        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 2, 3);
        rs_metas->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 4, 5);
        rs_metas->push_back(ptr2);

        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 6, 6);
        rs_metas->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 7, 8);
        rs_metas->push_back(ptr4);

        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 8);
        rs_metas->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 9, 9);
        rs_metas->push_back(ptr6);

        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 10, 10);
        rs_metas->push_back(ptr7);
    }

    void fetch_expired_row_rs_meta(std::vector<RowsetMetaSharedContainerPtr>* rs_metas) {
        RowsetMetaSharedContainerPtr v2(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 2, 3);
        v2->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 4, 5);
        v2->push_back(ptr2);

        RowsetMetaSharedContainerPtr v3(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 6, 6);
        v3->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 7, 8);
        v3->push_back(ptr4);

        RowsetMetaSharedContainerPtr v4(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 8);
        v4->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 9, 9);
        v4->push_back(ptr6);

        RowsetMetaSharedContainerPtr v5(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 10, 10);
        v5->push_back(ptr7);

        rs_metas->push_back(v2);
        rs_metas->push_back(v3);
        rs_metas->push_back(v4);
        rs_metas->push_back(v5);
    }

    void fetch_expired_row_rs_meta_with_same_rowset(
            std::vector<RowsetMetaSharedContainerPtr>* rs_metas) {
        RowsetMetaSharedContainerPtr v1(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr0(new RowsetMeta());
        init_rs_meta(ptr0, 1, 1);
        v1->push_back(ptr0);

        RowsetMetaSharedContainerPtr v2(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr1(new RowsetMeta());
        init_rs_meta(ptr1, 2, 3);
        v2->push_back(ptr1);

        RowsetMetaSharedPtr ptr2(new RowsetMeta());
        init_rs_meta(ptr2, 4, 5);
        v2->push_back(ptr2);

        RowsetMetaSharedContainerPtr v3(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr3(new RowsetMeta());
        init_rs_meta(ptr3, 6, 6);
        v3->push_back(ptr3);

        RowsetMetaSharedPtr ptr4(new RowsetMeta());
        init_rs_meta(ptr4, 7, 8);
        v3->push_back(ptr4);

        RowsetMetaSharedContainerPtr v4(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr5(new RowsetMeta());
        init_rs_meta(ptr5, 6, 8);
        v4->push_back(ptr5);

        RowsetMetaSharedPtr ptr6(new RowsetMeta());
        init_rs_meta(ptr6, 9, 9);
        v4->push_back(ptr6);

        RowsetMetaSharedContainerPtr v5(new std::vector<RowsetMetaSharedPtr>());
        RowsetMetaSharedPtr ptr7(new RowsetMeta());
        init_rs_meta(ptr7, 10, 10);
        v5->push_back(ptr7);

        rs_metas->push_back(v1);
        rs_metas->push_back(v2);
        rs_metas->push_back(v3);
        rs_metas->push_back(v4);
        rs_metas->push_back(v5);
    }

private:
    std::string _json_rowset_meta;
};

TEST_F(TestTimestampedVersionTracker, construct_version_graph) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    VersionGraph version_graph;

    init_all_rs_meta(&rs_metas);
    int64_t max_version = 0;
    version_graph.construct_version_graph(rs_metas, &max_version);

    EXPECT_EQ(6, version_graph._version_graph.size());
    int64_t exp = 11;
    EXPECT_EQ(exp, max_version);
}

TEST_F(TestTimestampedVersionTracker, construct_version_graph_with_same_version) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;

    VersionGraph version_graph;

    init_all_rs_meta(&rs_metas);

    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());
    int64_t max_version = 0;
    version_graph.construct_version_graph(rs_metas, &max_version);

    EXPECT_EQ(6, version_graph._version_graph.size());
    int64_t exp = 11;
    EXPECT_EQ(exp, max_version);
}

TEST_F(TestTimestampedVersionTracker, reconstruct_version_graph) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    VersionGraph version_graph;

    init_all_rs_meta(&rs_metas);
    int64_t max_version = 0;
    version_graph.reconstruct_version_graph(rs_metas, &max_version);

    EXPECT_EQ(6, version_graph._version_graph.size());
    int64_t exp = 11;
    EXPECT_EQ(exp, max_version);
}

TEST_F(TestTimestampedVersionTracker, delete_version_from_graph) {
    VersionGraph version_graph;

    Version version0(0, 0);

    version_graph.add_version_to_graph(version0);
    static_cast<void>(version_graph.delete_version_from_graph(version0));

    EXPECT_EQ(2, version_graph._version_graph.size());
    EXPECT_EQ(0, version_graph._version_graph[0].edges.size());
}

TEST_F(TestTimestampedVersionTracker, delete_version_from_graph_with_same_version) {
    VersionGraph version_graph;

    Version version0(0, 0);
    Version version1(0, 0);

    version_graph.add_version_to_graph(version0);
    version_graph.add_version_to_graph(version1);

    static_cast<void>(version_graph.delete_version_from_graph(version0));

    EXPECT_EQ(2, version_graph._version_graph.size());
    EXPECT_EQ(1, version_graph._version_graph[0].edges.size());
}

TEST_F(TestTimestampedVersionTracker, add_version_to_graph) {
    VersionGraph version_graph;

    Version version0(0, 0);
    Version version1(1, 1);

    version_graph.add_version_to_graph(version0);
    version_graph.add_version_to_graph(version1);

    EXPECT_EQ(3, version_graph._version_graph.size());
    EXPECT_EQ(0, version_graph._vertex_index_map.find(0)->second);
    EXPECT_EQ(1, version_graph._vertex_index_map.find(1)->second);
}

TEST_F(TestTimestampedVersionTracker, add_version_to_graph_with_same_version) {
    VersionGraph version_graph;

    Version version0(0, 0);
    Version version1(0, 0);

    version_graph.add_version_to_graph(version0);
    version_graph.add_version_to_graph(version1);

    EXPECT_EQ(2, version_graph._version_graph.size());
    EXPECT_EQ(2, version_graph._version_graph[0].edges.size());
}

TEST_F(TestTimestampedVersionTracker, capture_consistent_versions) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta(&expired_rs_metas);

    VersionGraph version_graph;
    int64_t max_version = 0;
    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());

    version_graph.construct_version_graph(rs_metas, &max_version);

    Version spec_version(0, 8);
    static_cast<void>(version_graph.capture_consistent_versions(spec_version, &version_path));

    EXPECT_EQ(4, version_path.size());
    EXPECT_EQ(Version(0, 0), version_path[0]);
    EXPECT_EQ(Version(1, 1), version_path[1]);
    EXPECT_EQ(Version(2, 5), version_path[2]);
    EXPECT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestTimestampedVersionTracker, capture_consistent_versions_with_same_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta_with_same_rowset(&expired_rs_metas);

    VersionGraph version_graph;
    int64_t max_version = 0;
    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());

    version_graph.construct_version_graph(rs_metas, &max_version);

    Version spec_version(0, 8);
    static_cast<void>(version_graph.capture_consistent_versions(spec_version, &version_path));

    EXPECT_EQ(4, version_path.size());
    EXPECT_EQ(Version(0, 0), version_path[0]);
    EXPECT_EQ(Version(1, 1), version_path[1]);
    EXPECT_EQ(Version(2, 5), version_path[2]);
    EXPECT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestTimestampedVersionTracker, construct_versioned_tracker) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta(&expired_rs_metas);

    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());
    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);

    EXPECT_EQ(10, tracker._version_graph._version_graph.size());
    EXPECT_EQ(0, tracker._stale_version_path_map.size());
    EXPECT_EQ(1, tracker._next_path_id);
}

TEST_F(TestTimestampedVersionTracker, construct_version_tracker_by_stale_meta) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expired_rs_metas);

    EXPECT_EQ(10, tracker._version_graph._version_graph.size());
    EXPECT_EQ(4, tracker._stale_version_path_map.size());
    EXPECT_EQ(5, tracker._next_path_id);
}

TEST_F(TestTimestampedVersionTracker, construct_versioned_tracker_with_same_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta_with_same_rowset(&expired_rs_metas);

    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());
    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);

    EXPECT_EQ(10, tracker._version_graph._version_graph.size());
    EXPECT_EQ(0, tracker._stale_version_path_map.size());
    EXPECT_EQ(1, tracker._next_path_id);
}

TEST_F(TestTimestampedVersionTracker, recover_versioned_tracker) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expired_row_rs_meta(&expired_rs_metas);
    rs_metas.insert(rs_metas.end(), expired_rs_metas.begin(), expired_rs_metas.end());

    const std::map<int64_t, PathVersionListSharedPtr> stale_version_path_map;
    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    tracker.recover_versioned_tracker(stale_version_path_map);

    EXPECT_EQ(10, tracker._version_graph._version_graph.size());
    EXPECT_EQ(0, tracker._stale_version_path_map.size());
    EXPECT_EQ(1, tracker._next_path_id);
}

TEST_F(TestTimestampedVersionTracker, add_version) {
    TimestampedVersionTracker tracker;

    Version version0(0, 0);
    Version version1(1, 1);

    tracker.add_version(version0);
    tracker.add_version(version1);

    EXPECT_EQ(3, tracker._version_graph._version_graph.size());
    EXPECT_EQ(0, tracker._version_graph._vertex_index_map.find(0)->second);
    EXPECT_EQ(1, tracker._version_graph._vertex_index_map.find(1)->second);
}

TEST_F(TestTimestampedVersionTracker, add_version_with_same_rowset) {
    TimestampedVersionTracker tracker;

    Version version0(0, 0);
    Version version1(0, 0);

    tracker.add_version(version0);
    tracker.add_version(version1);

    EXPECT_EQ(2, tracker._version_graph._version_graph.size());
    EXPECT_EQ(2, tracker._version_graph._version_graph[0].edges.size());
}

TEST_F(TestTimestampedVersionTracker, add_stale_path_version) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);

    init_expired_row_rs_meta(&expired_rs_metas);
    tracker.add_stale_path_version(expired_rs_metas);

    EXPECT_EQ(1, tracker._stale_version_path_map.size());
    EXPECT_EQ(7, tracker._stale_version_path_map.begin()->second->timestamped_versions().size());
}

TEST_F(TestTimestampedVersionTracker, add_stale_path_version_with_same_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);

    fetch_expired_row_rs_meta_with_same_rowset(&expired_rs_metas);
    for (auto ptr : expired_rs_metas) {
        tracker.add_stale_path_version(*ptr);
    }

    EXPECT_EQ(5, tracker._stale_version_path_map.size());
    EXPECT_EQ(1, tracker._stale_version_path_map.begin()->second->timestamped_versions().size());
}

TEST_F(TestTimestampedVersionTracker, capture_consistent_versions_tracker) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }

    Version spec_version(0, 8);
    static_cast<void>(tracker.capture_consistent_versions(spec_version, &version_path));

    EXPECT_EQ(4, version_path.size());
    EXPECT_EQ(Version(0, 0), version_path[0]);
    EXPECT_EQ(Version(1, 1), version_path[1]);
    EXPECT_EQ(Version(2, 5), version_path[2]);
    EXPECT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestTimestampedVersionTracker, capture_consistent_versions_tracker_with_same_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta_with_same_rowset(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }

    Version spec_version(0, 8);
    static_cast<void>(tracker.capture_consistent_versions(spec_version, &version_path));

    EXPECT_EQ(4, version_path.size());
    EXPECT_EQ(Version(0, 0), version_path[0]);
    EXPECT_EQ(Version(1, 1), version_path[1]);
    EXPECT_EQ(Version(2, 5), version_path[2]);
    EXPECT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestTimestampedVersionTracker, fetch_and_delete_path_version) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }

    EXPECT_EQ(4, tracker._stale_version_path_map.size());

    Version spec_version(0, 8);
    PathVersionListSharedPtr ptr = tracker.fetch_and_delete_path_by_id(1);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();

    EXPECT_EQ(2, timestamped_versions.size());
    EXPECT_EQ(Version(2, 3), timestamped_versions[0]->version());
    EXPECT_EQ(Version(4, 5), timestamped_versions[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(2);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions2 = ptr->timestamped_versions();
    EXPECT_EQ(2, timestamped_versions2.size());
    EXPECT_EQ(Version(6, 6), timestamped_versions2[0]->version());
    EXPECT_EQ(Version(7, 8), timestamped_versions2[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(3);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions3 = ptr->timestamped_versions();
    EXPECT_EQ(2, timestamped_versions3.size());
    EXPECT_EQ(Version(6, 8), timestamped_versions3[0]->version());
    EXPECT_EQ(Version(9, 9), timestamped_versions3[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(4);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions4 = ptr->timestamped_versions();
    EXPECT_EQ(1, timestamped_versions4.size());
    EXPECT_EQ(Version(10, 10), timestamped_versions4[0]->version());

    EXPECT_EQ(0, tracker._stale_version_path_map.size());
}

TEST_F(TestTimestampedVersionTracker, fetch_and_delete_path_version_with_same_rowset) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta_with_same_rowset(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }

    EXPECT_EQ(5, tracker._stale_version_path_map.size());

    PathVersionListSharedPtr ptr = tracker.fetch_and_delete_path_by_id(1);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions = ptr->timestamped_versions();
    EXPECT_EQ(1, timestamped_versions.size());
    EXPECT_EQ(Version(1, 1), timestamped_versions[0]->version());

    ptr = tracker.fetch_and_delete_path_by_id(2);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions2 = ptr->timestamped_versions();
    EXPECT_EQ(2, timestamped_versions2.size());
    EXPECT_EQ(Version(2, 3), timestamped_versions2[0]->version());
    EXPECT_EQ(Version(4, 5), timestamped_versions2[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(3);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions3 = ptr->timestamped_versions();
    EXPECT_EQ(2, timestamped_versions3.size());
    EXPECT_EQ(Version(6, 6), timestamped_versions3[0]->version());
    EXPECT_EQ(Version(7, 8), timestamped_versions3[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(4);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions4 = ptr->timestamped_versions();
    EXPECT_EQ(2, timestamped_versions4.size());
    EXPECT_EQ(Version(6, 8), timestamped_versions4[0]->version());
    EXPECT_EQ(Version(9, 9), timestamped_versions4[1]->version());

    ptr = tracker.fetch_and_delete_path_by_id(5);
    std::vector<TimestampedVersionSharedPtr>& timestamped_versions5 = ptr->timestamped_versions();
    EXPECT_EQ(1, timestamped_versions5.size());
    EXPECT_EQ(Version(10, 10), timestamped_versions5[0]->version());

    EXPECT_EQ(0, tracker._stale_version_path_map.size());
}

TEST_F(TestTimestampedVersionTracker, capture_expired_path_version) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<int64_t> path_version;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }

    tracker.capture_expired_paths(9999, &path_version);
    EXPECT_EQ(0, path_version.size());

    tracker.capture_expired_paths(10001, &path_version);
    EXPECT_EQ(4, path_version.size());
}

TEST_F(TestTimestampedVersionTracker, get_stale_version_path_json_doc) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);
    for (auto ptr : expired_rs_metas) {
        for (auto rs : *ptr) {
            tracker.add_version(rs->version());
        }
        tracker.add_stale_path_version(*ptr);
    }
    rapidjson::Document path_arr;
    path_arr.SetArray();

    tracker.get_stale_version_path_json_doc(path_arr);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    path_arr.Accept(writer);
    std::string json_result = std::string(strbuf.GetString());

    std::string datetime_format = "%Y-%m-%d %H:%M:%S";
    cctz::time_zone time_zone;
    cctz::load_time_zone("Asia/Shanghai", &time_zone);
    std::chrono::system_clock::time_point tp;
    cctz::parse(datetime_format, "1970-01-01 10:46:40", time_zone, &tp);
    auto time_zone_str =
            cctz::format(fmt::format("{} %z", datetime_format), tp, cctz::local_time_zone());

    std::string expect_result = R"([
    {
        "path id": "1",
        "last create time": "$0",
        "path list": "1 -> [2-3] -> [4-5]"
    },
    {
        "path id": "2",
        "last create time": "$0",
        "path list": "2 -> [6-6] -> [7-8]"
    },
    {
        "path id": "3",
        "last create time": "$0",
        "path list": "3 -> [6-8] -> [9-9]"
    },
    {
        "path id": "4",
        "last create time": "$0",
        "path list": "4 -> [10-10]"
    }
])";

    expect_result = strings::Substitute(expect_result, time_zone_str);
    EXPECT_EQ(expect_result, json_result);
}

TEST_F(TestTimestampedVersionTracker, get_stale_version_path_json_doc_empty) {
    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedContainerPtr> expired_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    fetch_expired_row_rs_meta(&expired_rs_metas);

    TimestampedVersionTracker tracker;
    tracker.construct_versioned_tracker(rs_metas);

    rapidjson::Document path_arr;
    path_arr.SetArray();

    tracker.get_stale_version_path_json_doc(path_arr);

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    path_arr.Accept(writer);
    std::string json_result = std::string(strbuf.GetString());

    std::string expect_result = R"([])";

    EXPECT_EQ(expect_result, json_result);
}

TEST_F(TestTimestampedVersionTracker, get_version_graph_orphan_vertex_ratio) {
    VersionGraph version_graph;

    Version version0(0, 5);
    Version version1(6, 8);
    Version version2(9, 10);
    Version version3(11, 12);

    version_graph.add_version_to_graph(version0);
    version_graph.add_version_to_graph(version1);
    version_graph.add_version_to_graph(version2);
    version_graph.add_version_to_graph(version3);
    static_cast<void>(version_graph.delete_version_from_graph(version2));
    static_cast<void>(version_graph.delete_version_from_graph(version3));

    EXPECT_EQ(5, version_graph._version_graph.size());
    EXPECT_EQ(0.4, version_graph.get_orphan_vertex_ratio());
}

} // namespace doris
