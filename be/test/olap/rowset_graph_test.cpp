#include <gtest/gtest.h>
#include <sstream>
#include <fstream>
#include "boost/filesystem.hpp"
#include "json2pb/json_to_pb.h"

#include "olap/rowset_graph.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

class TestVersionedRowsetTracker : public testing::Test {
public:
    TestVersionedRowsetTracker() {}
    void SetUp() {
        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "ALPHA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "version_hash": 8391828013814912580,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "alpha_rowset_extra_meta_pb": {
                "segment_groups": [
                {
                    "segment_group_id": 0,
                    "num_segments": 1,
                    "index_size": 132,
                    "data_size": 576,
                    "num_rows": 5,
                    "zone_maps": [
                    {
                        "min": "MQ==",
                        "max": "NQ==",
                        "null_flag": false
                    },
                    {
                        "min": "MQ==",
                        "max": "Mw==",
                        "null_flag": false
                    },
                    {
                        "min": "J2J1c2gn",
                        "max": "J3RvbSc=",
                        "null_flag": false
                    }
                    ],
                    "empty": false
                },
                {
                    "segment_group_id": 1,
                    "num_segments": 1,
                    "index_size": 132,
                    "data_size": 576,
                    "num_rows": 5,
                    "zone_maps": [
                    {
                        "min": "MQ==",
                        "max": "NQ==",
                        "null_flag": false
                    },
                    {
                        "min": "MQ==",
                        "max": "Mw==",
                        "null_flag": false
                    },
                    {
                        "min": "J2J1c2gn",
                        "max": "J3RvbSc=",
                        "null_flag": false
                    }
                    ],
                    "empty": false
                }
                ]
            }
        })";
    }
    void TearDown() {
    }

    void init_rs_meta(RowsetMetaSharedPtr &pb1, int64_t start, int64_t end) {

        pb1->init_from_json(_json_rowset_meta);
        pb1->set_start_version(start);
        pb1->set_end_version(end);
        pb1->set_creation_time(10000);
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

    void init_expried_row_rs_meta(std::vector<RowsetMetaSharedPtr>* rs_metas) {

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

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;

};

TEST_F(TestVersionedRowsetTracker, construct_rowset_graph) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    RowsetGraph rowset_graph;

    init_all_rs_meta(&rs_metas);
    int64_t max_version = 0;
    rowset_graph.construct_rowset_graph(rs_metas, max_version);

    ASSERT_EQ(6, rowset_graph._version_graph.size());
    int64_t exp = 11;
    ASSERT_EQ(exp, max_version);
}

TEST_F(TestVersionedRowsetTracker, reconstruct_rowset_graph) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    RowsetGraph rowset_graph;

    init_all_rs_meta(&rs_metas);
    int64_t max_version = 0;
    rowset_graph.reconstruct_rowset_graph(rs_metas, max_version);

    ASSERT_EQ(6, rowset_graph._version_graph.size());
    int64_t exp = 11;
    ASSERT_EQ(exp, max_version);
}

TEST_F(TestVersionedRowsetTracker, delete_version_from_graph) {

    RowsetGraph rowset_graph;

    Version version0(0, 0);

    rowset_graph.add_version_to_graph(version0);
    rowset_graph.delete_version_from_graph(version0);

    ASSERT_EQ(2, rowset_graph._version_graph.size());
    ASSERT_EQ(0, rowset_graph._version_graph[0].edges.size());
}

TEST_F(TestVersionedRowsetTracker, add_version_to_graph) {

    RowsetGraph rowset_graph;

    Version version0(0, 0);
    Version version1(1, 1);

    rowset_graph.add_version_to_graph(version0);
    rowset_graph.add_version_to_graph(version1);

    ASSERT_EQ(3, rowset_graph._version_graph.size());
    ASSERT_EQ(0, rowset_graph._vertex_index_map.find(0)->second);
    ASSERT_EQ(1, rowset_graph._vertex_index_map.find(1)->second);
}

TEST_F(TestVersionedRowsetTracker, capture_consistent_versions) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    RowsetGraph rowset_graph;
    int64_t max_version = 0;
    rs_metas.insert(rs_metas.end(), expried_rs_metas.begin(),
                        expried_rs_metas.end());

    rowset_graph.construct_rowset_graph(rs_metas, max_version);

    Version spec_version(0, 8);
    rowset_graph.capture_consistent_versions(spec_version, &version_path);

    ASSERT_EQ(4, version_path.size());
    ASSERT_EQ(Version(0, 0), version_path[0]);
    ASSERT_EQ(Version(1, 1), version_path[1]);
    ASSERT_EQ(Version(2, 5), version_path[2]);
    ASSERT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestVersionedRowsetTracker, construct_versioned_tracker) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    VersionedRowsetTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expried_rs_metas);

    ASSERT_EQ(10, tracker._rowsetGraph._version_graph.size());
    ASSERT_EQ(4, tracker._expired_snapshot_rs_path_map.size());
    ASSERT_EQ(5, tracker.next_path_version);
}

TEST_F(TestVersionedRowsetTracker, reconstruct_versioned_tracker) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    VersionedRowsetTracker tracker;
    tracker.reconstruct_versioned_tracker(rs_metas, expried_rs_metas);

    ASSERT_EQ(10, tracker._rowsetGraph._version_graph.size());
    ASSERT_EQ(4, tracker._expired_snapshot_rs_path_map.size());
    ASSERT_EQ(5, tracker.next_path_version);
}

TEST_F(TestVersionedRowsetTracker, add_version) {

    VersionedRowsetTracker tracker;

    Version version0(0, 0);
    Version version1(1, 1);

    tracker.add_version(version0);
    tracker.add_version(version1);

    ASSERT_EQ(3, tracker._rowsetGraph._version_graph.size());
    ASSERT_EQ(0, tracker._rowsetGraph._vertex_index_map.find(0)->second);
    ASSERT_EQ(1, tracker._rowsetGraph._vertex_index_map.find(1)->second);
}

TEST_F(TestVersionedRowsetTracker, add_expired_path_version) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    VersionedRowsetTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expried_rs_metas);

    init_expried_row_rs_meta(&expried_rs_metas);
    tracker.add_expired_path_version(expried_rs_metas);

    ASSERT_EQ(1, tracker._expired_snapshot_rs_path_map.size());
    ASSERT_EQ(7, tracker._expired_snapshot_rs_path_map.begin()->second->size());
}

TEST_F(TestVersionedRowsetTracker, capture_consistent_versions_tracker) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    VersionedRowsetTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expried_rs_metas);

    Version spec_version(0, 8);
    tracker.capture_consistent_versions(spec_version, &version_path);

    ASSERT_EQ(4, version_path.size());
    ASSERT_EQ(Version(0, 0), version_path[0]);
    ASSERT_EQ(Version(1, 1), version_path[1]);
    ASSERT_EQ(Version(2, 5), version_path[2]);
    ASSERT_EQ(Version(6, 8), version_path[3]);
}

TEST_F(TestVersionedRowsetTracker, fetch_and_delete_path_version) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<Version> version_path;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    VersionedRowsetTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expried_rs_metas);

    ASSERT_EQ(4, tracker._expired_snapshot_rs_path_map.size());

    Version spec_version(0, 8);
    tracker.fetch_and_delete_path_version(1, version_path);
    ASSERT_EQ(2, version_path.size());
    ASSERT_EQ(Version(2, 3), version_path[0]);
    ASSERT_EQ(Version(4, 5), version_path[1]);

    version_path.clear();
    tracker.fetch_and_delete_path_version(2, version_path);
    ASSERT_EQ(2, version_path.size());
    ASSERT_EQ(Version(6, 6), version_path[0]);
    ASSERT_EQ(Version(7, 8), version_path[1]);

    version_path.clear();
    tracker.fetch_and_delete_path_version(3, version_path);
    ASSERT_EQ(2, version_path.size());
    ASSERT_EQ(Version(6, 8), version_path[0]);
    ASSERT_EQ(Version(9, 9), version_path[1]);

    version_path.clear();
    tracker.fetch_and_delete_path_version(4, version_path);
    ASSERT_EQ(1, version_path.size());
    ASSERT_EQ(Version(10, 10), version_path[0]);

    ASSERT_EQ(0, tracker._expired_snapshot_rs_path_map.size());
}

TEST_F(TestVersionedRowsetTracker, capture_expired_path_version) {

    std::vector<RowsetMetaSharedPtr> rs_metas;
    std::vector<RowsetMetaSharedPtr> expried_rs_metas;
    std::vector<int64_t> path_version;

    init_all_rs_meta(&rs_metas);
    init_expried_row_rs_meta(&expried_rs_metas);

    VersionedRowsetTracker tracker;
    tracker.construct_versioned_tracker(rs_metas, expried_rs_metas);

    tracker.capture_expired_path_version(9999, &path_version);
    ASSERT_EQ(0, path_version.size());

    tracker.capture_expired_path_version(10001, &path_version);
    ASSERT_EQ(4, path_version.size());
}

}

// @brief Test Stub
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS(); 
}