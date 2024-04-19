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

#include "vec/sink/writer/vtablet_writer_v2.h"

#include <gtest/gtest.h>

#include "vec/sink/load_stream_map_pool.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {

class TestVTabletWriterV2 : public ::testing::Test {
public:
    TestVTabletWriterV2() = default;
    ~TestVTabletWriterV2() = default;
    static void SetUpTestSuite() {}
    static void TearDownTestSuite() {}
};

const int64_t src_id = 1000;

static void add_stream(std::shared_ptr<LoadStreamMap> load_stream_map, int64_t node_id,
                       std::vector<int64_t> success_tablets,
                       std::unordered_map<int64_t, Status> failed_tablets) {
    auto stub = load_stream_map->get_or_create(node_id);
    for (const auto& tablet_id : success_tablets) {
        stub->at(0)->add_success_tablet(tablet_id);
    }
    for (const auto& [tablet_id, reason] : failed_tablets) {
        stub->at(0)->add_failed_tablet(tablet_id, reason);
    }
}

TEST_F(TestVTabletWriterV2, one_replica) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 1;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 2);
}

TEST_F(TestVTabletWriterV2, one_replica_fail) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 1;
    add_stream(load_stream_map, 1001, {1}, {{2, Status::InternalError("test")}});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, two_replica) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 2;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, two_replica_fail) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 2;
    add_stream(load_stream_map, 1001, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1002, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, normal) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1, 2}, {});
    add_stream(load_stream_map, 1003, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 6);
}

TEST_F(TestVTabletWriterV2, miss_one) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {});
    add_stream(load_stream_map, 1003, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, miss_two) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {});
    add_stream(load_stream_map, 1003, {1}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_one) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, fail_one_duplicate) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    // Duplicate tablets from same node should be ignored
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, fail_two_diff_tablet_same_node) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {},
               {{1, Status::InternalError("test")}, {2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_two_diff_tablet_diff_node) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {2}, {{1, Status::InternalError("test")}});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_two_same_tablet) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1}, {{2, Status::InternalError("test")}});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    // BE should detect and abort commit if majority of replicas failed
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, fail_two_miss_one_same_tablet) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    const int num_replicas = 3;
    add_stream(load_stream_map, 1001, {1}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1}, {{2, Status::InternalError("test")}});
    auto st = vectorized::VTabletWriterV2::_create_commit_info(tablet_commit_infos, load_stream_map,
                                                               num_replicas);
    // BE should detect and abort commit if majority of replicas failed
    ASSERT_EQ(st, Status::InternalError("test"));
}

} // namespace doris
