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

#include <chrono>
#include <optional>
#include <unordered_set>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "gen_cpp/AgentService_types.h"

namespace doris {

class CloudWarmUpManagerFilterTest : public testing::Test {
public:
    CloudWarmUpManagerFilterTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

protected:
    CloudStorageEngine _engine;
};

static TReplicaInfo make_replica(int64_t backend_id) {
    TReplicaInfo replica;
    replica.__set_backend_id(backend_id);
    replica.__set_host("127.0.0.1");
    replica.__set_brpc_port(8000 + backend_id);
    replica.__set_is_alive(true);
    return replica;
}

TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterNullopt) {
    EventDrivenJobFilter filter = std::nullopt;
    EXPECT_FALSE(filter.has_value());
}

TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterWithTableIds) {
    EventDrivenJobFilter filter = std::unordered_set<int64_t> {100, 200, 300};
    EXPECT_TRUE(filter.has_value());
    EXPECT_EQ(3, filter->size());
    EXPECT_TRUE(filter->count(100) > 0);
    EXPECT_TRUE(filter->count(200) > 0);
    EXPECT_TRUE(filter->count(300) > 0);
    EXPECT_TRUE(filter->count(999) == 0);
}

TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterEmpty) {
    EventDrivenJobFilter filter = std::unordered_set<int64_t> {};
    EXPECT_TRUE(filter.has_value());
    EXPECT_EQ(0, filter->size());
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventWithoutTableIdsStoresClusterLevelFilter) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1001;

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(manager._tablet_replica_cache.contains(job_id));
    auto filter_it = manager._event_driven_filters.find(job_id);
    ASSERT_NE(filter_it, manager._event_driven_filters.end());
    EXPECT_FALSE(filter_it->second.has_value());
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventWithTableIdsStoresFilter) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1002;
    std::vector<int64_t> table_ids = {10, 20, 30};

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(manager._tablet_replica_cache.contains(job_id));
    auto filter_it = manager._event_driven_filters.find(job_id);
    ASSERT_NE(filter_it, manager._event_driven_filters.end());
    ASSERT_TRUE(filter_it->second.has_value());
    EXPECT_EQ(3, filter_it->second->size());
    EXPECT_TRUE(filter_it->second->contains(10));
    EXPECT_TRUE(filter_it->second->contains(20));
    EXPECT_TRUE(filter_it->second->contains(30));
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventWithEmptyTableIdsStoresEmptyFilter) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1003;
    std::vector<int64_t> table_ids = {};

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());
    auto filter_it = manager._event_driven_filters.find(job_id);
    ASSERT_NE(filter_it, manager._event_driven_filters.end());
    ASSERT_TRUE(filter_it->second.has_value());
    EXPECT_TRUE(filter_it->second->empty());
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventClearRemovesFilterAndCache) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1004;
    std::vector<int64_t> table_ids = {10, 20};

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(manager._tablet_replica_cache.contains(job_id));
    EXPECT_TRUE(manager._event_driven_filters.contains(job_id));

    st = manager.set_event(job_id, TWarmUpEventType::LOAD, true);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(manager._tablet_replica_cache.contains(job_id));
    EXPECT_FALSE(manager._event_driven_filters.contains(job_id));
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventUpdateTableIdsReplacesFilter) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1005;

    std::vector<int64_t> initial_ids = {10, 20};
    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &initial_ids);
    EXPECT_TRUE(st.ok());

    std::vector<int64_t> updated_ids = {30, 40, 50};
    st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &updated_ids);
    EXPECT_TRUE(st.ok());
    auto filter_it = manager._event_driven_filters.find(job_id);
    ASSERT_NE(filter_it, manager._event_driven_filters.end());
    ASSERT_TRUE(filter_it->second.has_value());
    EXPECT_EQ(3, filter_it->second->size());
    EXPECT_FALSE(filter_it->second->contains(10));
    EXPECT_TRUE(filter_it->second->contains(30));
    EXPECT_TRUE(filter_it->second->contains(40));
    EXPECT_TRUE(filter_it->second->contains(50));
}

TEST_F(CloudWarmUpManagerFilterTest, SetEventUnsupportedType) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1006;

    auto st = manager.set_event(job_id, TWarmUpEventType::QUERY, false, nullptr);
    EXPECT_FALSE(st.ok());
}

TEST_F(CloudWarmUpManagerFilterTest, GetReplicaInfoAppliesTableFilter) {
    CloudWarmUpManager manager(_engine);
    int64_t tablet_id = 3001;
    auto now = std::chrono::steady_clock::now();

    manager._tablet_replica_cache[2001][tablet_id] = {now, make_replica(11)};
    manager._event_driven_filters[2001] = std::unordered_set<int64_t> {10};

    manager._tablet_replica_cache[2002][tablet_id] = {now, make_replica(22)};
    manager._event_driven_filters[2002] = std::unordered_set<int64_t> {20};

    bool cache_hit = false;
    auto replicas = manager.get_replica_info(tablet_id, 20, false, cache_hit);

    ASSERT_EQ(1, replicas.size());
    EXPECT_EQ(22, replicas[0].backend_id);
    EXPECT_TRUE(cache_hit);
}

TEST_F(CloudWarmUpManagerFilterTest, GetReplicaInfoBypassesFilterWhenTableIdUnknown) {
    CloudWarmUpManager manager(_engine);
    int64_t tablet_id = 3002;
    auto now = std::chrono::steady_clock::now();

    manager._tablet_replica_cache[3001][tablet_id] = {now, make_replica(31)};
    manager._event_driven_filters[3001] = std::unordered_set<int64_t> {10};

    manager._tablet_replica_cache[3002][tablet_id] = {now, make_replica(32)};
    manager._event_driven_filters[3002] = std::unordered_set<int64_t> {20};

    bool cache_hit = false;
    auto replicas = manager.get_replica_info(tablet_id, 0, false, cache_hit);

    ASSERT_EQ(2, replicas.size());
    EXPECT_TRUE(cache_hit);
}

} // namespace doris
