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

#include "runtime/query_cache/query_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "testutil/column_helper.h"

namespace doris {
class QueryCacheTest : public testing::Test {
public:
    void SetUp() override {}
};

TEST_F(QueryCacheTest, create_global_cache) {
    auto* cache = QueryCache::create_global_cache(1024 * 1024 * 1024, 16);
    delete cache;
}

TEST_F(QueryCacheTest, build_cache_key) {
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.emplace_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.emplace_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_xyz"});
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        EXPECT_FALSE(st.ok());
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range;
        TPaloScanRange palp_scan_range;
        palp_scan_range.__set_tablet_id(42);
        palp_scan_range.__set_version("114514");
        scan_range.scan_range.__set_palo_scan_range(palp_scan_range);
        scan_ranges.push_back(scan_range);
        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        std::cout << version << std::endl;
        EXPECT_FALSE(st.ok());
    }
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range;
        TPaloScanRange palp_scan_range;
        palp_scan_range.__set_tablet_id(42);
        palp_scan_range.__set_version("114514");
        scan_range.scan_range.__set_palo_scan_range(palp_scan_range);
        scan_ranges.push_back(scan_range);
        TQueryCacheParam cache_param;
        cache_param.__set_digest("be ut");
        cache_param.tablet_to_range.insert({42, "test"});
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);
        std::cout << st.msg() << std::endl;
        std::cout << version << std::endl;
        std::cout << cache_key << std::endl;
        EXPECT_TRUE(st.ok());
    }
}

TEST_F(QueryCacheTest, build_cache_key_multiple_tablets) {
    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(3);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(1);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TScanRangeParams scan_range3;
        TPaloScanRange palp_scan_range3;
        palp_scan_range3.__set_tablet_id(2);
        palp_scan_range3.__set_version("100");
        scan_range3.scan_range.__set_palo_scan_range(palp_scan_range3);
        scan_ranges.push_back(scan_range3);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_abc"});
        cache_param.tablet_to_range.insert({3, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_TRUE(st.ok());
        EXPECT_EQ(version, 100);

        int64_t expected_tablet1 = 1;
        int64_t expected_tablet2 = 2;
        int64_t expected_tablet3 = 3;
        std::string expected_key =
                "test_digest" +
                std::string(reinterpret_cast<char*>(&expected_tablet1), sizeof(expected_tablet1)) +
                std::string(reinterpret_cast<char*>(&expected_tablet2), sizeof(expected_tablet2)) +
                std::string(reinterpret_cast<char*>(&expected_tablet3), sizeof(expected_tablet3)) +
                "range_abc";

        EXPECT_EQ(cache_key, expected_key);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("200");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("same version") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({2, "range_xyz"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("same tablet_to_range") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TScanRangeParams scan_range1;
        TPaloScanRange palp_scan_range1;
        palp_scan_range1.__set_tablet_id(1);
        palp_scan_range1.__set_version("100");
        scan_range1.scan_range.__set_palo_scan_range(palp_scan_range1);
        scan_ranges.push_back(scan_range1);

        TScanRangeParams scan_range2;
        TPaloScanRange palp_scan_range2;
        palp_scan_range2.__set_tablet_id(2);
        palp_scan_range2.__set_version("100");
        scan_range2.scan_range.__set_palo_scan_range(palp_scan_range2);
        scan_ranges.push_back(scan_range2);

        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        cache_param.tablet_to_range.insert({1, "range_abc"});
        cache_param.tablet_to_range.insert({3, "range_abc"});

        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("Not find tablet") != std::string::npos);
    }

    {
        std::vector<TScanRangeParams> scan_ranges;
        TQueryCacheParam cache_param;
        cache_param.__set_digest("test_digest");
        std::string cache_key;
        int64_t version = 0;
        auto st = QueryCache::build_cache_key(scan_ranges, cache_param, &cache_key, &version);

        EXPECT_FALSE(st.ok());
        EXPECT_TRUE(st.msg().find("empty") != std::string::npos);
    }
}

TEST_F(QueryCacheTest, insert_and_lookup) {
    std::unique_ptr<QueryCache> query_cache {QueryCache::create_global_cache(1024 * 1024 * 1024)};
    std::string cache_key = "be ut";
    {
        //insert
        CacheResult result;
        result.push_back(std::make_unique<Block>());
        *result.back() = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5});
        query_cache->insert(cache_key, 42, result, {1, 2, 3}, 1);
    }

    {
        //lookup
        std::unique_ptr<QueryCacheHandle> handle = std::make_unique<QueryCacheHandle>();
        EXPECT_TRUE(query_cache->lookup(cache_key, 42, handle.get()));
        EXPECT_TRUE(ColumnHelper::block_equal(
                *handle->get_cache_result()->back(),
                ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5})));
        EXPECT_EQ(handle->get_cache_slot_orders()->size(), 3);
        EXPECT_EQ(handle->get_cache_version(), 42);

        QueryCacheHandle handle1 {std::move(*handle)};
        QueryCacheHandle handle2;
        handle2 = std::move(handle1);

        EXPECT_TRUE(ColumnHelper::block_equal(
                *handle2.get_cache_result()->back(),
                ColumnHelper::create_block<DataTypeInt32>({1, 2, 3, 4, 5})));
        EXPECT_EQ(handle2.get_cache_slot_orders()->size(), 3);
        EXPECT_EQ(handle2.get_cache_version(), 42);
    }
}

// ./run-be-ut.sh --run --filter=DataQueueTest.*

} // namespace doris
