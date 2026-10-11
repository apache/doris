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

#include "cloud/config.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
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

TEST_F(QueryCacheTest, FilePayloadInsertLookupOwnsAllChildren) {
    // Keep both versioned payloads within one shard's budget so LRU-K admission
    // does not mask the value-ownership and replacement checks below.
    std::unique_ptr<QueryCache> cache(QueryCache::create_global_cache(1024 * 1024, 1));
    const auto type = make_nullable(std::make_shared<DataTypeFile>());
    const std::string bytes = std::string("\0\xff\x80", 3) + std::string(4096, 'q');
    auto column = type->create_column();
    File value {Field::create_field<TYPE_STRING>("s3://bucket/a%2Fb?versionId=cache"),
                Field::create_field<TYPE_BIGINT>(7),
                Field::create_field<TYPE_BIGINT>(4099),
                Field::create_field<TYPE_STRING>("application/octet-stream"),
                Field::create_field<TYPE_STRING>("ETAG:cache"),
                Field::create_field<TYPE_VARBINARY>(StringView(bytes))};
    column->insert(Field::create_field<TYPE_FILE>(value));
    value[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    column->insert(Field::create_field<TYPE_FILE>(value));
    for (size_t child = 1; child < value.size(); ++child) {
        value[child] = Field();
    }
    column->insert(Field::create_field<TYPE_FILE>(value));
    column->insert_default();

    auto expect_payload = [&](const Block& block) {
        ASSERT_EQ(block.rows(), 4);
        ASSERT_EQ(block.columns(), 1);
        ASSERT_TRUE(block.get_by_position(0).type->equals(*type));
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& file = assert_cast<const ColumnFile&>(nullable.get_nested_column());
        ASSERT_EQ(file.get_columns().size(), 6);
        EXPECT_TRUE(nullable.is_null_at(3));
        for (size_t row = 0; row < 3; ++row) {
            ASSERT_FALSE(nullable.is_null_at(row));
            const auto actual = file[row];
            ASSERT_EQ(actual.get_type(), TYPE_FILE);
            const auto& fields = actual.get<TYPE_FILE>();
            ASSERT_EQ(fields.size(), 6);
            EXPECT_EQ(fields[0].get<TYPE_STRING>(), "s3://bucket/a%2Fb?versionId=cache");
            if (row == 2) {
                for (size_t child = 1; child < fields.size(); ++child) {
                    EXPECT_TRUE(fields[child].is_null());
                }
            } else {
                EXPECT_EQ(fields[1].get<TYPE_BIGINT>(), 7);
                EXPECT_EQ(fields[2].get<TYPE_BIGINT>(), 4099);
                EXPECT_EQ(fields[3].get<TYPE_STRING>(), "application/octet-stream");
                EXPECT_EQ(fields[4].get<TYPE_STRING>(), "ETAG:cache");
                ASSERT_FALSE(fields[5].is_null());
                EXPECT_EQ(fields[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
                          row == 0 ? bytes : std::string());
            }
        }
    };

    CacheResult input;
    input.emplace_back(std::make_unique<Block>());
    input.back()->insert({std::move(column), type, "f"});
    cache->insert("file-payload", 42, input, {17}, input.back()->allocated_bytes());
    QueryCacheHandle handle;
    ASSERT_TRUE(cache->lookup("file-payload", 42, &handle));
    ASSERT_EQ(handle.get_cache_result()->size(), 1);
    EXPECT_EQ(*handle.get_cache_slot_orders(), (std::vector<int> {17}));
    EXPECT_EQ(handle.get_cache_result()->front()->rows(), 4);
    const auto& original = assert_cast<const ColumnFile&>(
            assert_cast<const ColumnNullable&>(*input.front()->get_by_position(0).column)
                    .get_nested_column());
    const auto& cached = assert_cast<const ColumnFile&>(
            assert_cast<const ColumnNullable&>(
                    *handle.get_cache_result()->front()->get_by_position(0).column)
                    .get_nested_column());
    for (size_t child = 0; child < 6; ++child) {
        EXPECT_NE(&original.get_column(child), &cached.get_column(child));
    }
    EXPECT_NE(original.get_column(5).get_data_at(0).data, cached.get_column(5).get_data_at(0).data);
    // Reuse the producer's block and release its storage before another lookup.
    input.front()->clear_column_data(1);
    input.front()->get_by_position(0).column->assert_mutable()->insert_default();
    input.clear();
    expect_payload(*handle.get_cache_result()->front());
    {
        QueryCacheHandle again;
        ASSERT_TRUE(cache->lookup("file-payload", 42, &again));
        expect_payload(*again.get_cache_result()->front());
    }
    QueryCacheHandle wrong_version;
    EXPECT_FALSE(cache->lookup("file-payload", 43, &wrong_version));

    CacheResult replacement;
    replacement.emplace_back(std::make_unique<Block>());
    auto null_column = type->create_column();
    null_column->insert_default();
    replacement.back()->insert({std::move(null_column), type, "f"});
    cache->insert("file-payload", 43, replacement, {17}, replacement.back()->allocated_bytes());
    replacement.clear();
    // Replacement must not invalidate a pinned old entry's FILE children/inline arena.
    QueryCacheHandle pinned(std::move(handle));
    expect_payload(*pinned.get_cache_result()->front());
    QueryCacheHandle latest;
    ASSERT_TRUE(cache->lookup("file-payload", 43, &latest));
    EXPECT_EQ(latest.get_cache_result()->front()->rows(), 1);
    EXPECT_TRUE(latest.get_cache_result()->front()->get_by_position(0).column->is_null_at(0));
}

// ./run-be-ut.sh --run --filter=DataQueueTest.*

} // namespace doris
