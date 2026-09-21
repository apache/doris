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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/primary_key_index.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris {

TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns, KeysType keys_type);
using Generator = std::function<void(size_t rid, int cid, Field& field)>;
void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                   TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                   std::shared_ptr<Segment>* res, std::string segment_dir);

class SegmentPrimaryKeyCacheTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_dir).ok());
        ASSERT_TRUE(fs->create_directory(_dir).ok());
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));
        _schema = create_schema(
                {create_varchar_key(0, false),
                 create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE,
                                  false)},
                UNIQUE_KEYS);
        SegmentWriterOptions opts;
        opts.enable_unique_key_merge_on_write = true;
        auto generator = [](size_t rid, int cid, Field& field) {
            if (cid == 0) {
                // Each key exceeds the normal PK data-page target, producing a large value index.
                field = Field::create_field<TYPE_VARCHAR>(std::string(50000, 'a' + rid));
            } else {
                field = Field::create_field<TYPE_INT>(static_cast<int32_t>(rid));
            }
        };
        build_segment(opts, _schema, 0, _schema, 8, generator, &_segment, _dir);
        ASSERT_NE(_segment, nullptr);
        ASSERT_EQ(_segment->_pk_index_reader, nullptr);
        ASSERT_FALSE(_segment->_load_index_once.has_called());
        ASSERT_FALSE(_segment->_load_pk_bf_once.has_called());
        ASSERT_FALSE(_segment->_pk_index_meta->primary_key_index()
                             .value_index_meta()
                             .is_root_data_page());
        _present_key = _segment->min_key();

        // Find a definite BF miss without initializing the segment's own reader. Do not assume
        // that an arbitrary absent key is rejected, because bloom filters allow false positives.
        PrimaryKeyIndexReader probe;
        ASSERT_TRUE(
                probe.parse_bf(_segment->file_reader(), *_segment->_pk_index_meta, nullptr).ok());
        for (int i = 0; i < 10000; ++i) {
            auto candidate = _present_key + std::to_string(i);
            if (!probe.check_present(Slice(candidate))) {
                _missing_key = std::move(candidate);
                break;
            }
        }
        ASSERT_FALSE(_missing_key.empty());
    }

    void TearDown() override {
        _segment.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_dir).ok());
    }

    Status lookup(const std::string& key, RowLocation* location) {
        return _segment->lookup_row_key(Slice(key), _schema.get(), false, false, location, nullptr);
    }

    const std::string _dir = "./ut_dir/segment_primary_key_cache_test";
    TabletSchemaSPtr _schema;
    std::shared_ptr<segment_v2::Segment> _segment;
    std::string _present_key;
    std::string _missing_key;
};

TEST_F(SegmentPrimaryKeyCacheTest, CachedRootPagesIncreaseChargeOnlyOnce) {
    SegmentCache cache(1024 * 1024 * 1024, 10000);
    SegmentCacheHandle handle;
    const SegmentCache::CacheKey key(_segment->rowset_id(), _segment->id());
    cache.insert(key, *new SegmentCache::CacheValue(_segment), &handle);
    const auto usage = cache.get_usage();
    const auto tracked = cache.mem_consumption();
    RowLocation location;
    ASSERT_TRUE(lookup(_missing_key, &location).is<ErrorCode::KEY_NOT_FOUND>());
    // Keep the original index-first lookup: even a definite BF miss loads the roots.
    const auto roots = _segment->_pk_index_reader->get_memory_size();
    EXPECT_GT(roots, 300000);
    EXPECT_EQ(cache.get_usage(), usage + roots);
    EXPECT_EQ(cache.mem_consumption(), tracked);
    ASSERT_TRUE(lookup(_present_key, &location).ok());
    ASSERT_TRUE(_segment->load_index(nullptr).ok());
    EXPECT_EQ(cache.get_usage(), usage + roots);
}

TEST_F(SegmentPrimaryKeyCacheTest, LazyRootLoadingEvictsOversizedSegment) {
    // SegmentCache has 64 shards. Metadata fits; the large PK roots do not.
    SegmentCache cache((_segment->cache_charge() + 65536) * 64, 10000);
    SegmentCacheHandle handle;
    const SegmentCache::CacheKey key(_segment->rowset_id(), _segment->id());
    cache.insert(key, *new SegmentCache::CacheValue(_segment), &handle);
    ASSERT_EQ(cache.get_element_count(), 1);
    ASSERT_TRUE(_segment->load_index(nullptr).ok());
    EXPECT_EQ(cache.get_element_count(), 0);
    EXPECT_EQ(cache.get_usage(), 0);
    // Eviction drops cache ownership, not the active operation's shared_ptr.
    RowLocation location;
    ASSERT_TRUE(lookup(_present_key, &location).ok());
    EXPECT_EQ(location.row_id, 0);
}

TEST_F(SegmentPrimaryKeyCacheTest, EagerRootLoadingChargesOnInsert) {
    const auto capacity = (_segment->cache_charge() + 65536) * 64;
    ASSERT_TRUE(_segment->load_index(nullptr).ok());
    SegmentCache cache(capacity, 10000);
    SegmentCacheHandle handle;
    const SegmentCache::CacheKey key(_segment->rowset_id(), _segment->id());
    cache.insert(key, *new SegmentCache::CacheValue(_segment), &handle);
    EXPECT_EQ(cache.get_usage(), 0);
    EXPECT_EQ(cache.get_element_count(), 0);
    EXPECT_EQ(handle.get_segments().front(), _segment);
}

TEST_F(SegmentPrimaryKeyCacheTest, OldSegmentDoesNotChargeItsReplacement) {
    SegmentCache cache(1024 * 1024 * 1024, 10000);
    const SegmentCache::CacheKey key(_segment->rowset_id(), _segment->id());
    SegmentCacheHandle old_handle, new_handle;
    cache.insert(key, *new SegmentCache::CacheValue(_segment), &old_handle);
    auto replacement = std::make_shared<segment_v2::Segment>(_segment->id(), _segment->rowset_id(),
                                                             _schema, InvertedIndexFileInfo {});
    cache.insert(key, *new SegmentCache::CacheValue(replacement), &new_handle);
    const auto usage = cache.get_usage();
    ASSERT_TRUE(_segment->load_index(nullptr).ok());
    EXPECT_EQ(cache.get_usage(), usage);
    EXPECT_EQ(cache.get_element_count(), 1);
}

TEST_F(SegmentPrimaryKeyCacheTest, SegmentCanOutliveCache) {
    {
        SegmentCache cache(1024 * 1024 * 1024, 10000);
        SegmentCacheHandle handle;
        const SegmentCache::CacheKey key(_segment->rowset_id(), _segment->id());
        cache.insert(key, *new SegmentCache::CacheValue(_segment), &handle);
    }
    ASSERT_TRUE(_segment->load_index(nullptr).ok());
    RowLocation location;
    ASSERT_TRUE(lookup(_present_key, &location).ok());
}

TEST_F(SegmentPrimaryKeyCacheTest, FailedIndexLoadDoesNotPinPartialRoots) {
    auto meta = *_segment->_pk_index_meta;
    meta.mutable_primary_key_index()->mutable_value_index_meta()->mutable_root_page()->set_offset(
            uint64_t {1} << 50);
    PrimaryKeyIndexReader reader;
    EXPECT_FALSE(reader.parse_index(_segment->file_reader(), meta, nullptr).ok());
    EXPECT_EQ(reader._index_reader, nullptr);
}

} // namespace doris
