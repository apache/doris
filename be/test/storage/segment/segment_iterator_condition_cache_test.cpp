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

#include <memory>
#include <vector>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/predicate/null_predicate.h"
#include "storage/segment/condition_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

class SegmentIteratorConditionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_cache = ExecEnv::GetInstance()->get_condition_cache();
        _cache.reset(ConditionCache::create_global_cache(1024 * 1024, 4));
        ExecEnv::GetInstance()->_condition_cache = _cache.get();

        TabletSchemaPB schema;
        schema.set_keys_type(KeysType::DUP_KEYS);
        auto* column = schema.add_column();
        column->set_unique_id(0);
        column->set_name("k1");
        column->set_type("INT");
        column->set_is_key(true);
        column->set_is_nullable(true);
        _tablet_schema = std::make_shared<TabletSchema>();
        _tablet_schema->init_from_pb(schema);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
        _segment =
                std::make_shared<Segment>(7, RowsetId(), _tablet_schema, InvertedIndexFileInfo());
        _segment->_num_rows = 3 * ConditionCacheContext::GRANULE_SIZE;
    }

    void TearDown() override { ExecEnv::GetInstance()->_condition_cache = _previous_cache; }

    std::unique_ptr<SegmentIterator> make_iterator(bool write_enabled) {
        StorageReadOptions opts(_stats);
        opts.runtime_state = &_runtime_state;
        opts.condition_cache_digest = 12345;
        opts.condition_cache_write_enabled = write_enabled;
        auto iterator = std::make_unique<SegmentIterator>(_segment, _read_schema, opts);
        iterator->_col_predicates = {
                NullPredicate::create_shared(0, "k1", false, PrimitiveType::TYPE_INT)};
        iterator->_row_bitmap.addRange(0, _segment->num_rows());
        return iterator;
    }

    ConditionCache::CacheKey cache_key() { return {RowsetId(), 7, 12345}; }

    ConditionCache* _previous_cache = nullptr;
    std::unique_ptr<ConditionCache> _cache;
    TabletSchemaSPtr _tablet_schema;
    ReadSchemaSPtr _read_schema;
    std::shared_ptr<Segment> _segment;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
};

TEST_F(SegmentIteratorConditionCacheTest, ReadOnlyMissDoesNotCollectOrPublish) {
    auto iterator = make_iterator(false);
    iterator->_init_row_bitmap_by_condition_cache();
    EXPECT_FALSE(iterator->_find_condition_cache);
    EXPECT_EQ(iterator->_condition_cache, nullptr);
    EXPECT_EQ(iterator->_row_bitmap.cardinality(), _segment->num_rows());
    EXPECT_FALSE(iterator->_should_write_condition_cache());

    // This is also the finalization path when a TopN scan reaches EOF. It must neither
    // publish threshold-based exclusions nor dereference a missing MISS bitmap.
    iterator->_write_condition_cache();
    ConditionCacheHandle handle;
    EXPECT_FALSE(_cache->lookup(cache_key(), &handle));
}

TEST_F(SegmentIteratorConditionCacheTest, ReadOnlyHitUsesStaticExclusionsWithoutMutatingCache) {
    auto bitmap = std::make_shared<std::vector<bool>>(
            std::initializer_list<bool> {false, true, false, false});
    _cache->insert(cache_key(), bitmap);
    auto iterator = make_iterator(false);
    iterator->_init_row_bitmap_by_condition_cache();

    ASSERT_TRUE(iterator->_find_condition_cache);
    EXPECT_EQ(iterator->_condition_cache, nullptr);
    EXPECT_EQ(iterator->_row_bitmap.cardinality(), ConditionCacheContext::GRANULE_SIZE);
    EXPECT_TRUE(iterator->_row_bitmap.contains(ConditionCacheContext::GRANULE_SIZE));
    EXPECT_FALSE(iterator->_row_bitmap.contains(0));
    EXPECT_FALSE(iterator->_row_bitmap.contains(2 * ConditionCacheContext::GRANULE_SIZE));
    EXPECT_EQ(_stats.condition_cache_hit_seg_nums, 1);
    EXPECT_FALSE(iterator->_should_write_condition_cache());

    iterator->_write_condition_cache();
    ConditionCacheHandle handle;
    ASSERT_TRUE(_cache->lookup(cache_key(), &handle));
    EXPECT_EQ(handle.get_filter_result(), bitmap);
    EXPECT_EQ(*bitmap, (std::vector<bool> {false, true, false, false}));
}

TEST_F(SegmentIteratorConditionCacheTest, OrdinaryScanStillPopulatesCacheForReadOnlyScan) {
    auto writer = make_iterator(true);
    writer->_init_row_bitmap_by_condition_cache();
    ASSERT_NE(writer->_condition_cache, nullptr);
    EXPECT_TRUE(writer->_should_write_condition_cache());
    (*writer->_condition_cache)[1] = true;
    writer->_write_condition_cache();

    auto reader = make_iterator(false);
    reader->_init_row_bitmap_by_condition_cache();
    EXPECT_TRUE(reader->_find_condition_cache);
    EXPECT_EQ(reader->_row_bitmap.cardinality(), ConditionCacheContext::GRANULE_SIZE);
    EXPECT_EQ(reader->_condition_cache, nullptr);
}

TEST_F(SegmentIteratorConditionCacheTest, DisabledDigestDoesNotReadOrWrite) {
    auto iterator = make_iterator(true);
    iterator->_opts.condition_cache_digest = 0;
    iterator->_init_row_bitmap_by_condition_cache();
    EXPECT_FALSE(iterator->_find_condition_cache);
    EXPECT_EQ(iterator->_condition_cache, nullptr);
    EXPECT_FALSE(iterator->_should_write_condition_cache());
    iterator->_write_condition_cache();
    ConditionCacheHandle handle;
    EXPECT_FALSE(_cache->lookup(cache_key(), &handle));
}

TEST_F(SegmentIteratorConditionCacheTest, ReadLimitDoesNotPublishPartialBitmap) {
    auto iterator = make_iterator(true);
    iterator->_init_row_bitmap_by_condition_cache();
    ASSERT_NE(iterator->_condition_cache, nullptr);
    (*iterator->_condition_cache)[0] = true;
    iterator->_opts.read_limit = 1;
    iterator->_rows_returned = 1;

    iterator->_write_condition_cache();
    ConditionCacheHandle handle;
    EXPECT_FALSE(_cache->lookup(cache_key(), &handle));
}

} // namespace doris::segment_v2
