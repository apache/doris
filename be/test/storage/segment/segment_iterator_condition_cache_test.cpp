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

#include "exprs/vexpr_context.h"
#include "exprs/virtual_slot_ref.h"
#include "runtime/exec_env.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/condition_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {
class ScopedConditionCache {
public:
    ScopedConditionCache()
            : _previous(ExecEnv::GetInstance()->get_condition_cache()),
              _cache(ConditionCache::create_global_cache(1024 * 1024, 4)) {
        ExecEnv::GetInstance()->_condition_cache = _cache.get();
    }

    ~ScopedConditionCache() { ExecEnv::GetInstance()->_condition_cache = _previous; }

    ConditionCache* get() const { return _cache.get(); }

private:
    ConditionCache* _previous;
    std::unique_ptr<ConditionCache> _cache;
};

TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* col = schema_pb.add_column();
    col->set_unique_id(0);
    col->set_name("k0");
    col->set_type("INT");
    col->set_is_key(true);
    col->set_is_nullable(false);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

std::shared_ptr<Segment> make_stub_segment(uint32_t num_rows,
                                           const TabletSchemaSPtr& tablet_schema) {
    auto seg = std::make_shared<Segment>(0, RowsetId(), tablet_schema, InvertedIndexFileInfo());
    seg->_num_rows = num_rows;
    return seg;
}

} // namespace

class SegmentIteratorConditionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        auto tablet_schema = make_tablet_schema();
        _segment = make_stub_segment(100, tablet_schema);
        _read_schema =
                std::make_shared<Schema>(tablet_schema->columns(), std::vector<ColumnId> {0});
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);
        _iter->_opts.stats = &_stats;
        TExprNode node;
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_node_type(TExprNodeType::VIRTUAL_SLOT_REF);
        node.__set_is_nullable(true);
        node.__set_label("cache_test");
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(-1);
        slot_ref.__set_tuple_id(-1);
        node.__set_slot_ref(slot_ref);
        _iter->_common_expr_ctxs_push_down = {
                std::make_shared<VExprContext>(VirtualSlotRef::create_shared(node))};
    }

    OlapReaderStatistics _stats;
    std::shared_ptr<Segment> _segment;
    std::shared_ptr<Schema> _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
};

TEST_F(SegmentIteratorConditionCacheTest,
       condition_cache_lookups_count_misses_and_hits_per_reader) {
    ScopedConditionCache cache;
    constexpr uint64_t digest = 12345;
    _iter->_opts.condition_cache_digest = digest;
    _iter->_row_bitmap.addRange(0, 100);
    _iter->_init_row_bitmap_by_condition_cache();
    EXPECT_FALSE(_iter->_find_condition_cache);
    EXPECT_EQ(_stats.condition_cache_lookup_count, 1);
    EXPECT_EQ(_stats.condition_cache_hit_seg_nums, 0);

    ConditionCache::CacheKey key(_iter->_opts.rowset_id, _iter->segment_id(), digest);
    cache.get()->insert(key, std::move(_iter->_condition_cache));

    OlapReaderStatistics second_stats;
    auto second_reader = std::make_unique<SegmentIterator>(_segment, _read_schema);
    second_reader->_opts.stats = &second_stats;
    second_reader->_opts.condition_cache_digest = digest;
    second_reader->_common_expr_ctxs_push_down = _iter->_common_expr_ctxs_push_down;
    second_reader->_row_bitmap.addRange(0, 100);
    second_reader->_init_row_bitmap_by_condition_cache();
    EXPECT_TRUE(second_reader->_find_condition_cache);
    EXPECT_EQ(second_stats.condition_cache_lookup_count, 1);
    EXPECT_EQ(second_stats.condition_cache_hit_seg_nums, 1);
    EXPECT_TRUE(second_reader->_row_bitmap.isEmpty());
    EXPECT_EQ(_stats.condition_cache_lookup_count, 1);
    EXPECT_EQ(_stats.condition_cache_hit_seg_nums, 0);
}

TEST_F(SegmentIteratorConditionCacheTest, condition_cache_bypass_does_not_count_lookups) {
    ScopedConditionCache cache;
    _iter->_opts.condition_cache_digest = 0;
    _iter->_init_row_bitmap_by_condition_cache();
    EXPECT_EQ(_stats.condition_cache_lookup_count, 0);

    _iter->_opts.condition_cache_digest = 12345;
    _iter->_common_expr_ctxs_push_down.clear();
    _iter->_init_row_bitmap_by_condition_cache();
    EXPECT_EQ(_stats.condition_cache_lookup_count, 0);
    EXPECT_EQ(_iter->_opts.condition_cache_digest, 0);
}

} // namespace doris::segment_v2
