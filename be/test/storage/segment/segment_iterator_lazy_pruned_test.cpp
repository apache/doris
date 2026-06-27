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

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

// Use #define private public to access SegmentIterator::_read_lazy_pruned_columns()
// and the small amount of state it consumes. This mirrors the existing
// segment_iterator_* white-box tests.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "storage/segment/segment_iterator.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

class TrackingLazyColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t ord) override {
        seek_ordinals.push_back(ord);
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        read_phases.push_back(_read_phase);
        read_rowids.assign(rowids, rowids + count);
        ++read_by_rowids_count;

        auto& int_column = assert_cast<ColumnVector<TYPE_INT>&>(*dst);
        for (size_t i = 0; i < count; ++i) {
            int_column.insert_value(cast_set<int32_t>(rowids[i]));
        }
        return Status::OK();
    }

    void finalize_lazy_phase(MutableColumnPtr& dst) override {
        finalize_phases.push_back(_read_phase);
        ++finalize_count;
    }

    ordinal_t get_current_ordinal() const override { return 0; }

    ReadPhase phase() const { return _read_phase; }

    std::vector<ordinal_t> seek_ordinals;
    std::vector<rowid_t> read_rowids;
    std::vector<ReadPhase> read_phases;
    std::vector<ReadPhase> finalize_phases;
    int read_by_rowids_count = 0;
    int finalize_count = 0;
};

TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* col = schema_pb.add_column();
    col->set_unique_id(0);
    col->set_name("c0");
    col->set_type("INT");
    col->set_is_key(true);
    col->set_is_nullable(false);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

SchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    std::vector<ColumnId> read_column_ids(tablet_schema->num_columns());
    for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
        read_column_ids[cid] = cid;
    }
    return std::make_shared<Schema>(tablet_schema->columns(), read_column_ids);
}

Block make_int_block() {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "c0"});
    return block;
}

} // namespace

class SegmentIteratorLazyPrunedTest : public ::testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema();
        _read_schema = make_read_schema(_tablet_schema);
    }

    std::unique_ptr<SegmentIterator> make_iter(TrackingLazyColumnIterator** tracking_iter) {
        auto iter = std::make_unique<SegmentIterator>(nullptr, _read_schema);
        iter->_opts.tablet_schema = _tablet_schema;
        iter->_opts.stats = &_stats;
        iter->_schema_block_id_map = {0};
        iter->_support_lazy_read_pruned_columns.insert(0);
        iter->_column_iterators.resize(1);

        auto column_iter = std::make_unique<TrackingLazyColumnIterator>();
        *tracking_iter = column_iter.get();
        iter->_column_iterators[0] = std::move(column_iter);
        return iter;
    }

    TabletSchemaSPtr _tablet_schema;
    SchemaSPtr _read_schema;
    OlapReaderStatistics _stats;
};

TEST_F(SegmentIteratorLazyPrunedTest, readsSelectedRowidsInLazyPhaseAndRestoresPhase) {
    TrackingLazyColumnIterator* tracking_iter = nullptr;
    auto iter = make_iter(&tracking_iter);
    iter->_selected_size = 2;
    iter->_block_rowids = {10, 20, 30, 40};
    iter->_sel_rowid_idx = {2, 0};

    auto block = make_int_block();
    auto st = iter->_read_lazy_pruned_columns(&block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(tracking_iter->read_by_rowids_count, 1);
    EXPECT_EQ(tracking_iter->finalize_count, 1);
    EXPECT_EQ(tracking_iter->read_rowids, (std::vector<rowid_t> {30, 10}));
    EXPECT_EQ(tracking_iter->read_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::LAZY}));
    EXPECT_EQ(tracking_iter->finalize_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::LAZY}));
    EXPECT_EQ(tracking_iter->phase(), ColumnIterator::ReadPhase::NORMAL);

    const auto& result =
            assert_cast<const ColumnVector<TYPE_INT>&>(*block.get_by_position(0).column);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.get_data()[0], 30);
    EXPECT_EQ(result.get_data()[1], 10);
}

TEST_F(SegmentIteratorLazyPrunedTest, emptySelectionStillFinalizesLazyPlaceholders) {
    TrackingLazyColumnIterator* tracking_iter = nullptr;
    auto iter = make_iter(&tracking_iter);
    iter->_selected_size = 0;

    auto block = make_int_block();
    auto st = iter->_read_lazy_pruned_columns(&block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(tracking_iter->read_by_rowids_count, 0);
    EXPECT_EQ(tracking_iter->finalize_count, 1);
    EXPECT_EQ(tracking_iter->finalize_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::LAZY}));
    EXPECT_EQ(tracking_iter->phase(), ColumnIterator::ReadPhase::NORMAL);
    EXPECT_EQ(block.get_by_position(0).column->size(), 0);
}

} // namespace doris::segment_v2
