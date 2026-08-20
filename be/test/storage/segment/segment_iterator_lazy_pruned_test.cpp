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

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/page_prefetcher.h"
#include "storage/tablet/tablet_schema.h"

// and the small amount of state it consumes. This mirrors the existing
// segment_iterator_* white-box tests.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/segment/segment_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

class TrackingLazyColumnIterator final : public ColumnIterator {
public:
    explicit TrackingLazyColumnIterator(std::string name = {},
                                        std::vector<std::string>* global_events = nullptr)
            : _name(std::move(name)), _global_events(global_events) {}

    Status seek_to_ordinal(ordinal_t ord) override {
        seek_ordinals.push_back(ord);
        _current_ordinal = ord;
        record_event("seek");
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        next_batch_phases.push_back(_read_phase);
        record_event("next");
        auto& int_column = assert_cast<ColumnVector<TYPE_INT>&>(*dst);
        for (size_t i = 0; i < *n; ++i) {
            int_column.insert_value(cast_set<int32_t>(_current_ordinal + i));
        }
        _current_ordinal += *n;
        *has_null = false;
        return Status::OK();
    }

    Status prepare_page_prefetch(const PagePrefetchRequest& request) override {
        prefetch_phases.push_back(_read_phase);
        prefetch_kinds.push_back(request.kind);
        if (request.kind == PagePrefetchRequest::Kind::ROWIDS) {
            DORIS_CHECK(request.rowids != nullptr);
            prefetch_rowids.emplace_back(request.rowids, request.rowids + request.rowid_count);
        } else {
            prefetch_rowids.emplace_back();
        }
        prefetch_first_ordinals.push_back(request.first_ordinal);
        prefetch_ordinal_counts.push_back(request.ordinal_count);
        prefetch_directions.push_back(request.is_forward);
        record_event("prepare");
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        read_phases.push_back(_read_phase);
        read_rowids.assign(rowids, rowids + count);
        ++read_by_rowids_count;
        record_event("read");

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

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    ReadPhase phase() const { return _read_phase; }

    std::vector<ordinal_t> seek_ordinals;
    std::vector<rowid_t> read_rowids;
    std::vector<std::vector<rowid_t>> prefetch_rowids;
    std::vector<PagePrefetchRequest::Kind> prefetch_kinds;
    std::vector<ordinal_t> prefetch_first_ordinals;
    std::vector<size_t> prefetch_ordinal_counts;
    std::vector<bool> prefetch_directions;
    std::vector<ReadPhase> prefetch_phases;
    std::vector<ReadPhase> next_batch_phases;
    std::vector<ReadPhase> read_phases;
    std::vector<ReadPhase> finalize_phases;
    int read_by_rowids_count = 0;
    int finalize_count = 0;

private:
    void record_event(const std::string& operation) {
        if (_global_events != nullptr) {
            _global_events->emplace_back(operation + ":" + _name);
        }
    }

    std::string _name;
    std::vector<std::string>* _global_events = nullptr;
    ordinal_t _current_ordinal = 0;
};

TabletSchemaSPtr make_tablet_schema(size_t column_count = 1) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    for (size_t cid = 0; cid < column_count; ++cid) {
        auto* col = schema_pb.add_column();
        col->set_unique_id(cast_set<int32_t>(cid));
        col->set_name("c" + std::to_string(cid));
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
    }

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

Block make_int_block(size_t column_count = 1) {
    Block block;
    for (size_t cid = 0; cid < column_count; ++cid) {
        block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(),
                      "c" + std::to_string(cid)});
    }
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
    ASSERT_EQ(tracking_iter->prefetch_rowids.size(), 1);
    EXPECT_EQ(tracking_iter->prefetch_rowids[0], (std::vector<rowid_t> {30, 10}));
    EXPECT_EQ(tracking_iter->prefetch_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::LAZY}));
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
    EXPECT_TRUE(tracking_iter->prefetch_rowids.empty());
    EXPECT_EQ(tracking_iter->finalize_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::LAZY}));
    EXPECT_EQ(tracking_iter->phase(), ColumnIterator::ReadPhase::NORMAL);
    EXPECT_EQ(block.get_by_position(0).column->size(), 0);
}

TEST_F(SegmentIteratorLazyPrunedTest, preparesContinuousPredicateColumnsBeforeConsumption) {
    auto tablet_schema = make_tablet_schema(2);
    auto read_schema = make_read_schema(tablet_schema);
    auto iter = std::make_unique<SegmentIterator>(nullptr, read_schema);
    iter->_opts.tablet_schema = tablet_schema;
    iter->_opts.stats = &_stats;
    iter->_support_lazy_read_pruned_columns = {0};
    iter->_predicate_column_ids = {0, 1};
    iter->_column_iterators.resize(2);
    iter->_current_return_columns.resize(2);
    iter->_current_return_columns[0] = ColumnInt32::create();
    iter->_current_return_columns[1] = ColumnInt32::create();
    iter->_block_rowids.resize(2);
    iter->_row_bitmap.add(10);
    iter->_row_bitmap.add(11);
    iter->_init_range_iterator();

    std::vector<std::string> events;
    auto first = std::make_unique<TrackingLazyColumnIterator>("c0", &events);
    auto* first_ptr = first.get();
    auto second = std::make_unique<TrackingLazyColumnIterator>("c1", &events);
    auto* second_ptr = second.get();
    iter->_column_iterators[0] = std::move(first);
    iter->_column_iterators[1] = std::move(second);

    uint16_t rows_read = 0;
    auto st = iter->_read_columns_by_index(2, rows_read);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(rows_read, 2);
    EXPECT_EQ(events, (std::vector<std::string> {"prepare:c0", "prepare:c1", "seek:c0", "next:c0",
                                                 "seek:c1", "next:c1"}));
    ASSERT_EQ(first_ptr->prefetch_kinds.size(), 1);
    EXPECT_EQ(first_ptr->prefetch_kinds[0], PagePrefetchRequest::Kind::ORDINAL_RANGE);
    EXPECT_EQ(first_ptr->prefetch_first_ordinals[0], 10);
    EXPECT_EQ(first_ptr->prefetch_ordinal_counts[0], 2);
    EXPECT_EQ(first_ptr->prefetch_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::PREDICATE}));
    ASSERT_EQ(second_ptr->prefetch_kinds.size(), 1);
    EXPECT_EQ(second_ptr->prefetch_kinds[0], PagePrefetchRequest::Kind::ORDINAL_RANGE);
    EXPECT_EQ(second_ptr->prefetch_first_ordinals[0], 10);
    EXPECT_EQ(second_ptr->prefetch_ordinal_counts[0], 2);
    EXPECT_EQ(second_ptr->prefetch_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::NORMAL}));
}

TEST_F(SegmentIteratorLazyPrunedTest, preparesReverseOrdinalRangeBeforePredicateConsumption) {
    auto tablet_schema = make_tablet_schema(2);
    auto read_schema = make_read_schema(tablet_schema);
    auto iter = std::make_unique<SegmentIterator>(nullptr, read_schema);
    iter->_opts.tablet_schema = tablet_schema;
    iter->_opts.stats = &_stats;
    iter->_opts.read_orderby_key_reverse = true;
    iter->_predicate_column_ids = {0, 1};
    iter->_column_iterators.resize(2);
    iter->_current_return_columns.resize(2);
    iter->_current_return_columns[0] = ColumnInt32::create();
    iter->_current_return_columns[1] = ColumnInt32::create();
    iter->_block_rowids.resize(2);
    iter->_row_bitmap.add(10);
    iter->_row_bitmap.add(11);
    iter->_init_range_iterator();

    std::vector<std::string> events;
    auto first = std::make_unique<TrackingLazyColumnIterator>("c0", &events);
    auto* first_ptr = first.get();
    auto second = std::make_unique<TrackingLazyColumnIterator>("c1", &events);
    auto* second_ptr = second.get();
    iter->_column_iterators[0] = std::move(first);
    iter->_column_iterators[1] = std::move(second);

    uint16_t rows_read = 0;
    auto st = iter->_read_columns_by_index(2, rows_read);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(rows_read, 2);
    EXPECT_EQ(events, (std::vector<std::string> {"prepare:c0", "prepare:c1", "seek:c0", "next:c0",
                                                 "seek:c1", "next:c1"}));
    ASSERT_EQ(first_ptr->prefetch_kinds.size(), 1);
    EXPECT_EQ(first_ptr->prefetch_kinds[0], PagePrefetchRequest::Kind::ORDINAL_RANGE);
    EXPECT_EQ(first_ptr->prefetch_first_ordinals[0], 11);
    EXPECT_EQ(first_ptr->prefetch_ordinal_counts[0], 2);
    EXPECT_FALSE(first_ptr->prefetch_directions[0]);
    ASSERT_EQ(second_ptr->prefetch_kinds.size(), 1);
    EXPECT_EQ(second_ptr->prefetch_kinds[0], PagePrefetchRequest::Kind::ORDINAL_RANGE);
    EXPECT_EQ(second_ptr->prefetch_first_ordinals[0], 11);
    EXPECT_EQ(second_ptr->prefetch_ordinal_counts[0], 2);
    EXPECT_FALSE(second_ptr->prefetch_directions[0]);
}

TEST_F(SegmentIteratorLazyPrunedTest, preparesAllLazyColumnsBeforeConsumption) {
    auto tablet_schema = make_tablet_schema(2);
    auto read_schema = make_read_schema(tablet_schema);
    auto iter = std::make_unique<SegmentIterator>(nullptr, read_schema);
    iter->_opts.tablet_schema = tablet_schema;
    iter->_opts.stats = &_stats;
    iter->_support_lazy_read_pruned_columns = {0, 1};
    iter->_column_iterators.resize(2);

    std::vector<std::string> events;
    auto first = std::make_unique<TrackingLazyColumnIterator>("c0", &events);
    auto* first_ptr = first.get();
    auto second = std::make_unique<TrackingLazyColumnIterator>("c1", &events);
    auto* second_ptr = second.get();
    iter->_column_iterators[0] = std::move(first);
    iter->_column_iterators[1] = std::move(second);
    iter->_selected_size = 2;
    iter->_block_rowids = {10, 20, 30, 40};
    iter->_sel_rowid_idx = {2, 0};

    auto block = make_int_block(2);
    auto st = iter->_read_lazy_pruned_columns(&block);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(events,
              (std::vector<std::string> {"prepare:c0", "prepare:c1", "read:c0", "read:c1"}));
    ASSERT_EQ(first_ptr->prefetch_rowids.size(), 1);
    EXPECT_EQ(first_ptr->prefetch_rowids[0], (std::vector<rowid_t> {30, 10}));
    ASSERT_EQ(second_ptr->prefetch_rowids.size(), 1);
    EXPECT_EQ(second_ptr->prefetch_rowids[0], (std::vector<rowid_t> {30, 10}));
}

TEST_F(SegmentIteratorLazyPrunedTest, preparesSelectedColumnsBeforeReadByRowids) {
    auto tablet_schema = make_tablet_schema(2);
    auto read_schema = make_read_schema(tablet_schema);
    auto iter = std::make_unique<SegmentIterator>(nullptr, read_schema);
    iter->_opts.tablet_schema = tablet_schema;
    iter->_opts.stats = &_stats;
    iter->_support_lazy_read_pruned_columns = {0};
    iter->_column_iterators.resize(2);
    iter->_current_return_columns.resize(2);
    iter->_current_return_columns[0] = ColumnInt32::create();
    iter->_current_return_columns[1] = ColumnInt32::create();

    std::vector<std::string> events;
    auto first = std::make_unique<TrackingLazyColumnIterator>("c0", &events);
    auto* first_ptr = first.get();
    auto second = std::make_unique<TrackingLazyColumnIterator>("c1", &events);
    auto* second_ptr = second.get();
    iter->_column_iterators[0] = std::move(first);
    iter->_column_iterators[1] = std::move(second);

    std::vector<ColumnId> read_column_ids {0, 1};
    std::vector<rowid_t> source_rowids {10, 20, 30, 40};
    uint16_t selected_indexes[] = {2, 0};
    auto st = iter->_read_columns_by_rowids(read_column_ids, source_rowids, selected_indexes,
                                            std::size(selected_indexes),
                                            &iter->_current_return_columns, false, true);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ(events,
              (std::vector<std::string> {"prepare:c0", "prepare:c1", "read:c0", "read:c1"}));
    ASSERT_EQ(first_ptr->prefetch_rowids.size(), 1);
    EXPECT_EQ(first_ptr->prefetch_rowids[0], (std::vector<rowid_t> {30, 10}));
    EXPECT_EQ(first_ptr->prefetch_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::PREDICATE}));
    ASSERT_EQ(second_ptr->prefetch_rowids.size(), 1);
    EXPECT_EQ(second_ptr->prefetch_rowids[0], (std::vector<rowid_t> {30, 10}));
    EXPECT_EQ(second_ptr->prefetch_phases,
              (std::vector<ColumnIterator::ReadPhase> {ColumnIterator::ReadPhase::NORMAL}));
}

} // namespace doris::segment_v2
