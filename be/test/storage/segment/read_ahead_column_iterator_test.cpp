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

#include "storage/segment/read_ahead_column_iterator.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "core/column/column_vector.h"
#include "storage/iterators.h"
#include "storage/segment/column_read_ahead.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment_prefetcher.h"

namespace doris::segment_v2 {
namespace {

// A stateful decoder exercises the decorator through the public ColumnIterator contract.
class StatefulColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t ordinal) override {
        RETURN_IF_ERROR(read_status);
        _ordinal = ordinal;
        return Status::OK();
    }
    ordinal_t get_current_ordinal() const override { return _ordinal; }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        RETURN_IF_ERROR(read_status);
        *has_null = false;
        if (!need_to_read()) {
            _convert_to_place_holder_column(dst, *n);
            return Status::OK();
        }
        _recovery_from_place_holder_column(dst);
        for (size_t i = 0; i < *n; ++i) {
            assert_cast<ColumnInt32&>(*dst).insert_value(cast_set<int32_t>(_ordinal++));
        }
        return Status::OK();
    }
    Status read_by_rowids(const rowid_t* rows, size_t count, MutableColumnPtr& dst) override {
        RETURN_IF_ERROR(read_status);
        _recovery_from_place_holder_column(dst);
        for (size_t i = 0; i < count; ++i) {
            assert_cast<ColumnInt32&>(*dst).insert_value(cast_set<int32_t>(rows[i]));
            _ordinal = rows[i] + 1;
        }
        return Status::OK();
    }
    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override {
        RETURN_IF_ERROR(read_status);
        dst->insert_many_defaults(*n);
        _ordinal += *n;
        return Status::OK();
    }
    Status get_row_ranges_by_zone_map(const AndBlockColumnPredicate*,
                                      const std::vector<std::shared_ptr<const ColumnPredicate>>*,
                                      RowRanges* ranges) override {
        *ranges = RowRanges::create_single(2, 8);
        return read_status;
    }
    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate*,
                                          RowRanges* ranges) override {
        *ranges = RowRanges::create_single(3, 7);
        return read_status;
    }
    Status get_row_ranges_by_dict(const AndBlockColumnPredicate*, RowRanges* ranges) override {
        *ranges = RowRanges::create_single(4, 6);
        return read_status;
    }
    bool is_all_dict_encoding() const override { return true; }

    Status init_prefetcher(const SegmentPrefetchParams&) override {
        ++prefetcher_init_calls;
        return read_status;
    }
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod method) override {
        prefetchers[method].push_back(nullptr);
    }
    Status prepare_read_ahead(const ColumnReadAheadHint&) override { return read_status; }

    Status read_status = Status::OK();
    int prefetcher_init_calls = 0;

private:
    ordinal_t _ordinal = 0;
};

TEST(ReadAheadColumnIteratorTest, ReadsAndErrorsUseInnerCursor) {
    auto inner = std::make_unique<StatefulColumnIterator>();
    auto* decoder = inner.get();
    ColumnIteratorUPtr iterator = std::make_unique<ReadAheadColumnIterator>(std::move(inner));
    ASSERT_TRUE(iterator->init({}).ok());
    ASSERT_TRUE(iterator->seek_to_ordinal(7).ok());
    MutableColumnPtr output = ColumnInt32::create();
    size_t count = 2;
    ASSERT_TRUE(iterator->next_batch(&count, output).ok());
    EXPECT_EQ(iterator->get_current_ordinal(), 9);
    std::array<rowid_t, 2> rowids {11, 19};
    ASSERT_TRUE(iterator->read_by_rowids(rowids.data(), rowids.size(), output).ok());
    EXPECT_EQ(iterator->get_current_ordinal(), 20);
    bool has_null = true;
    count = 1;
    ASSERT_TRUE(iterator->next_batch(&count, output, &has_null).ok());
    EXPECT_FALSE(has_null);
    const auto& values = assert_cast<const ColumnInt32&>(*output).get_data();
    ASSERT_EQ(values.size(), 5);
    EXPECT_EQ(values[0], 7);
    EXPECT_EQ(values[1], 8);
    EXPECT_EQ(values[2], 11);
    EXPECT_EQ(values[3], 19);
    EXPECT_EQ(values[4], 20);
    ASSERT_TRUE(iterator->next_batch_of_zone_map(&count, output).ok());
    EXPECT_EQ(iterator->get_current_ordinal(), 22);
    decoder->read_status = Status::IOError("decoder failed");
    EXPECT_FALSE(iterator->seek_to_ordinal(0).ok());
    EXPECT_FALSE(iterator->next_batch(&count, output).ok());
    EXPECT_FALSE(iterator->read_by_rowids(rowids.data(), rowids.size(), output).ok());
    EXPECT_EQ(output->size(), 6);
    EXPECT_EQ(iterator->get_current_ordinal(), 22);
}

TEST(ReadAheadColumnIteratorTest, LazyMaterializationUsesOneSetOfState) {
    auto inner = std::make_unique<StatefulColumnIterator>();
    auto* decoder = inner.get();
    inner->set_column_name("before_wrapping");
    inner->set_lazy_output_requirement();
    ColumnIteratorUPtr iterator = std::make_unique<ReadAheadColumnIterator>(std::move(inner));
    EXPECT_EQ(iterator->column_name(), "before_wrapping");
    EXPECT_EQ(iterator->read_requirement(), ColumnIterator::ReadRequirement::LAZY_OUTPUT);
    EXPECT_TRUE(iterator->has_lazy_read_target());
    iterator->set_column_name("after_wrapping");
    EXPECT_EQ(decoder->column_name(), "after_wrapping");
    iterator->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    EXPECT_FALSE(iterator->need_to_read());
    MutableColumnPtr output = ColumnInt32::create();
    size_t count = 5;
    ASSERT_TRUE(iterator->next_batch(&count, output).ok());
    ASSERT_EQ(output->size(), 5);
    // A filter retains two placeholders, then the lazy phase must clear them on the inner.
    output->pop_back(3);
    decoder->set_read_phase(ColumnIterator::ReadPhase::LAZY);
    EXPECT_TRUE(iterator->need_to_read());
    EXPECT_TRUE(iterator->need_to_read_meta_columns());
    iterator->finalize_lazy_phase(output);
    EXPECT_EQ(output->size(), 0);
    const std::array<rowid_t, 2> rowids {2, 4};
    ASSERT_TRUE(iterator->read_by_rowids(rowids.data(), rowids.size(), output).ok());
    ASSERT_EQ(output->size(), 2);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*output).get_data()[1], 4);
    iterator->set_read_requirement_self(ColumnIterator::ReadRequirement::PREDICATE);
    EXPECT_FALSE(iterator->has_lazy_read_target());
    EXPECT_FALSE(iterator->need_to_read_meta_columns());
    iterator->set_read_phase(ColumnIterator::ReadPhase::PREDICATE);
    EXPECT_TRUE(iterator->need_to_read());
}

TEST(ReadAheadColumnIteratorTest, PruningAndLegacyPrefetchSurviveDisabledReadAhead) {
    auto inner = std::make_unique<StatefulColumnIterator>();
    auto* decoder = inner.get();
    ReadAheadColumnIterator iterator(std::move(inner));
    ASSERT_TRUE(iterator.init({}).ok());
    EXPECT_TRUE(iterator.is_all_dict_encoding());
    RowRanges ranges = RowRanges::create_single(10);
    ASSERT_TRUE(iterator.get_row_ranges_by_zone_map(nullptr, nullptr, &ranges).ok());
    EXPECT_EQ(ranges.to_string(), RowRanges::create_single(2, 8).to_string());
    ASSERT_TRUE(iterator.get_row_ranges_by_bloom_filter(nullptr, &ranges).ok());
    EXPECT_EQ(ranges.to_string(), RowRanges::create_single(3, 7).to_string());
    ASSERT_TRUE(iterator.get_row_ranges_by_dict(nullptr, &ranges).ok());
    EXPECT_EQ(ranges.to_string(), RowRanges::create_single(4, 6).to_string());
    OlapReaderStatistics stats;
    StorageReadOptions read_options(stats);
    ASSERT_TRUE(iterator.init_prefetcher({{4, 1024}, read_options}).ok());
    EXPECT_EQ(decoder->prefetcher_init_calls, 1);
    std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>> prefetchers;
    const auto method = static_cast<PrefetcherInitMethod>(0);
    iterator.collect_prefetchers(prefetchers, method);
    ASSERT_EQ(prefetchers[method].size(), 1);
    decoder->read_status = Status::IOError("pruning failed");
    EXPECT_FALSE(iterator.get_row_ranges_by_zone_map(nullptr, nullptr, &ranges).ok());
    EXPECT_FALSE(iterator.get_row_ranges_by_bloom_filter(nullptr, &ranges).ok());
    EXPECT_FALSE(iterator.get_row_ranges_by_dict(nullptr, &ranges).ok());
    EXPECT_FALSE(iterator.prepare_read_ahead(ExactRowReadAheadHint {}).ok());
    EXPECT_FALSE(iterator.init_prefetcher({{4, 1024}, read_options}).ok());
}

} // namespace
} // namespace doris::segment_v2
