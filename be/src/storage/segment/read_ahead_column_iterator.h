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

#pragma once

#include <memory>

#include "storage/segment/column_page_read_observer.h"
#include "storage/segment/column_reader.h"

namespace doris::segment_v2 {

class ColumnReadAheadController;

/// Full ColumnIterator decorator. The inner iterator remains the sole authority for read state:
/// access paths set before wrapping and changes made through either iterator stay visible through
/// the virtual accessors. The wrapper owns read-ahead policy and intercepts physical page loads.
class ReadAheadColumnIterator final : public ColumnIterator, private ColumnPageReadObserver {
public:
    explicit ReadAheadColumnIterator(ColumnIteratorUPtr inner);
    ~ReadAheadColumnIterator() override;

    using ColumnIterator::next_batch;

    Status init(const ColumnIteratorOptions& opts) override;
    Status seek_to_ordinal(ordinal_t ord) override { return _inner->seek_to_ordinal(ord); }
    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        return _inner->next_batch(n, dst, has_null);
    }
    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override {
        return _inner->next_batch_of_zone_map(n, dst);
    }
    Status read_by_rowids(const rowid_t* rowids, size_t count, MutableColumnPtr& dst) override {
        return _inner->read_by_rowids(rowids, count, dst);
    }
    ordinal_t get_current_ordinal() const override { return _inner->get_current_ordinal(); }
    std::shared_ptr<ColumnReader> get_column_reader() const override {
        return _inner->get_column_reader();
    }

    Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_zone_map(predicates, delete_predicates, row_ranges);
    }
    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* predicates,
                                          RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_bloom_filter(predicates, row_ranges);
    }
    Status get_row_ranges_by_dict(const AndBlockColumnPredicate* predicates,
                                  RowRanges* row_ranges) override {
        return _inner->get_row_ranges_by_dict(predicates, row_ranges);
    }
    bool is_all_dict_encoding() const override { return _inner->is_all_dict_encoding(); }

    Status set_access_paths(const TColumnAccessPaths& all,
                            const TColumnAccessPaths& predicate) override {
        return _inner->set_access_paths(all, predicate);
    }
    void set_column_name(const std::string& name) override { _inner->set_column_name(name); }
    const std::string& column_name() const override { return _inner->column_name(); }
    void set_read_requirement(ReadRequirement requirement) override {
        _inner->set_read_requirement(requirement);
    }
    void set_read_requirement_self(ReadRequirement requirement) override {
        _inner->set_read_requirement_self(requirement);
    }
    ReadRequirement read_requirement() const override { return _inner->read_requirement(); }
    void set_lazy_output_requirement() override { _inner->set_lazy_output_requirement(); }
    void remove_pruned_sub_iterators() override { _inner->remove_pruned_sub_iterators(); }
    bool read_offset_only() const override { return _inner->read_offset_only(); }
    bool read_null_map_only() const override { return _inner->read_null_map_only(); }
    void set_read_phase(ReadPhase phase) override { _inner->set_read_phase(phase); }
    bool need_to_read() const override { return _inner->need_to_read(); }
    bool need_to_read_meta_columns() const override { return _inner->need_to_read_meta_columns(); }
    void finalize_lazy_phase(MutableColumnPtr& dst) override { _inner->finalize_lazy_phase(dst); }
    bool has_lazy_read_target() const override { return _inner->has_lazy_read_target(); }

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    Status prepare_read_ahead(const ColumnReadAheadHint& hint) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

private:
    ColumnPageReadContext before_page_read(int32_t page_index, io::FileRange range) override;
    void after_page_read(int32_t page_index, io::FileRange range) override;

    ColumnIteratorUPtr _inner;
    std::unique_ptr<ColumnReadAheadController> _read_ahead;
};

} // namespace doris::segment_v2
