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

#include "parallel_scanner_builder.h"

#include "exec/olap_utils.h"
#include "olap/iterators.h"
#include "olap/primary_key_index.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/short_key_index.h"
#include "util/key_util.h"
#include "vec/exec/scan/new_olap_scanner.h"

namespace doris {

using namespace vectorized;

using SplitKey = StorageReadOptions::SplitKey;
using SplitKeyRange = StorageReadOptions::SplitKeyRange;

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::build_scanners(std::list<VScannerSPtr>& scanners) {
    RETURN_IF_ERROR(_load());
    if (_is_dup_mow_key) {
        return _build_scanners_by_rowid(scanners);
    } else {
        return _build_scanners_by_key_range(scanners);
    }
}

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_build_scanners_by_rowid(
        std::list<VScannerSPtr>& scanners) {
    DCHECK_GE(_rows_per_scanner, _min_rows_per_scanner);
    for (auto&& [tablet, version] : _tablets) {
        DCHECK(_all_rowsets.contains(tablet->tablet_id()));
        auto& rowsets = _all_rowsets[tablet->tablet_id()];

        TabletReader::ReadSource reade_source_with_delete_info;
        if (!_state->skip_delete_predicate()) {
            RETURN_IF_ERROR(tablet->capture_rs_readers(
                    {0, version}, &reade_source_with_delete_info.rs_splits, false));
            reade_source_with_delete_info.fill_delete_predicates();
        }

        TabletReader::ReadSource read_source;

        int64_t rows_collected = 0;
        for (auto& rowset : rowsets) {
            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            RowsetReaderSharedPtr reader;
            RETURN_IF_ERROR(beta_rowset->create_reader(&reader));
            const auto rowset_id = beta_rowset->rowset_id();

            DCHECK(_segment_cache_handles.contains(rowset_id));
            auto& segment_cache_handle = _segment_cache_handles[rowset_id];

            if (beta_rowset->num_rows() == 0) {
                continue;
            }

            const auto& segments = segment_cache_handle.get_segments();
            int segment_start = 0;
            auto split = RowSetSplits(reader->clone());

            for (size_t i = 0; i != segments.size(); ++i) {
                const auto& segment = segments[i];
                RowRanges row_ranges;
                const size_t rows_of_segment = segment->num_rows();
                int64_t offset_in_segment = 0;

                // try to split large segments into RowRanges
                while (offset_in_segment < rows_of_segment) {
                    const int64_t remaining_rows = rows_of_segment - offset_in_segment;
                    auto rows_need = _rows_per_scanner - rows_collected;

                    // 0.9: try to avoid splitting the segments into excessively small parts.
                    if (rows_need >= remaining_rows * 0.9) {
                        rows_need = remaining_rows;
                    }
                    DCHECK_LE(rows_need, remaining_rows);

                    // RowRange stands for range: [From, To), From is inclusive, To is exclusive.
                    row_ranges.add({offset_in_segment,
                                    offset_in_segment + static_cast<int64_t>(rows_need)});
                    rows_collected += rows_need;
                    offset_in_segment += rows_need;

                    // If collected enough rows, build a new scanner
                    if (rows_collected >= _rows_per_scanner) {
                        split.segment_offsets.first = segment_start,
                        split.segment_offsets.second = i + 1;
                        split.segment_row_ranges.emplace_back(std::move(row_ranges));

                        DCHECK_EQ(split.segment_offsets.second - split.segment_offsets.first,
                                  split.segment_row_ranges.size());

                        read_source.rs_splits.emplace_back(std::move(split));

                        scanners.emplace_back(
                                _build_scanner(tablet, version, _key_ranges,
                                               {std::move(read_source.rs_splits),
                                                reade_source_with_delete_info.delete_predicates}));

                        read_source = TabletReader::ReadSource();
                        split = RowSetSplits(reader->clone());
                        row_ranges = RowRanges();

                        segment_start = offset_in_segment < rows_of_segment ? i : i + 1;
                        rows_collected = 0;
                    }
                }

                // The non-empty `row_ranges` means there are some rows left in this segment not added into `split`.
                if (!row_ranges.is_empty()) {
                    DCHECK_GT(rows_collected, 0);
                    DCHECK_EQ(row_ranges.to(), segment->num_rows());
                    split.segment_row_ranges.emplace_back(std::move(row_ranges));
                }
            }

            DCHECK_LE(rows_collected, _rows_per_scanner);
            if (rows_collected > 0) {
                split.segment_offsets.first = segment_start;
                split.segment_offsets.second = segments.size();
                DCHECK_GT(split.segment_offsets.second, split.segment_offsets.first);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
                read_source.rs_splits.emplace_back(std::move(split));
            }
        } // end `for (auto& rowset : rowsets)`

        DCHECK_LE(rows_collected, _rows_per_scanner);
        if (rows_collected > 0) {
            DCHECK_GT(read_source.rs_splits.size(), 0);
#ifndef NDEBUG
            for (auto& split : read_source.rs_splits) {
                DCHECK(split.rs_reader != nullptr);
                DCHECK_LT(split.segment_offsets.first, split.segment_offsets.second);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
            }
#endif
            scanners.emplace_back(
                    _build_scanner(tablet, version, _key_ranges,
                                   {std::move(read_source.rs_splits),
                                    reade_source_with_delete_info.delete_predicates}));
        }
    }

    return Status::OK();
}

/**
 * Load rowsets of each tablet with specified version, segments of each rowset.
 */
template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_load() {
    for (auto&& [tablet, version] : _tablets) {
        const auto tablet_id = tablet->tablet_id();
        _tablets_rows.emplace_back(0);
        auto& rows = _tablets_rows.back();
        auto& rowsets = _all_rowsets[tablet_id];
        RETURN_IF_ERROR(tablet->capture_consistent_rowsets({0, version}, &rowsets));

        for (auto& rowset : rowsets) {
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();
            auto& segment_cache_handle = _segment_cache_handles[rowset_id];

            /// All loaded segments will be cached into segment_cache_handle.
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                    std::dynamic_pointer_cast<BetaRowset>(rowset), &segment_cache_handle, true));
            _total_rows += rowset->num_rows();
            rows += _total_rows;
        }
    }

    _rows_per_scanner = _total_rows / _max_scanners_count;
    _rows_per_scanner = std::max<size_t>(_rows_per_scanner, _min_rows_per_scanner);

    return Status::OK();
}

template <typename ParentType>
std::unique_ptr<SegmentGroup>
ParallelScannerBuilder<ParentType>::_create_segment_group_from_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return {};
    }

    size_t max_rows = 0;
    auto segment_group = std::make_unique<SegmentGroup>();
    for (const auto& rowset : rowsets) {
        const auto rowset_id = rowset->rowset_id();
        DCHECK(_segment_cache_handles.contains(rowset_id));
        auto& cache_handle = _segment_cache_handles[rowset_id];
        auto& segments = cache_handle.get_segments();

        if (segments.empty()) {
            continue;
        }

        if (rowset->is_segments_overlapping()) {
            auto* largest_segment = segments[0].get();
            for (size_t i = 1; i != segments.size(); ++i) {
                if (segments[i]->num_rows() > largest_segment->num_rows()) {
                    largest_segment = segments[i].get();
                }
            }

            if (largest_segment->num_rows() > max_rows) {
                segment_group->rowset = rowset;
                segment_group->num_rows = largest_segment->num_rows();
                segment_group->segments.clear();
                segment_group->segments.emplace_back(largest_segment);
                max_rows = largest_segment->num_rows();
            }
        } else if (rowset->num_rows() > max_rows) {
            max_rows = rowset->num_rows();
            segment_group->rowset = rowset;
            segment_group->segments.clear();
            segment_group->segments.reserve(segments.size());
            segment_group->num_rows = 0;
            for (auto& segment : segments) {
                segment_group->num_rows += segment->num_rows();
                segment_group->segments.emplace_back(segment.get());
            }
        }
    }

    if (max_rows == 0) {
        return {};
    }

    DCHECK(segment_group->rowset != nullptr);
    return segment_group;
}

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_parse_segment_group(SegmentGroup& group) {
    for (auto* segment : group.segments) {
        RETURN_IF_ERROR(segment->load_index());
        const ShortKeyIndexDecoder* index = segment->get_short_key_index();
        group.num_blocks += index->num_items();
    }
    return Status::OK();
}

template <typename ParentType>
std::shared_ptr<NewOlapScanner> ParallelScannerBuilder<ParentType>::_build_scanner(
        BaseTabletSPtr tablet, int64_t version, const std::vector<OlapScanRange*>& key_ranges,
        TabletReader::ReadSource&& read_source) {
    NewOlapScanner::Params params {
            _state,  _scanner_profile.get(), key_ranges,         std::move(tablet),
            version, std::move(read_source), _limit_per_scanner, _is_preaggregation};
    return NewOlapScanner::create_shared(_parent, std::move(params));
}

// Convert key ranges from `OlapScanRange` to `RowCursor` and then encode them into `std::string`.
template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_convert_key_ranges(
        const TabletSPtr& tablet, std::vector<std::string>& start_keys,
        std::vector<std::string>& end_keys, std::vector<bool>& include_begin_keys,
        std::vector<bool>& include_end_keys, std::shared_ptr<Schema>& schema) {
    DCHECK(start_keys.empty());
    DCHECK(end_keys.empty());

    TabletSchemaSPtr tablet_schema = tablet->tablet_schema();

    std::vector<uint32_t> columns;
    if (_key_ranges.empty()) {
        columns.resize(tablet->num_short_key_columns());
    } else {
        DCHECK_EQ(_key_ranges.front()->end_scan_range.size(),
                  _key_ranges.front()->begin_scan_range.size());
        columns.resize(_key_ranges.front()->end_scan_range.size());
    }

    std::iota(columns.begin(), columns.end(), 0);
    schema = std::make_shared<Schema>(tablet_schema->columns(), columns);

    if (_key_ranges.empty()) {
        /// Empty `std::string` means the min/max value of keys which will be handled in `SegmentIterator`.
        start_keys.emplace_back();
        end_keys.emplace_back();
        include_begin_keys.emplace_back(true);
        include_end_keys.emplace_back(true);
        return Status::OK();
    }

    for (auto& key_range : _key_ranges) {
        start_keys.emplace_back();
        end_keys.emplace_back();

        include_begin_keys.emplace_back(key_range->begin_include);
        include_end_keys.emplace_back(key_range->end_include);

        RowCursor start_key;
        RowCursor end_key;
        RETURN_IF_ERROR(start_key.init_scan_key(tablet->tablet_schema(),
                                                key_range->begin_scan_range.values(), schema));
        RETURN_IF_ERROR(start_key.from_tuple(key_range->begin_scan_range));

        encode_key_with_padding(&start_keys.back(), start_key,
                                tablet_schema->num_short_key_columns(), key_range->begin_include);

        RETURN_IF_ERROR(end_key.init_scan_key(tablet->tablet_schema(),
                                              key_range->end_scan_range.values(), schema));
        RETURN_IF_ERROR(end_key.from_tuple(key_range->end_scan_range));

        encode_key_with_padding(&end_keys.back(), end_key, tablet_schema->num_short_key_columns(),
                                !key_range->end_include);
    }

    return Status::OK();
}

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_build_scanners_by_key_range(
        std::list<VScannerSPtr>& scanners) {
    for (size_t i = 0; i != _tablets.size(); ++i) {
        auto& tablet = _tablets[i].tablet;
        auto version = _tablets[i].version;

        DCHECK(_all_rowsets.contains(tablet->tablet_id()));
        auto& rowsets = _all_rowsets[tablet->tablet_id()];
        auto segment_group = _create_segment_group_from_rowsets(rowsets);

        if (!segment_group || segment_group->num_rows == 0) {
            continue;
        }

        RETURN_IF_ERROR(_parse_segment_group(*segment_group));

        DCHECK_GE(_tablets_rows[i], segment_group->num_rows);

        TabletSchemaSPtr tablet_schema = tablet->tablet_schema();
        std::vector<std::string> original_start_keys;
        std::vector<std::string> original_end_keys;
        std::vector<bool> original_include_begin_keys;
        std::vector<bool> original_include_end_keys;

        std::vector<SplitKeyRange> split_ranges;

        std::shared_ptr<Schema> key_column_schema;
        RETURN_IF_ERROR(_convert_key_ranges(tablet, original_start_keys, original_end_keys,
                                            original_include_begin_keys, original_include_end_keys,
                                            key_column_schema));

        auto total_blocks_count = segment_group->num_blocks;
        size_t blocks_per_scanner = total_blocks_count * _rows_per_scanner / _tablets_rows[i];
        blocks_per_scanner = std::max<size_t>(1, blocks_per_scanner);

        for (size_t key_index = 0; key_index != original_start_keys.size(); ++key_index) {
            const std::string& start_key_string = original_start_keys[key_index];
            const std::string& end_key_string = original_end_keys[key_index];
            size_t collected_blocks = 0;

            Slice start_key(start_key_string);
            const Slice end_key(end_key_string);

            bool include_begin = original_include_begin_keys[key_index];
            bool original_end_key_matched = false;
            for (Segment* segment : segment_group->segments) {
                const ShortKeyIndexDecoder* key_index_decoder = segment->get_short_key_index();

                // `start_key` or `end_key` with empty value mean the minimum(-∞) or maximum value(+∞).
                // If the `start_key` is empty,
                // the first row(ordinal = 0) of this segment should equal with or greater than the `start_key`,
                // so here take the begin as `lower_bound`
                auto lower_bound = start_key.empty() ? key_index_decoder->begin()
                                                     : key_index_decoder->lower_bound(start_key);

                // If the `end_key` is empty,
                // the last row(ordinal = num_rows - 1) will equal with or lesser than the `end_key`,
                // so here take the end as `upper_bound`.
                auto upper_bound = end_key.empty() ? key_index_decoder->end()
                                                   : key_index_decoder->upper_bound(end_key);

                // If `lower_bound` is not valid, it means than cannot find any index which equal with
                // or greater than the `start_key`. For example: start_key: 1001, segment's key range: 200-1000.
                // So this segment is not in this key range, we should ignore it.
                if (!lower_bound.valid() || lower_bound.ordinal() > upper_bound.ordinal()) {
                    continue;
                }

                auto previous_key = key_index_decoder->key(lower_bound.ordinal());

                ++lower_bound;
                while (lower_bound.valid() && lower_bound != upper_bound) {
                    auto key = key_index_decoder->key(lower_bound.ordinal());
                    ++lower_bound;
                    if (key != previous_key) {
                        previous_key = key;
                        ++collected_blocks;

                        if (collected_blocks >= blocks_per_scanner) {
                            DCHECK_LT(start_key.compare(key), 0);
                            SplitKey lower_key {start_key.to_string(), key_column_schema,
                                                include_begin};

                            bool include_end = false;
                            if (!end_key.empty() && key.compare(end_key) > 0) {
                                DCHECK(upper_bound.valid());
                                original_end_key_matched = true;
                                key = end_key;
                                include_end = original_include_end_keys[key_index];
                            }
                            SplitKey upper_key {key.to_string(), key_column_schema, include_end};

                            split_ranges.emplace_back(std::move(lower_key), std::move(upper_key));

                            collected_blocks = 0;
                            include_begin = true;
                            start_key = key;

                            if (key == end_key) {
                                break;
                            }
                        }
                    }
                }

                if (upper_bound.valid() && !original_end_key_matched) {
                    original_end_key_matched = true;
                    SplitKey lower_key {start_key.to_string(), key_column_schema, include_begin};
                    SplitKey upper_key {end_key.to_string(), key_column_schema,
                                        original_include_end_keys[key_index]};
                    split_ranges.emplace_back(std::move(lower_key), std::move(upper_key));
                    break;
                }
            }

            if (!original_end_key_matched) {
                SplitKey lower_key {start_key.to_string(), key_column_schema, include_begin};
                SplitKey upper_key {end_key.to_string(), key_column_schema,
                                    original_include_end_keys[key_index]};
                split_ranges.emplace_back(std::move(lower_key), std::move(upper_key));
            }

            DCHECK_EQ(split_ranges.back().upper_key.key, end_key.to_string());
        }

        const auto ranges_total_count = split_ranges.size();
        for (size_t index = 0; index != ranges_total_count; ++index) {
            auto scanner = _build_scanner(tablet, version, _key_ranges, {});
            scanner->set_split_key_range(split_ranges[index]);
            scanners.emplace_back(std::move(scanner));
        }
    }
    return Status::OK();
}

template class ParallelScannerBuilder<NewOlapScanNode>;

} // namespace doris