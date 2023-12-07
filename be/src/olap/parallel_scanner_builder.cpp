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

#include "olap/rowset/beta_rowset.h"
#include "vec/exec/scan/new_olap_scanner.h"

namespace doris {

using namespace vectorized;

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::build_scanners(std::list<VScannerSPtr>& scanners) {
    return _build_scanners_by_rowid(scanners);
}

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_build_scanners_by_rowid(
        std::list<VScannerSPtr>& scanners) {
    size_t total_rows = 0;
    for (auto&& [tablet, _] : _tablets) {
        total_rows += tablet->num_rows();
    }

    size_t rows_per_scanner = total_rows / _max_scanners_count;
    rows_per_scanner = std::max<size_t>(rows_per_scanner, _min_rows_per_scanner);

    for (auto&& [tablet, version] : _tablets) {
        std::vector<RowsetSharedPtr> rowsets;
        RETURN_IF_ERROR(tablet->capture_consistent_rowsets({0, version}, &rowsets));

        TabletReader::ReadSource read_source;

        int64_t rows_collected = 0;
        for (auto& rowset : rowsets) {
            auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowset);
            RowsetReaderSharedPtr reader;
            RETURN_IF_ERROR(beta_rowset->create_reader(&reader));
            const auto rowset_id = beta_rowset->rowset_id();
            auto& segment_cache_handle = _segment_cache_handles[rowset_id];

            RETURN_IF_ERROR(beta_rowset->load());
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(beta_rowset,
                                                                     &segment_cache_handle, true));

            if (beta_rowset->num_rows() == 0) {
                continue;
            }

            const auto& segments = segment_cache_handle.get_segments();
            int segment_start = 0;
            auto split = RowSetSplits(reader->clone());
            size_t total_collected = 0;

            for (size_t i = 0; i != segments.size(); ++i) {
                const auto& segment = segments[i];
                RowRanges row_ranges;
                const size_t rows_of_segment = segment->num_rows();
                int64_t offset_in_segment = 0;

                while (offset_in_segment < rows_of_segment) {
                    const int64_t remaining_rows = rows_of_segment - offset_in_segment;
                    auto rows_need = rows_per_scanner - rows_collected;
                    if (rows_need >= remaining_rows * 0.9) {
                        rows_need = remaining_rows;
                    }
                    DCHECK_LE(rows_need, remaining_rows);

                    auto old_count = row_ranges.count();
                    row_ranges.add({offset_in_segment,
                                    offset_in_segment + static_cast<int64_t>(rows_need)});
                    DCHECK_EQ(rows_need + old_count, row_ranges.count());
                    rows_collected += rows_need;
                    offset_in_segment += rows_need;
                    total_collected += rows_need;

                    if (rows_collected >= rows_per_scanner) { // build a new scanner
                        split.segment_offsets.first = segment_start,
                        split.segment_offsets.second = i + 1;
                        split.segment_row_ranges.emplace_back(std::move(row_ranges));

                        DCHECK_EQ(split.segment_offsets.second - split.segment_offsets.first,
                                  split.segment_row_ranges.size());

                        read_source.rs_splits.emplace_back(std::move(split));

                        scanners.emplace_back(_build_scanner(tablet, version, _key_ranges,
                                                             std::move(read_source)));

                        read_source = TabletReader::ReadSource();
                        split = RowSetSplits(reader->clone());
                        row_ranges = RowRanges();
                        DCHECK(row_ranges.is_empty());
                        DCHECK_EQ(row_ranges.range_size(), 0);

                        segment_start = offset_in_segment < rows_of_segment ? i : i + 1;
                        rows_collected = 0;
                    }
                }

                if (!row_ranges.is_empty()) {
                    DCHECK_GT(rows_collected, 0);
                    DCHECK_EQ(row_ranges.to(), segment->num_rows());
                    split.segment_row_ranges.emplace_back(std::move(row_ranges));
                }
            }
            DCHECK_EQ(total_collected, beta_rowset->num_rows());

            DCHECK_LE(rows_collected, rows_per_scanner);
            if (rows_collected > 0) {
                split.segment_offsets.first = segment_start;
                split.segment_offsets.second = segments.size();
                DCHECK_GT(split.segment_offsets.second, split.segment_offsets.first);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
                read_source.rs_splits.emplace_back(std::move(split));
            }
        } // end `for (auto& rowset : rowsets)`

        DCHECK_LE(rows_collected, rows_per_scanner);
        if (rows_collected > 0) {
            DCHECK_GT(read_source.rs_splits.size(), 0);
            for (auto& split : read_source.rs_splits) {
                DCHECK(split.rs_reader != nullptr);
                DCHECK_LT(split.segment_offsets.first, split.segment_offsets.second);
                DCHECK_EQ(split.segment_row_ranges.size(),
                          split.segment_offsets.second - split.segment_offsets.first);
            }
            scanners.emplace_back(
                    _build_scanner(tablet, version, _key_ranges, std::move(read_source)));
        }
    }

    return Status::OK();
}
template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_load() {
    for (auto&& [tablet, version] : _tablets) {
        const auto tablet_id = tablet->table_id();
        auto& rowsets = _all_rowsets[tablet_id];
        RETURN_IF_ERROR(tablet->capture_consistent_rowsets({0, version}, &rowsets));

        for (auto& rowset : rowsets) {
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();
            auto& segment_cache_handle = _segment_cache_handles[rowset_id];

            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                    std::dynamic_pointer_cast<BetaRowset>(rowset), &segment_cache_handle, true));
        }
    }
}

template <typename ParentType>
std::unique_ptr<SegmentGroup>
ParallelScannerBuilder<ParentType>::_create_segment_group_from_rowsets(
        const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return {};
    }

    size_t max_rows = 0;
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

            }
        }
    }

    auto get_max_rows = [this](Rowset* rowset) -> uint32_t {
        const auto rowset_id = rowset->rowset_id();
        DCHECK(_segment_cache_handles.contains(rowset_id));
        auto& cache_handle = _segment_cache_handles[rowset_id];

        auto& segments = cache_handle.get_segments();
        if (segments.empty()) {
            return 0;
        }

        if (rowset->is_segments_overlapping()) {
            auto* largest_segment = segments[0].get();
            for (size_t i = 1; i != segments.size(); ++i) {
                if (segments[i]->num_rows() > largest_segment->num_rows()) {
                    largest_segment = segments[i].get();
                }
            }
            return largest_segment->num_rows();
        }

        return rowset->num_rows();
    };

    auto* biggest_rowset = rowsets[0].get();
    for (size_t i = 1; i != rowsets.size(); ++i) {
        if (rowsets[i]->num_rows() > biggest_rowset->num_rows()) {
            biggest_rowset = rowsets[i].get();
        }
    }
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

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::_build_scanners_by_key_range(
        std::list<VScannerSPtr>& scanners) {
    return Status::OK();
}

template class ParallelScannerBuilder<NewOlapScanNode>;

} // namespace doris