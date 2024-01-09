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
#include "pipeline/exec/olap_scan_operator.h"
#include "vec/exec/scan/new_olap_scanner.h"

namespace doris {

using namespace vectorized;

template <typename ParentType>
Status ParallelScannerBuilder<ParentType>::build_scanners(std::list<VScannerSPtr>& scanners) {
    RETURN_IF_ERROR(_load());
    if (_is_dup_mow_key) {
        return _build_scanners_by_rowid(scanners);
    } else {
        // TODO: support to split by key range
        return Status::NotSupported("split by key range not supported yet.");
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
    _total_rows = 0;
    for (auto&& [tablet, version] : _tablets) {
        const auto tablet_id = tablet->tablet_id();
        auto& rowsets = _all_rowsets[tablet_id];
        RETURN_IF_ERROR(tablet->capture_consistent_rowsets({0, version}, &rowsets));

        for (auto& rowset : rowsets) {
            RETURN_IF_ERROR(rowset->load());
            const auto rowset_id = rowset->rowset_id();
            auto& segment_cache_handle = _segment_cache_handles[rowset_id];

            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                    std::dynamic_pointer_cast<BetaRowset>(rowset), &segment_cache_handle, true));
            _total_rows += rowset->num_rows();
        }
    }

    _rows_per_scanner = _total_rows / _max_scanners_count;
    _rows_per_scanner = std::max<size_t>(_rows_per_scanner, _min_rows_per_scanner);

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

template class ParallelScannerBuilder<NewOlapScanNode>;
template class ParallelScannerBuilder<pipeline::OlapScanLocalState>;

} // namespace doris