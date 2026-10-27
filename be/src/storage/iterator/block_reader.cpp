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

#include "storage/iterator/block_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/changelog_block_iterator.h"
#include "storage/iterator/direct_block_iterator.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/sequence_map_block_iterator.h"
#include "storage/iterator/topn_block_iterator.h"
#include "storage/iterator/union_iterator.h"
#include "storage/iterator/unique_key_block_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

BlockReader::BlockReader() : _rows_options(_stats) {}

BlockReader::~BlockReader() = default;

Status BlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    DCHECK(_block_iter != nullptr) << "init() must succeed before reading";
    SCOPED_RAW_TIMER(_merge ? &_stats.collect_iterator_merge_next_timer
                            : &_stats.collect_iterator_normal_next_timer);
    auto res = _block_iter->next_block(block, eof);
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

uint64_t BlockReader::merged_rows() const {
    uint64_t merged_rows = _merged_rows;
    if (_rows_iter != nullptr) {
        merged_rows += _rows_iter->merged_rows();
    }
    for (const auto& rowset_iter : _rowset_iters) {
        merged_rows += rowset_iter->merged_rows();
    }
    return merged_rows;
}

void BlockReader::update_profile(RuntimeProfile* profile) {
    if (_rows_iter != nullptr) {
        _rows_iter->update_profile(profile);
    }
    for (const auto& rowset_iter : _rowset_iters) {
        rowset_iter->update_profile(profile);
    }
}

bool BlockReader::_rowsets_overlap(const ReaderParams& read_params) const {
    std::string pre_rs_last_key;
    bool pre_rs_key_bounds_truncated {false};
    const std::vector<RowSetSplits>& rs_splits = read_params.rs_splits;
    for (const auto& rs_split : rs_splits) {
        if (rs_split.rs_reader->rowset()->num_rows() == 0) {
            continue;
        }
        if (rs_split.rs_reader->rowset()->is_segments_overlapping()) {
            return true;
        }
        std::string rs_first_key;
        bool has_first_key = rs_split.rs_reader->rowset()->first_key(&rs_first_key);
        if (!has_first_key) {
            return true;
        }
        bool cur_rs_key_bounds_truncated {
                rs_split.rs_reader->rowset()->is_segments_key_bounds_truncated()};
        if (!Slice::lhs_is_strictly_less_than_rhs(Slice {pre_rs_last_key},
                                                  pre_rs_key_bounds_truncated, Slice {rs_first_key},
                                                  cur_rs_key_bounds_truncated)) {
            return true;
        }
        bool has_last_key = rs_split.rs_reader->rowset()->last_key(&pre_rs_last_key);
        pre_rs_key_bounds_truncated = cur_rs_key_bounds_truncated;
        CHECK(has_last_key);
    }
    return false;
}

void BlockReader::_init_rows_options(const ReaderParams& read_params) {
    _rows_options.block_row_max = cast_set<uint32_t>(batch_max_rows());
    _rows_options.preferred_block_size_bytes = _reader_context.preferred_block_size_bytes;
    _rows_options.runtime_state = read_params.runtime_state;
    _rows_options.tablet_id = _tablet->tablet_id();
}

Status BlockReader::_new_rows_iter(std::vector<RowwiseIteratorUPtr> children, bool merge) {
    SCOPED_RAW_TIMER(&_stats.block_reader_build_heap_init_timer_ns);
    const MergeDecision& decision = _reader_context.merge;
    if (merge) {
        _rows_iter = new_heap_merge_iterator(std::move(children), _read_schema, decision);
    } else {
        if (decision.reverse) {
            std::ranges::reverse(children);
        }
        _rows_iter = new_union_iterator(std::move(children), _read_schema);
    }
    return _rows_iter->init(_rows_options);
}

Status BlockReader::_init_rows_iter(const ReaderParams& read_params) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }

    // A TopN read (ORDER BY key LIMIT n pushed down) picks its rows per rowset instead of
    // merging; the scanner sets it only for the table models read without a merge.
    const bool topn = _reader_context.read_orderby_key_limit > 0;
    const MergeDecision& decision = _reader_context.merge;
    const bool reverse = decision.reverse;
    bool merge = false;
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_vcollect_iter_init_timer_ns);
        // The rowsets one after another are already in key order when they do not overlap.
        merge = !topn && decision.key_ordered && _rowsets_overlap(read_params);
    }
    _init_rows_options(read_params);

    _merge = merge;

    // The children of the merge or union, in (rowset version, segment) order. The same-key
    // tie-break of the merge is the child order, so the rowsets must come in version order.
    std::vector<RowwiseIteratorUPtr> children;
    RuntimeState* runtime_state = read_params.runtime_state;
    int64_t previous_start_version = -1;
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_rs_readers_init_timer_ns);
        _rs_readers.reserve(read_params.rs_splits.size());
        for (const auto& rs_split : read_params.rs_splits) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }
            DCHECK_GE(rs_split.rs_reader->rowset()->start_version(), previous_start_version);
            previous_start_version = rs_split.rs_reader->rowset()->start_version();
            _rs_readers.push_back(rs_split.rs_reader);
            RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader_context, rs_split));
            if (topn) {
                RowwiseIteratorUPtr rowset_iter;
                RETURN_IF_ERROR(rs_split.rs_reader->new_iterator(&_reader_context, &rowset_iter));
                _rowset_iters.push_back(std::move(rowset_iter));
                continue;
            }
            std::vector<RowwiseIteratorUPtr> segment_iters;
            RETURN_IF_ERROR(
                    rs_split.rs_reader->get_segment_iterators(&_reader_context, &segment_iters));
            if (segment_iters.empty()) {
                continue;
            }
            const bool segments_overlap = rs_split.rs_reader->rowset()->is_segments_overlapping();
            if (merge && segment_iters.size() > 1 && !segments_overlap) {
                // The segments of this rowset are key-disjoint and ordered: one child, read
                // backward for ORDER BY key DESC.
                if (reverse) {
                    std::ranges::reverse(segment_iters);
                }
                children.push_back(new_union_iterator(std::move(segment_iters), _read_schema));
            } else {
                for (auto& segment_iter : segment_iters) {
                    children.push_back(std::move(segment_iter));
                }
            }
        }
    }
    if (topn) {
        if (reverse) {
            std::ranges::reverse(_rowset_iters);
        }
        return Status::OK();
    }
    return _new_rows_iter(std::move(children), merge);
}

// One TabletBlockIterator per (table model, read mode); see tablet_block_iterator.h.
//
//   ORDER BY key LIMIT n pushed down                -> TopNBlockIterator
//   changelog MIN_DELTA scan                      -> ChangelogMinDeltaBlockIterator
//   changelog DETAIL scan                         -> ChangelogDetailBlockIterator
//   direct mode (the scanner asked for no merge)  -> DirectBlockIterator
//   DUP_KEYS                                      -> DirectBlockIterator
//   UNIQUE_KEYS, query on merge-on-write          -> DirectBlockIterator
//   UNIQUE_KEYS, merged by a sequence map         -> SequenceMapBlockIterator
//   UNIQUE_KEYS, other merge-on-read reads        -> UniqueKeyBlockIterator
//   AGG_KEYS                                      -> AggKeyBlockIterator
Status BlockReader::_init_block_iterator(const ReaderParams& read_params,
                                         bool merge_by_sequence_mapping) {
    const OutputBlockLimits limits {.max_rows = batch_max_rows(),
                                    .preferred_bytes = _reader_context.preferred_block_size_bytes};
    const KeysType keys_type = _tablet_schema->keys_type();
    const bool no_merge_model = _direct_mode || keys_type == KeysType::DUP_KEYS ||
                                (keys_type == KeysType::UNIQUE_KEYS &&
                                 read_params.reader_type == ReaderType::READER_QUERY &&
                                 _reader_context.enable_unique_key_merge_on_write);

    if (_reader_context.read_orderby_key_limit > 0) {
        // The TopN rows are whole Blocks picked per rowset, which only the table models
        // read without a merge can use; the scanner sets the limit for those only.
        DORIS_CHECK(no_merge_model);
        _block_iter = std::make_unique<TopNBlockIterator>(&_rowset_iters, &_rows_options,
                                                          &_reader_context);
    } else if (read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA) {
        _block_iter = std::make_unique<ChangelogMinDeltaBlockIterator>(_rows_iter.get(), limits,
                                                                       _read_schema);
    } else if (read_params.changelog_scan_type == TChangelogScanType::DETAIL) {
        _block_iter = std::make_unique<ChangelogDetailBlockIterator>(_rows_iter.get(), limits,
                                                                     _read_schema);
    } else if (no_merge_model) {
        _block_iter = std::make_unique<DirectBlockIterator>(_rows_iter.get());
    } else if (keys_type == KeysType::UNIQUE_KEYS) {
        if (merge_by_sequence_mapping) {
            _block_iter = std::make_unique<SequenceMapBlockIterator>(_rows_iter.get(), limits,
                                                                     _read_schema, &_merged_rows);
        } else {
            int32_t delete_sign_ordinal = -1;
            if (_delete_sign_available) {
                // The reads that may apply the delete sign (compaction, checksum) read the full
                // tablet schema, and every UNIQUE_KEYS schema carries __DORIS_DELETE_SIGN__
                // after its key columns.
                delete_sign_ordinal = _read_schema->delete_sign_ordinal();
                DORIS_CHECK_GT(delete_sign_ordinal, 0);
            }
            _block_iter = std::make_unique<UniqueKeyBlockIterator>(_rows_iter.get(), limits,
                                                                   delete_sign_ordinal, &_stats);
        }
    } else if (keys_type == KeysType::AGG_KEYS) {
        _block_iter = std::make_unique<AggKeyBlockIterator>(_rows_iter.get(), limits, _read_schema,
                                                            read_params.get_be_exec_version(),
                                                            &_merged_rows);
    } else {
        return Status::InternalError("no block iterator for keys type {} of tablet {}",
                                     static_cast<int>(keys_type), _tablet->tablet_id());
    }
    return _block_iter->init();
}

Status BlockReader::init(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_timer_ns);
    RETURN_IF_ERROR(TabletReader::init(read_params));

    // A changelog scan maps the before-image columns; every other read of this reader, which is
    // the one that merges rows across rowsets, builds the sequence mapping instead.
    const bool map_changelog_columns =
            read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA ||
            read_params.changelog_scan_type == TChangelogScanType::DETAIL;
    const bool merge_by_sequence_mapping =
            !map_changelog_columns && _tablet_schema->has_seq_map() &&
            _tablet_schema->keys_type() == UNIQUE_KEYS && !_direct_mode &&
            !(read_params.reader_type == ReaderType::READER_QUERY &&
              _tablet->enable_unique_key_merge_on_write());
    RETURN_IF_ERROR(_read_schema->init_from_tablet_schema(
            *_tablet_schema, merge_by_sequence_mapping, map_changelog_columns));

    auto status = _init_rows_iter(read_params);
    if (!status.ok()) [[unlikely]] {
        if (!config::is_cloud_mode()) {
            static_cast<Tablet*>(_tablet.get())->report_error(status);
        }
        return status;
    }

    status = _init_block_iterator(read_params, merge_by_sequence_mapping);
    if (!status.ok()) [[unlikely]] {
        LOG(WARNING) << "fail to init the block iterator. res:" << status
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return status;
    }
    return Status::OK();
}

} // namespace doris
