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

#include <memory>
#include <ostream>
#include <string>

#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/changelog_block_iterator.h"
#include "storage/iterator/direct_block_iterator.h"
#include "storage/iterator/sequence_map_block_iterator.h"
#include "storage/iterator/unique_key_block_iterator.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

BlockReader::~BlockReader() = default;

Status BlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    DCHECK(_block_iter != nullptr) << "init() must succeed before reading";
    auto res = _block_iter->next_block(block, eof);
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

bool BlockReader::_rowsets_not_mono_asc_disjoint(const ReaderParams& read_params) {
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

Status BlockReader::_init_collect_iter(const ReaderParams& read_params) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    // check if rowsets are noneoverlapping
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_vcollect_iter_init_timer_ns);
        const bool is_rowsets_overlapping = _rowsets_not_mono_asc_disjoint(read_params);
        const bool is_min_delta_stream =
                read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA;
        const bool force_merge = read_params.read_orderby_key || is_min_delta_stream;
        const bool is_reverse = !is_min_delta_stream && read_params.read_orderby_key_reverse;
        _vcollect_iter.init(this, is_rowsets_overlapping, force_merge, is_reverse);
    }

    std::vector<RowsetReaderSharedPtr> valid_rs_readers;
    RuntimeState* runtime_state = read_params.runtime_state;

    {
        SCOPED_RAW_TIMER(&_stats.block_reader_rs_readers_init_timer_ns);
        for (const auto& rs_split : read_params.rs_splits) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }

            // _vcollect_iter.topn_next() will init rs_reader by itself
            if (!_vcollect_iter.use_topn_next()) {
                RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader_context, rs_split));
            }

            Status res1 = _vcollect_iter.add_child(rs_split);
            if (!res1.ok() && !res1.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to add child to iterator, err=" << res1;
                return res1;
            }
            if (res1.ok()) {
                valid_rs_readers.push_back(rs_split.rs_reader);
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_build_heap_init_timer_ns);
        RETURN_IF_ERROR(_vcollect_iter.build_heap(valid_rs_readers));
    }

    return Status::OK();
}

// One TabletBlockIterator per (table model, read mode); see tablet_block_iterator.h.
//
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

    if (read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA) {
        _block_iter = std::make_unique<ChangelogMinDeltaBlockIterator>(&_vcollect_iter, limits,
                                                                       _read_schema);
    } else if (read_params.changelog_scan_type == TChangelogScanType::DETAIL) {
        _block_iter = std::make_unique<ChangelogDetailBlockIterator>(&_vcollect_iter, limits,
                                                                     _read_schema);
    } else if (_direct_mode || keys_type == KeysType::DUP_KEYS ||
               (keys_type == KeysType::UNIQUE_KEYS &&
                read_params.reader_type == ReaderType::READER_QUERY &&
                _reader_context.enable_unique_key_merge_on_write)) {
        _block_iter = std::make_unique<DirectBlockIterator>(&_vcollect_iter);
    } else if (keys_type == KeysType::UNIQUE_KEYS) {
        if (merge_by_sequence_mapping) {
            _block_iter = std::make_unique<SequenceMapBlockIterator>(&_vcollect_iter, limits,
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
            _block_iter = std::make_unique<UniqueKeyBlockIterator>(&_vcollect_iter, limits,
                                                                   delete_sign_ordinal, &_stats);
        }
    } else if (keys_type == KeysType::AGG_KEYS) {
        _block_iter = std::make_unique<AggKeyBlockIterator>(&_vcollect_iter, limits, _read_schema,
                                                            read_params.get_be_exec_version(),
                                                            &_merged_rows);
    } else {
        return Status::InternalError("no block iterator for keys type {} of tablet {}",
                                     static_cast<int>(keys_type), _tablet->tablet_id());
    }

    // The scanner-local TopN rows only exist as whole Blocks of VCollectIterator::next(Block*),
    // which is what DirectBlockIterator reads; the scanner sets the TopN limit only for reads
    // of the no-merge table models above.
    DORIS_CHECK(!_vcollect_iter.use_topn_next() ||
                dynamic_cast<DirectBlockIterator*>(_block_iter.get()) != nullptr);

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

    auto status = _init_collect_iter(read_params);
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
