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

#include "format_v2/table/fluss_hybrid_reader.h"

#include <string>
#include <utility>

#include "common/cast_set.h"
#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/fluss_jni_reader.h"
#include "format_v2/table/fluss_union_lake_reader.h"
#include "runtime/file_scan_profile.h"

namespace doris::format::fluss {
namespace {

// The per-range dispatch key and its values, exactly as FE writes them. LOG, PK_FULL and PK_TAIL
// are ranges of fluss's own log; LAKE and LAKE_SUPPRESS are the sibling's lake splits, the latter
// with a log tail bound to it.
constexpr const char* PROP_RANGE_TYPE = "fluss.range_type";
constexpr const char* RANGE_TYPE_LOG = "LOG";
constexpr const char* RANGE_TYPE_PK_FULL = "PK_FULL";
constexpr const char* RANGE_TYPE_PK_TAIL = "PK_TAIL";
constexpr const char* RANGE_TYPE_LAKE = "LAKE";
constexpr const char* RANGE_TYPE_LAKE_SUPPRESS = "LAKE_SUPPRESS";

// This reader's own profile lines, all under the shared TableReader layer. A side's timer is the
// display parent of everything belonging to that side, so a profile reader sees which half of the
// scan the time went to before seeing anything finer.
constexpr const char* METRIC_LOG_READ_TIME = "FlussLogReadTime";
constexpr const char* METRIC_LOG_ROWS_RETURNED = "FlussLogRowsReturned";
constexpr const char* METRIC_LAKE_READ_TIME = "FlussLakeReadTime";
constexpr const char* METRIC_LAKE_ROWS_RETURNED = "FlussLakeRowsReturned";

// One stem per kind of range, shared by that kind's count and its timer.
constexpr const char* METRIC_STEM_LOG = "FlussLogRange";
constexpr const char* METRIC_STEM_PK_FULL = "FlussPkFullRange";
constexpr const char* METRIC_STEM_PK_TAIL = "FlussPkTailRange";
constexpr const char* METRIC_STEM_LAKE = "FlussLakeRange";
constexpr const char* METRIC_STEM_LAKE_SUPPRESS = "FlussLakeSuppressRange";

void update_counter(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
    }
}

const std::string* range_type_of(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.fluss_params) {
        return nullptr;
    }
    const auto& params = range.table_format_params.fluss_params;
    const auto it = params.find(PROP_RANGE_TYPE);
    return it == params.end() ? nullptr : &it->second;
}

} // namespace

bool FlussHybridReader::is_lake_range(const TFileRangeDesc& range) {
    const auto* range_type = range_type_of(range);
    return range_type != nullptr &&
           (*range_type == RANGE_TYPE_LAKE || *range_type == RANGE_TYPE_LAKE_SUPPRESS);
}

Status FlussHybridReader::init(format::TableReadOptions&& options) {
    return format::TableReader::init(std::move(options));
}

Status FlussHybridReader::prepare_split(const format::SplitReadOptions& options) {
    RETURN_IF_ERROR(_ensure_current_split_reader(options));
    DORIS_CHECK(_current_split_reader != nullptr);
    DORIS_CHECK(_current_side != nullptr && _current_type != nullptr);
    // Two different counters, so one scope may hold both: the side's total and this kind of range's
    // share of it. Building the child was timed inside _ensure_current_split_reader, whose scope has
    // already closed, so the side's timer counts that once rather than twice.
    SCOPED_TIMER(_current_side->read_time);
    SCOPED_TIMER(_current_type->read_time);
    // Forwarded untouched: a lake split's physical format (native Parquet/ORC or a serialized JNI
    // split) is resolved by the paimon stack inside the lake child, and a log range is always JNI.
    return _current_split_reader->prepare_split(options);
}

Status FlussHybridReader::refresh_conjuncts(VExprContextSPtrs conjuncts) {
    RETURN_IF_ERROR(format::TableReader::refresh_conjuncts(std::move(conjuncts)));
    if (_current_split_reader == nullptr) {
        return Status::OK();
    }
    DORIS_CHECK(_current_side != nullptr);
    // The side's alone: a filter arriving mid-split is handed to whichever child is active, but the
    // work of handing it over belongs to no one range of that child.
    SCOPED_TIMER(_current_side->read_time);
    VExprContextSPtrs child_conjuncts;
    RETURN_IF_ERROR(_clone_conjuncts(&child_conjuncts));
    // The hybrid wrapper owns no physical reader; forward a clone so the active child, rather than
    // only the wrapper snapshot, observes late predicates for the remainder of this split.
    return _current_split_reader->refresh_conjuncts(std::move(child_conjuncts));
}

Status FlussHybridReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_current_split_reader != nullptr);
    DORIS_CHECK(_current_side != nullptr && _current_type != nullptr);
    SCOPED_TIMER(_current_side->read_time);
    SCOPED_TIMER(_current_type->read_time);
    RETURN_IF_ERROR(_current_split_reader->get_block(block, eos));
    // What this side actually hands on - for the lake side already net of the rows its log tail
    // superseded. Time alone cannot tell a slow side from the side that holds most of the rows.
    update_counter(_current_side->rows_returned, cast_set<int64_t>(block->rows()));
    return Status::OK();
}

bool FlussHybridReader::current_split_pruned() const {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->current_split_pruned();
}

bool FlussHybridReader::current_split_uses_metadata_count() const {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->current_split_uses_metadata_count();
}

Status FlussHybridReader::abort_split() {
    DORIS_CHECK(_current_split_reader != nullptr);
    DORIS_CHECK(_current_side != nullptr && _current_type != nullptr);
    // Abandoning a split is still that split's cost, so it lands where its reads did.
    SCOPED_TIMER(_current_side->read_time);
    SCOPED_TIMER(_current_type->read_time);
    return _current_split_reader->abort_split();
}

Status FlussHybridReader::close() {
    Status close_status = Status::OK();
    if (_log_reader != nullptr) {
        // Each child's own side, and no range's: at this point the current kind is merely whichever
        // range arrived last, and charging the teardown to it would misread as that kind being slow.
        SCOPED_TIMER(_log_metrics.read_time);
        close_status = _log_reader->close();
    }
    if (_lake_reader != nullptr) {
        SCOPED_TIMER(_lake_metrics.read_time);
        auto status = _lake_reader->close();
        if (!status.ok() && close_status.ok()) {
            close_status = std::move(status);
        }
    }
    _current_split_reader = nullptr;
    _current_side = nullptr;
    _current_type = nullptr;
    return close_status;
}

void FlussHybridReader::set_batch_size(size_t batch_size) {
    format::TableReader::set_batch_size(batch_size);
    if (_log_reader != nullptr) {
        _log_reader->set_batch_size(_batch_size);
    }
    if (_lake_reader != nullptr) {
        _lake_reader->set_batch_size(_batch_size);
    }
}

int64_t FlussHybridReader::condition_cache_hit_count() const {
    // Both children survive split switches, so the wrapper must publish their cumulative totals;
    // returning only the active child would make FileScannerV2's monotonic delta go backwards.
    return (_log_reader == nullptr ? 0 : _log_reader->condition_cache_hit_count()) +
           (_lake_reader == nullptr ? 0 : _lake_reader->condition_cache_hit_count());
}

Status FlussHybridReader::_ensure_current_split_reader(const format::SplitReadOptions& options) {
    const auto* range_type = range_type_of(options.current_range);
    if (range_type == nullptr) {
        return Status::InternalError(
                "missing '{}' on a fluss range, possibly caused by FE/BE protocol mismatch",
                PROP_RANGE_TYPE);
    }
    if (*range_type == RANGE_TYPE_LAKE || *range_type == RANGE_TYPE_LAKE_SUPPRESS) {
        if (_lake_reader == nullptr) {
            _lake_metrics =
                    _register_side_metrics(METRIC_LAKE_READ_TIME, METRIC_LAKE_ROWS_RETURNED);
            // Standing the child up is the side's cost and no single range's: the paimon stack is
            // built once per scan, and what the side's total exceeds its ranges' by is exactly that.
            SCOPED_TIMER(_lake_metrics.read_time);
#ifdef BE_TEST
            if (_test_lake_reader_factory) {
                _lake_reader = _test_lake_reader_factory();
            } else {
                _lake_reader = std::make_unique<FlussUnionLakeReader>();
            }
#else
            _lake_reader = std::make_unique<FlussUnionLakeReader>();
#endif
            RETURN_IF_ERROR(_init_child_reader(_lake_reader.get(), options.current_split_format));
        }
        _current_split_reader = _lake_reader.get();
        _current_side = &_lake_metrics;
        _current_type = _count_range_of_type(
                *range_type,
                *range_type == RANGE_TYPE_LAKE ? METRIC_STEM_LAKE : METRIC_STEM_LAKE_SUPPRESS,
                METRIC_LAKE_READ_TIME);
        return Status::OK();
    }
    if (*range_type == RANGE_TYPE_LOG || *range_type == RANGE_TYPE_PK_FULL ||
        *range_type == RANGE_TYPE_PK_TAIL) {
        if (_log_reader == nullptr) {
            _log_metrics = _register_side_metrics(METRIC_LOG_READ_TIME, METRIC_LOG_ROWS_RETURNED);
            // As above, and here the one-off cost is loading the JNI scanner's classes.
            SCOPED_TIMER(_log_metrics.read_time);
#ifdef BE_TEST
            if (_test_log_reader_factory) {
                _log_reader = _test_log_reader_factory();
            } else {
                _log_reader = std::make_unique<FlussJniReader>();
            }
#else
            _log_reader = std::make_unique<FlussJniReader>();
#endif
            RETURN_IF_ERROR(_init_child_reader(_log_reader.get(), format::FileFormat::JNI));
        }
        _current_split_reader = _log_reader.get();
        _current_side = &_log_metrics;
        const char* stem = METRIC_STEM_PK_TAIL;
        if (*range_type == RANGE_TYPE_LOG) {
            stem = METRIC_STEM_LOG;
        } else if (*range_type == RANGE_TYPE_PK_FULL) {
            stem = METRIC_STEM_PK_FULL;
        }
        _current_type = _count_range_of_type(*range_type, stem, METRIC_LOG_READ_TIME);
        return Status::OK();
    }
    return Status::InternalError("fluss scan: a range carries unknown '{}' value '{}'",
                                 PROP_RANGE_TYPE, *range_type);
}

FlussHybridReader::SideMetrics FlussHybridReader::_register_side_metrics(
        const char* read_time_name, const char* rows_returned_name) const {
    if (_scanner_profile == nullptr) {
        return {};
    }
    SideMetrics metrics;
    metrics.read_time = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, read_time_name,
                                                   file_scan_profile::TABLE_READER, 1);
    metrics.rows_returned = ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, rows_returned_name,
                                                         TUnit::UNIT, read_time_name, 1);
    return metrics;
}

FlussHybridReader::RangeTypeMetrics* FlussHybridReader::_count_range_of_type(
        const std::string& range_type, const char* metric_stem, const char* side_timer_name) {
    auto it = _range_type_metrics.find(range_type);
    if (it == _range_type_metrics.end()) {
        // Registered on first sight rather than up front, so that the kinds a scan never read leave
        // no line behind: a zero on a kind that cannot occur for this table reads as information.
        RangeTypeMetrics metrics;
        if (_scanner_profile != nullptr) {
            metrics.num =
                    ADD_CHILD_COUNTER_WITH_LEVEL(_scanner_profile, std::string(metric_stem) + "Num",
                                                 TUnit::UNIT, side_timer_name, 1);
            metrics.read_time = ADD_CHILD_TIMER_WITH_LEVEL(
                    _scanner_profile, std::string(metric_stem) + "ReadTime", side_timer_name, 1);
        }
        it = _range_type_metrics.emplace(range_type, metrics).first;
    }
    update_counter(it->second.num, 1);
    return &it->second;
}

Status FlussHybridReader::_init_child_reader(format::TableReader* reader,
                                             format::FileFormat file_format) {
    DORIS_CHECK(reader != nullptr);
    VExprContextSPtrs conjuncts;
    RETURN_IF_ERROR(_clone_conjuncts(&conjuncts));
    RETURN_IF_ERROR(reader->init({
            .projected_columns = _projected_columns,
            .conjuncts = std::move(conjuncts),
            .format = file_format,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
            .file_slot_descs = _file_slot_descs,
            .push_down_agg_type = _push_down_agg_type,
            .push_down_count_columns = _push_down_count_columns,
            .condition_cache_digest = _condition_cache_digest,
    }));
    // Zero means no adaptive prediction has been produced yet. Preserve the child's normal
    // runtime default until FileScannerV2 supplies the first positive prediction.
    if (_batch_size > 0) {
        reader->set_batch_size(_batch_size);
    }
    return Status::OK();
}

Status FlussHybridReader::_clone_conjuncts(VExprContextSPtrs* conjuncts) const {
    DORIS_CHECK(conjuncts != nullptr);
    conjuncts->clear();
    conjuncts->reserve(_conjuncts.size());
    for (const auto& conjunct : _conjuncts) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        conjuncts->push_back(VExprContext::create_shared(std::move(root)));
    }
    return Status::OK();
}

} // namespace doris::format::fluss
