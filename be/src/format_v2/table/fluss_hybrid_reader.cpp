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

#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/fluss_jni_reader.h"
#include "format_v2/table/fluss_union_lake_reader.h"

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
    // Forwarded untouched: a lake split's physical format (native Parquet/ORC or a serialized JNI
    // split) is resolved by the paimon stack inside the lake child, and a log range is always JNI.
    return _current_split_reader->prepare_split(options);
}

Status FlussHybridReader::refresh_conjuncts(VExprContextSPtrs conjuncts) {
    RETURN_IF_ERROR(format::TableReader::refresh_conjuncts(std::move(conjuncts)));
    if (_current_split_reader == nullptr) {
        return Status::OK();
    }
    VExprContextSPtrs child_conjuncts;
    RETURN_IF_ERROR(_clone_conjuncts(&child_conjuncts));
    // The hybrid wrapper owns no physical reader; forward a clone so the active child, rather than
    // only the wrapper snapshot, observes late predicates for the remainder of this split.
    return _current_split_reader->refresh_conjuncts(std::move(child_conjuncts));
}

Status FlussHybridReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->get_block(block, eos);
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
    return _current_split_reader->abort_split();
}

Status FlussHybridReader::close() {
    Status close_status = Status::OK();
    if (_log_reader != nullptr) {
        close_status = _log_reader->close();
    }
    if (_lake_reader != nullptr) {
        auto status = _lake_reader->close();
        if (!status.ok() && close_status.ok()) {
            close_status = std::move(status);
        }
    }
    _current_split_reader = nullptr;
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
        return Status::OK();
    }
    if (*range_type == RANGE_TYPE_LOG || *range_type == RANGE_TYPE_PK_FULL ||
        *range_type == RANGE_TYPE_PK_TAIL) {
        if (_log_reader == nullptr) {
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
        return Status::OK();
    }
    return Status::InternalError("fluss scan: a range carries unknown '{}' value '{}'",
                                 PROP_RANGE_TYPE, *range_type);
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
