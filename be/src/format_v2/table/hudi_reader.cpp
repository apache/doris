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

#include "format_v2/table/hudi_reader.h"

#include <utility>

#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/hudi_jni_reader.h"
#include "format_v2/table/schema_history_util.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::hudi {

Status HudiReader::prepare_split(const format::SplitReadOptions& options) {
    _split_schema_id = -1;
    if (options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.__isset.hudi_params &&
        options.current_range.table_format_params.hudi_params.__isset.schema_id) {
        _split_schema_id = options.current_range.table_format_params.hudi_params.schema_id;
    }
    return format::TableReader::prepare_split(options);
}

format::TableColumnMappingMode HudiReader::mapping_mode() const {
    return format::can_map_by_history_schema(_scan_params, _split_schema_id)
                   ? format::TableColumnMappingMode::BY_FIELD_ID
                   : format::TableColumnMappingMode::BY_NAME;
}

Status HudiReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (mapping_mode() != format::TableColumnMappingMode::BY_FIELD_ID) {
        return Status::OK();
    }
    return format::annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema);
}

Status HudiHybridReader::init(format::TableReadOptions&& options) {
    return format::TableReader::init(std::move(options));
}

Status HudiHybridReader::prepare_split(const format::SplitReadOptions& options) {
    RETURN_IF_ERROR(_ensure_current_split_reader(options));
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->prepare_split(options);
}

Status HudiHybridReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->get_block(block, eos);
}

bool HudiHybridReader::current_split_pruned() const {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->current_split_pruned();
}

Status HudiHybridReader::abort_split() {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->abort_split();
}

Status HudiHybridReader::close() {
    Status close_status = Status::OK();
    if (_native_reader != nullptr) {
        close_status = _native_reader->close();
    }
    if (_jni_reader != nullptr) {
        auto status = _jni_reader->close();
        if (!status.ok() && close_status.ok()) {
            close_status = std::move(status);
        }
    }
    _current_split_reader = nullptr;
    return close_status;
}

void HudiHybridReader::set_batch_size(size_t batch_size) {
    format::TableReader::set_batch_size(batch_size);
    if (_native_reader != nullptr) {
        _native_reader->set_batch_size(_batch_size);
    }
    if (_jni_reader != nullptr) {
        _jni_reader->set_batch_size(_batch_size);
    }
}

Status HudiHybridReader::_ensure_current_split_reader(const format::SplitReadOptions& options) {
    DORIS_CHECK(_scan_params != nullptr);
    if (_is_jni_split(*_scan_params, options.current_range)) {
        if (_jni_reader == nullptr) {
            _jni_reader = std::make_unique<format::hudi::HudiJniReader>();
            RETURN_IF_ERROR(_init_child_reader(_jni_reader.get(), format::FileFormat::JNI));
        }
        _current_split_reader = _jni_reader.get();
    } else {
        format::FileFormat file_format;
        RETURN_IF_ERROR(_to_file_format(*_scan_params, options.current_range, &file_format));
        if (_native_reader == nullptr) {
            _native_reader = format::hudi::HudiReader::create_unique();
            RETURN_IF_ERROR(_init_child_reader(_native_reader.get(), file_format));
        }
        _current_split_reader = _native_reader.get();
    }
    return Status::OK();
}

Status HudiHybridReader::_init_child_reader(format::TableReader* reader,
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
            .push_down_agg_type = _push_down_agg_type,
            .condition_cache_digest = _condition_cache_digest,
    }));
    // Zero means no adaptive prediction has been produced yet. Preserve the child's normal
    // runtime default until FileScannerV2 supplies the first positive prediction.
    if (_batch_size > 0) {
        reader->set_batch_size(_batch_size);
    }
    return Status::OK();
}

Status HudiHybridReader::_clone_conjuncts(VExprContextSPtrs* conjuncts) const {
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

TFileFormatType::type HudiHybridReader::_range_format_type(const TFileScanRangeParams& params,
                                                           const TFileRangeDesc& range) {
    return range.__isset.format_type ? range.format_type : params.format_type;
}

bool HudiHybridReader::_is_jni_split(const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range) {
    return _range_format_type(params, range) == TFileFormatType::FORMAT_JNI;
}

Status HudiHybridReader::_to_file_format(const TFileScanRangeParams& params,
                                         const TFileRangeDesc& range,
                                         format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    const auto format_type = _range_format_type(params, range);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *file_format = format::FileFormat::PARQUET;
        return Status::OK();
    case TFileFormatType::FORMAT_ORC:
        *file_format = format::FileFormat::ORC;
        return Status::OK();
    default:
        return Status::NotSupported("Unsupported native Hudi file format {}",
                                    to_string(format_type));
    }
}

} // namespace doris::format::hudi
