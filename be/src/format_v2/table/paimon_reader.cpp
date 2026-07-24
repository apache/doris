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

#include "format_v2/table/paimon_reader.h"

#include <glog/logging.h>

#include <string>
#include <utility>

#include "exprs/vexpr_context.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/paimon_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/paimon_jni_reader.h"
#include "format_v2/table/schema_history_util.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::paimon {

Status PaimonReader::prepare_split(const format::SplitReadOptions& options) {
    {
        // Derived schema selection is additive to, not nested around, the common base timers.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        _split_schema_id = -1;
        const auto& paimon_params = options.current_range.table_format_params.paimon_params;
        if (paimon_params.__isset.schema_id) {
            _split_schema_id = paimon_params.schema_id;
        }
    }
    RETURN_IF_ERROR(format::TableReader::prepare_split(options));
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.prepare_split_timer);
    if (current_split_pruned()) {
        return Status::OK();
    }
    // Paimon commits data-file changes by adding and logically deleting files in snapshots.
    // Compaction also writes replacement files and commits them in a new snapshot instead of
    // modifying an existing Parquet/ORC file in place. Native Paimon data files are therefore
    // safe to cache by path and size when the split does not provide mtime. Serialized JNI splits
    // do not reach this reader.
    mark_current_data_file_immutable();
    return Status::OK();
}

format::TableColumnMappingMode PaimonReader::mapping_mode() const {
    return format::can_map_by_history_schema(_scan_params, _split_schema_id)
                   ? format::TableColumnMappingMode::BY_FIELD_ID
                   : format::TableColumnMappingMode::BY_NAME;
}

Status PaimonReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (mapping_mode() != format::TableColumnMappingMode::BY_FIELD_ID) {
        return Status::OK();
    }
    return format::annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema);
}

Status PaimonReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                 DeleteFileDesc* desc, bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    const auto& table_desc = t_desc.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }
    const auto& deletion_file = table_desc.deletion_file;
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read));

    desc->key = build_paimon_deletion_vector_cache_key(deletion_file);
    desc->path = deletion_file.path;
    desc->start_offset = deletion_file.offset;
    desc->size = static_cast<int64_t>(bytes_read);
    desc->file_size = -1;
    desc->format = DeleteFileDesc::Format::PAIMON;
    *has_delete_file = true;
    return Status::OK();
}

Status PaimonHybridReader::init(format::TableReadOptions&& options) {
    return format::TableReader::init(std::move(options));
}

Status PaimonHybridReader::prepare_split(const format::SplitReadOptions& options) {
    // Child initialization uses the scanner profile too; hybrid dispatch must not nest the same
    // timer around the first native or JNI child and double-count that initialization.
    RETURN_IF_ERROR(_ensure_current_split_reader(options));
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->prepare_split(options);
}

Status PaimonHybridReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->get_block(block, eos);
}

bool PaimonHybridReader::current_split_pruned() const {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->current_split_pruned();
}

bool PaimonHybridReader::current_split_uses_metadata_count() const {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->current_split_uses_metadata_count();
}

Status PaimonHybridReader::abort_split() {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->abort_split();
}

Status PaimonHybridReader::close() {
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

void PaimonHybridReader::set_batch_size(size_t batch_size) {
    format::TableReader::set_batch_size(batch_size);
    if (_native_reader != nullptr) {
        _native_reader->set_batch_size(_batch_size);
    }
    if (_jni_reader != nullptr) {
        _jni_reader->set_batch_size(_batch_size);
    }
}

int64_t PaimonHybridReader::condition_cache_hit_count() const {
    // Both children survive split switches, so the wrapper must publish their cumulative totals;
    // returning only the active child would make FileScannerV2's monotonic delta go backwards.
    return (_native_reader == nullptr ? 0 : _native_reader->condition_cache_hit_count()) +
           (_jni_reader == nullptr ? 0 : _jni_reader->condition_cache_hit_count());
}

Status PaimonHybridReader::_ensure_current_split_reader(const format::SplitReadOptions& options) {
    if (_is_jni_split(options.current_range)) {
        DCHECK(options.current_split_format == format::FileFormat::JNI);
        if (_jni_reader == nullptr) {
#ifdef BE_TEST
            if (_test_jni_reader_factory) {
                _jni_reader = _test_jni_reader_factory();
            } else {
                _jni_reader = std::make_unique<format::paimon::PaimonJniReader>();
            }
#else
            _jni_reader = std::make_unique<format::paimon::PaimonJniReader>();
#endif
            RETURN_IF_ERROR(_init_child_reader(_jni_reader.get(), format::FileFormat::JNI));
        }
        _current_split_reader = _jni_reader.get();
    } else {
        format::FileFormat file_format;
        RETURN_IF_ERROR(_to_file_format(options.current_range, &file_format));
        // Old FE plans encoded a native file as FORMAT_JNI without paimon_split and carried the
        // physical format only in paimon_params.file_format.
        DCHECK(options.current_split_format == file_format ||
               options.current_split_format == format::FileFormat::JNI);
        DCHECK(file_format == format::FileFormat::PARQUET ||
               file_format == format::FileFormat::ORC);
        if (_native_reader == nullptr) {
#ifdef BE_TEST
            if (_test_native_reader_factory) {
                _native_reader = _test_native_reader_factory();
            } else {
                _native_reader = format::paimon::PaimonReader::create_unique();
            }
#else
            _native_reader = format::paimon::PaimonReader::create_unique();
#endif
            RETURN_IF_ERROR(_init_child_reader(_native_reader.get(), file_format));
        }
        _current_split_reader = _native_reader.get();
    }
    return Status::OK();
}

Status PaimonHybridReader::_init_child_reader(format::TableReader* reader,
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

Status PaimonHybridReader::_clone_conjuncts(VExprContextSPtrs* conjuncts) const {
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

bool PaimonHybridReader::_is_jni_split(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.paimon_params) {
        return false;
    }
    const auto& params = range.table_format_params.paimon_params;
    return params.__isset.paimon_split &&
           (!params.__isset.reader_type || params.reader_type == TPaimonReaderType::PAIMON_JNI);
}

Status PaimonHybridReader::_to_file_format(const TFileRangeDesc& range,
                                           format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    auto format_type =
            range.__isset.format_type ? range.format_type : TFileFormatType::FORMAT_PARQUET;
    // JNI splits also carry file_format metadata; only a split without paimon_split can use
    // FORMAT_JNI as the legacy encoding of a native file.
    if (format_type == TFileFormatType::FORMAT_JNI && !_is_jni_split(range) &&
        range.__isset.table_format_params && range.table_format_params.__isset.paimon_params) {
        const auto& params = range.table_format_params.paimon_params;
        if (params.__isset.file_format && params.file_format == "orc") {
            format_type = TFileFormatType::FORMAT_ORC;
        } else if (params.__isset.file_format && params.file_format == "parquet") {
            format_type = TFileFormatType::FORMAT_PARQUET;
        }
    }
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *file_format = format::FileFormat::PARQUET;
        return Status::OK();
    case TFileFormatType::FORMAT_ORC:
        *file_format = format::FileFormat::ORC;
        return Status::OK();
    default:
        return Status::NotSupported("Unsupported native Paimon file format {}",
                                    to_string(format_type));
    }
}

} // namespace doris::format::paimon
