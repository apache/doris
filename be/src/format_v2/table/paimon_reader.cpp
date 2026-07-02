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

#include <cstring>
#include <string>
#include <utility>

#include "exprs/vexpr_context.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/paimon_jni_reader.h"
#include "format_v2/table/schema_history_util.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::paimon {

Status PaimonReader::prepare_split(const format::SplitReadOptions& options) {
    _split_schema_id = -1;
    const auto& paimon_params = options.current_range.table_format_params.paimon_params;
    if (paimon_params.__isset.schema_id) {
        _split_schema_id = paimon_params.schema_id;
    }
    return format::TableReader::prepare_split(options);
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

    const std::string key_prefix = "paimon_dv:";
    desc->key.resize(key_prefix.size() + deletion_file.path.size() + sizeof(deletion_file.offset));
    char* key_data = desc->key.data();
    memcpy(key_data, key_prefix.data(), key_prefix.size());
    key_data += key_prefix.size();
    memcpy(key_data, deletion_file.path.data(), deletion_file.path.size());
    key_data += deletion_file.path.size();
    memcpy(key_data, &deletion_file.offset, sizeof(deletion_file.offset));
    desc->path = deletion_file.path;
    desc->start_offset = deletion_file.offset;
    desc->size = deletion_file.length + 4;
    desc->file_size = -1;
    desc->format = DeleteFileDesc::Format::PAIMON;
    *has_delete_file = true;
    return Status::OK();
}

Status PaimonHybridReader::init(format::TableReadOptions&& options) {
    return format::TableReader::init(std::move(options));
}

Status PaimonHybridReader::prepare_split(const format::SplitReadOptions& options) {
    RETURN_IF_ERROR(_ensure_current_split_reader(options));
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->prepare_split(options);
}

Status PaimonHybridReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_current_split_reader != nullptr);
    return _current_split_reader->get_block(block, eos);
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

Status PaimonHybridReader::_ensure_current_split_reader(const format::SplitReadOptions& options) {
    if (_is_jni_split(options.current_range)) {
        DCHECK(options.current_split_format == format::FileFormat::JNI);
        if (_jni_reader == nullptr) {
            _jni_reader = std::make_unique<format::paimon::PaimonJniReader>();
            RETURN_IF_ERROR(_init_child_reader(_jni_reader.get(), format::FileFormat::JNI));
        }
        _current_split_reader = _jni_reader.get();
    } else {
        format::FileFormat file_format;
        RETURN_IF_ERROR(_to_file_format(options.current_range, &file_format));
        DCHECK(options.current_split_format == file_format);
        DCHECK(file_format == format::FileFormat::PARQUET ||
               file_format == format::FileFormat::ORC);
        if (_native_reader == nullptr) {
            _native_reader = format::paimon::PaimonReader::create_unique();
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
    return reader->init({
            .projected_columns = _projected_columns,
            .column_predicates = _table_column_predicates,
            .conjuncts = std::move(conjuncts),
            .format = file_format,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
            .push_down_agg_type = _push_down_agg_type,
            .condition_cache_digest = _condition_cache_digest,
    });
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
    return range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
           range.table_format_params.paimon_params.__isset.reader_type &&
           range.table_format_params.paimon_params.reader_type == TPaimonReaderType::PAIMON_JNI;
}

Status PaimonHybridReader::_to_file_format(const TFileRangeDesc& range,
                                           format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    const auto format_type =
            range.__isset.format_type ? range.format_type : TFileFormatType::FORMAT_PARQUET;
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
