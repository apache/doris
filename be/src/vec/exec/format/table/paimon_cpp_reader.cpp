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

#include "paimon_cpp_reader.h"

#include <algorithm>
#include <mutex>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/table/source/table_read.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exec/format/table/paimon_doris_file_system.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

namespace {
constexpr const char* VALUE_KIND_FIELD = "_VALUE_KIND";

} // namespace

PaimonCppReader::PaimonCppReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 const TFileRangeDesc& range,
                                 const TFileScanRangeParams* range_params)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _range(range),
          _range_params(range_params) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
    if (range.__isset.table_format_params &&
        range.table_format_params.__isset.table_level_row_count) {
        _remaining_table_level_row_count = range.table_format_params.table_level_row_count;
    } else {
        _remaining_table_level_row_count = -1;
    }
}

PaimonCppReader::~PaimonCppReader() = default;

Status PaimonCppReader::init_reader() {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        return Status::OK();
    }
    return _init_paimon_reader();
}

Status PaimonCppReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        auto rows = std::min(_remaining_table_level_row_count,
                             (int64_t)_state->query_options().batch_size);
        _remaining_table_level_row_count -= rows;
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));
        *read_rows = rows;
        *eof = false;
        if (_remaining_table_level_row_count == 0) {
            *eof = true;
        }
        return Status::OK();
    }

    if (!_batch_reader) {
        return Status::InternalError("paimon-cpp reader is not initialized");
    }

    if (_col_name_to_block_idx.empty()) {
        _col_name_to_block_idx = block->get_name_to_pos_map();
    }

    auto batch_result = _batch_reader->NextBatch();
    if (!batch_result.ok()) {
        return Status::InternalError("paimon-cpp read batch failed: {}",
                                     batch_result.status().ToString());
    }
    auto batch = std::move(batch_result).value();
    if (paimon::BatchReader::IsEofBatch(batch)) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> import_result =
            arrow::ImportRecordBatch(batch.first.get(), batch.second.get());
    if (!import_result.ok()) {
        return Status::InternalError("failed to import paimon-cpp arrow batch: {}",
                                     import_result.status().message());
    }

    auto record_batch = std::move(import_result).ValueUnsafe();
    const auto num_rows = static_cast<size_t>(record_batch->num_rows());
    const auto num_columns = record_batch->num_columns();
    for (int c = 0; c < num_columns; ++c) {
        const auto& field = record_batch->schema()->field(c);
        if (field->name() == VALUE_KIND_FIELD) {
            continue;
        }

        auto it = _col_name_to_block_idx.find(field->name());
        if (it == _col_name_to_block_idx.end()) {
            // Skip columns that are not in the block (e.g., partition columns handled elsewhere)
            continue;
        }
        const vectorized::ColumnWithTypeAndName& column_with_name =
                block->get_by_position(it->second);
        try {
            RETURN_IF_ERROR(column_with_name.type->get_serde()->read_column_from_arrow(
                    column_with_name.column->assume_mutable_ref(), record_batch->column(c).get(), 0,
                    num_rows, _ctzz));
        } catch (Exception& e) {
            return Status::InternalError("Failed to convert from arrow to block: {}", e.what());
        }
    }

    *read_rows = num_rows;
    *eof = false;
    return Status::OK();
}

Status PaimonCppReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                    std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status PaimonCppReader::close() {
    if (_batch_reader) {
        _batch_reader->Close();
    }
    return Status::OK();
}

Status PaimonCppReader::_init_paimon_reader() {
    register_paimon_doris_file_system();
    RETURN_IF_ERROR(_decode_split(&_split));

    auto table_path_opt = _resolve_table_path();
    if (!table_path_opt.has_value()) {
        return Status::InternalError(
                "paimon-cpp missing paimon_table; cannot resolve paimon table root path");
    }
    auto options = _build_options();
    auto read_columns = _build_read_columns();

    // Avoid moving strings across module boundaries to prevent allocator mismatches in ASAN builds.
    std::string table_path = table_path_opt.value();
    static std::once_flag options_log_once;
    std::call_once(options_log_once, [&]() {
        auto has_key = [&](const char* key) {
            auto it = options.find(key);
            return (it != options.end() && !it->second.empty()) ? "set" : "empty";
        };
        auto value_or = [&](const char* key) {
            auto it = options.find(key);
            return it != options.end() ? it->second : std::string("<unset>");
        };
        LOG(INFO) << "paimon-cpp options summary: table_path=" << table_path
                  << " AWS_ACCESS_KEY=" << has_key("AWS_ACCESS_KEY")
                  << " AWS_SECRET_KEY=" << has_key("AWS_SECRET_KEY")
                  << " AWS_TOKEN=" << has_key("AWS_TOKEN")
                  << " AWS_ENDPOINT=" << value_or("AWS_ENDPOINT")
                  << " AWS_REGION=" << value_or("AWS_REGION")
                  << " use_path_style=" << value_or("use_path_style")
                  << " fs.oss.endpoint=" << value_or("fs.oss.endpoint")
                  << " fs.s3a.endpoint=" << value_or("fs.s3a.endpoint");
    });
    paimon::ReadContextBuilder builder(table_path);
    if (!read_columns.empty()) {
        builder.SetReadSchema(read_columns);
    }
    if (!options.empty()) {
        builder.SetOptions(options);
    }
    if (_predicate) {
        builder.SetPredicate(_predicate);
        builder.EnablePredicateFilter(true);
    }

    auto context_result = builder.Finish();
    if (!context_result.ok()) {
        return Status::InternalError("paimon-cpp build read context failed: {}",
                                     context_result.status().ToString());
    }
    auto context = std::move(context_result).value();

    auto table_read_result = paimon::TableRead::Create(std::move(context));
    if (!table_read_result.ok()) {
        return Status::InternalError("paimon-cpp create table read failed: {}",
                                     table_read_result.status().ToString());
    }
    auto table_read = std::move(table_read_result).value();
    auto reader_result = table_read->CreateReader(_split);
    if (!reader_result.ok()) {
        return Status::InternalError("paimon-cpp create reader failed: {}",
                                     reader_result.status().ToString());
    }
    _table_read = std::move(table_read);
    _batch_reader = std::move(reader_result).value();
    return Status::OK();
}

Status PaimonCppReader::_decode_split(std::shared_ptr<paimon::Split>* split) {
    if (!_range.__isset.table_format_params || !_range.table_format_params.__isset.paimon_params ||
        !_range.table_format_params.paimon_params.__isset.paimon_split) {
        return Status::InternalError("paimon-cpp missing paimon_split in scan range");
    }
    const auto& encoded_split = _range.table_format_params.paimon_params.paimon_split;
    std::string decoded_split;
    if (!base64_decode(encoded_split, &decoded_split)) {
        return Status::InternalError("paimon-cpp base64 decode paimon_split failed");
    }
    auto pool = paimon::GetDefaultPool();
    auto split_result =
            paimon::Split::Deserialize(decoded_split.data(), decoded_split.size(), pool);
    if (!split_result.ok()) {
        return Status::InternalError("paimon-cpp deserialize split failed: {}",
                                     split_result.status().ToString());
    }
    *split = std::move(split_result).value();
    return Status::OK();
}

std::optional<std::string> PaimonCppReader::_resolve_table_path() const {
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.paimon_params &&
        _range.table_format_params.paimon_params.__isset.paimon_table &&
        !_range.table_format_params.paimon_params.paimon_table.empty()) {
        return _range.table_format_params.paimon_params.paimon_table;
    }
    return std::nullopt;
}

std::vector<std::string> PaimonCppReader::_build_read_columns() const {
    std::vector<std::string> columns;
    columns.reserve(_file_slot_descs.size());
    for (const auto& slot : _file_slot_descs) {
        columns.emplace_back(slot->col_name());
    }
    return columns;
}

std::map<std::string, std::string> PaimonCppReader::_build_options() const {
    std::map<std::string, std::string> options;
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.paimon_params &&
        _range.table_format_params.paimon_params.__isset.paimon_options) {
        options.insert(_range.table_format_params.paimon_params.paimon_options.begin(),
                       _range.table_format_params.paimon_params.paimon_options.end());
    }

    if (_range_params && _range_params->__isset.properties && !_range_params->properties.empty()) {
        for (const auto& kv : _range_params->properties) {
            options[kv.first] = kv.second;
        }
    } else if (_range.__isset.table_format_params &&
               _range.table_format_params.__isset.paimon_params &&
               _range.table_format_params.paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : _range.table_format_params.paimon_params.hadoop_conf) {
            options[kv.first] = kv.second;
        }
    }

    auto copy_if_missing = [&](const char* from_key, const char* to_key) {
        if (options.find(to_key) != options.end()) {
            return;
        }
        auto it = options.find(from_key);
        if (it != options.end() && !it->second.empty()) {
            options[to_key] = it->second;
        }
    };

    // Map common OSS/S3 Hadoop configs to Doris S3 property keys.
    copy_if_missing("fs.oss.accessKeyId", "AWS_ACCESS_KEY");
    copy_if_missing("fs.oss.accessKeySecret", "AWS_SECRET_KEY");
    copy_if_missing("fs.oss.sessionToken", "AWS_TOKEN");
    copy_if_missing("fs.oss.endpoint", "AWS_ENDPOINT");
    copy_if_missing("fs.oss.region", "AWS_REGION");
    copy_if_missing("fs.s3a.access.key", "AWS_ACCESS_KEY");
    copy_if_missing("fs.s3a.secret.key", "AWS_SECRET_KEY");
    copy_if_missing("fs.s3a.session.token", "AWS_TOKEN");
    copy_if_missing("fs.s3a.endpoint", "AWS_ENDPOINT");
    copy_if_missing("fs.s3a.region", "AWS_REGION");
    copy_if_missing("fs.s3a.path.style.access", "use_path_style");

    options[paimon::Options::FILE_SYSTEM] = "doris";
    return options;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
