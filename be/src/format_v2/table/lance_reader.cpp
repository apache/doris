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

#include "format_v2/table/lance_reader.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <lance/lance.h>

#include <memory>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris::format::lance {
namespace {

struct LanceScannerDeleter {
    void operator()(LanceScanner* scanner) const { lance_scanner_close(scanner); }
};

struct LanceBatchDeleter {
    void operator()(LanceBatch* batch) const { lance_batch_free(batch); }
};

} // namespace

std::string LanceTableReader::DatasetKey::debug_string() const {
    std::ostringstream out;
    out << "{uri=" << uri << ", version=" << version
        << ", storage_option_entries=" << storage_options.size() << "}";
    return out.str();
}

LanceTableReader::~LanceTableReader() {
    static_cast<void>(close());
}

Status LanceTableReader::init(format::TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    RETURN_IF_ERROR(_build_column_name_to_output_index());
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctz);
    RowDescriptor row_desc;
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    return Status::OK();
}

Status LanceTableReader::_build_column_name_to_output_index() {
    _column_name_to_output_index.clear();
    _column_name_to_output_index.reserve(_projected_columns.size());
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        const auto& column = _projected_columns[idx];
        const auto emplace_result = _column_name_to_output_index.emplace(column.name, idx);
        if (!emplace_result.second) {
            return Status::InvalidArgument("Lance reader received duplicate projected column '{}'",
                                           column.name);
        }
    }
    return Status::OK();
}

Status LanceTableReader::prepare_split(const format::SplitReadOptions& options) {
    _close_scanner();
    _closed = false;
    _eof = false;
    RETURN_IF_ERROR(TableReader::prepare_split(options));
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_validate_current_split());
    RETURN_IF_ERROR(_ensure_dataset_open());
    RETURN_IF_ERROR(_open_scanner());
    return Status::OK();
}

Status LanceTableReader::get_block(Block* output_block, bool* eos) {
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(output_block->columns() == _projected_columns.size());
    output_block->clear_column_data(_projected_columns.size());

    if (_is_table_level_count_active()) {
        return _read_table_level_count(output_block, eos);
    }
    DORIS_CHECK(_scanner_opened);
    if (_eof) {
        *eos = true;
        return Status::OK();
    }

    while (true) {
        RETURN_IF_ERROR(_read_next_lance_block(output_block, eos));
        if (*eos || output_block->rows() > 0) {
            return Status::OK();
        }
        output_block->clear_column_data(_projected_columns.size());
    }
}

Status LanceTableReader::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    _close_scanner();
    _close_dataset();
    return Status::OK();
}

std::string LanceTableReader::debug_string() const {
    std::ostringstream out;
    out << "LanceTableReader{base=" << TableReader::debug_string()
        << ", dataset_uri=" << _dataset_uri() << ", scanner_opened=" << _scanner_opened
        << ", dataset_opened=" << (_dataset != nullptr)
        << ", eof=" << _eof << ", column_count=" << _projected_columns.size() << "}";
    return out.str();
}

Status LanceTableReader::_ensure_dataset_open() {
    const auto key = _current_dataset_key();
    DORIS_CHECK(!key.uri.empty());
    if (_dataset != nullptr) {
        DORIS_CHECK(_dataset_key.has_value());
        if (!_dataset_key_equal(*_dataset_key, key)) {
            return Status::InternalError(
                    "Lance reader received split from a different dataset. expected={}, actual={}",
                    _dataset_key->debug_string(), key.debug_string());
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(_open_dataset(key));
    _dataset_key = key;
    return Status::OK();
}

Status LanceTableReader::_open_dataset(const DatasetKey& key) {
    DORIS_CHECK(_dataset == nullptr);

    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(key.storage_options.size() + 1);
    for (const auto& value : key.storage_options) {
        storage_option_ptrs.push_back(value.c_str());
    }
    storage_option_ptrs.push_back(nullptr);
    const char* const* storage_options_arg =
            key.storage_options.empty() ? nullptr : storage_option_ptrs.data();

    _dataset = lance_dataset_open(key.uri.c_str(), storage_options_arg, key.version);
    if (_dataset == nullptr) {
        return _lance_error("open Lance dataset");
    }
    return Status::OK();
}

Status LanceTableReader::_open_scanner() {
    DORIS_CHECK(_dataset != nullptr);
    DORIS_CHECK(_scanner == nullptr);
    std::vector<const char*> column_ptrs;
    column_ptrs.reserve(_projected_columns.size() + 1);
    for (const auto& column : _projected_columns) {
        column_ptrs.push_back(column.name.c_str());
    }
    column_ptrs.push_back(nullptr);
    const char* const* columns = _projected_columns.empty() ? nullptr : column_ptrs.data();
    LanceScanner* raw_scanner = lance_scanner_new(_dataset, columns, nullptr);
    if (raw_scanner == nullptr) {
        return _lance_error("create Lance scanner");
    }

    std::unique_ptr<LanceScanner, LanceScannerDeleter> scanner_guard(raw_scanner);
    if (lance_scanner_set_batch_size(raw_scanner,
                                     cast_set<int64_t>(_runtime_state->batch_size())) != 0) {
        return _lance_error("set Lance scanner batch size");
    }
    std::vector<uint64_t> fragment_ids;
    RETURN_IF_ERROR(_fragment_ids(&fragment_ids));
    DORIS_CHECK(!fragment_ids.empty());
    if (lance_scanner_set_fragment_ids(raw_scanner, fragment_ids.data(), fragment_ids.size()) !=
        0) {
        return _lance_error("set Lance scanner fragment ids");
    }

    _scanner = scanner_guard.release();
    _scanner_opened = true;
    return Status::OK();
}

Status LanceTableReader::_validate_current_split() const {
    if (!_current_file_range_desc.__isset.table_format_params) {
        return Status::InvalidArgument("Lance reader requires table format params");
    }
    if (!_current_file_range_desc.table_format_params.__isset.lance_params) {
        return Status::InvalidArgument("Lance reader requires lance params");
    }
    const auto& lance_params = _current_file_range_desc.table_format_params.lance_params;
    if (!lance_params.__isset.dataset_uri || lance_params.dataset_uri.empty()) {
        return Status::InvalidArgument("Lance reader requires dataset URI");
    }
    if (!lance_params.__isset.version) {
        return Status::InvalidArgument("Lance reader requires dataset version");
    }
    if (lance_params.version < 0) {
        return Status::InvalidArgument("Lance dataset version must be non-negative: {}",
                                       lance_params.version);
    }
    if (!lance_params.__isset.fragment_ids || lance_params.fragment_ids.empty()) {
        return Status::InvalidArgument("Lance reader requires non-empty fragment ids for each split");
    }
    for (const auto id : lance_params.fragment_ids) {
        if (id < 0) {
            return Status::InvalidArgument("Lance fragment id must be non-negative: {}", id);
        }
    }
    return Status::OK();
}

Status LanceTableReader::_read_next_lance_block(Block* output_block, bool* eos) {
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(_scanner != nullptr);

    *eos = false;
    LanceBatch* raw_batch = nullptr;
    const int32_t next_rc = lance_scanner_next(_scanner, &raw_batch);
    if (next_rc == 1) {
        _eof = true;
        _current_reader_reached_eof = true;
        _close_scanner();
        *eos = true;
        return Status::OK();
    }
    if (next_rc != 0) {
        return _lance_error("read next Lance batch");
    }
    DORIS_CHECK(raw_batch != nullptr);
    std::unique_ptr<LanceBatch, LanceBatchDeleter> batch(raw_batch);

    ArrowSchema c_schema {};
    ArrowArray c_array {};
    if (lance_batch_to_arrow(batch.get(), &c_array, &c_schema) != 0) {
        return _lance_error("export Lance batch to Arrow");
    }

    auto import_result = arrow::ImportRecordBatch(&c_array, &c_schema);
    if (!import_result.ok()) {
        if (c_array.release != nullptr) {
            c_array.release(&c_array);
        }
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
        return Status::InternalError("Failed to import Lance Arrow batch: {}",
                                     import_result.status().message());
    }

    auto record_batch = std::move(import_result).ValueUnsafe();
    size_t rows = 0;
    RETURN_IF_ERROR(_fill_output_block(*record_batch, output_block, &rows));
    DORIS_CHECK(output_block->rows() == rows);
    if (!_conjuncts.empty()) {
        RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block,
                                                   output_block->columns()));
    }
    return Status::OK();
}

Status LanceTableReader::_fill_output_block(const arrow::RecordBatch& batch, Block* output_block,
                                            size_t* rows) const {
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(output_block->columns() == _projected_columns.size());
    std::vector<bool> materialized_columns(output_block->columns(), false);
    auto columns_guard = output_block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int arrow_idx = 0; arrow_idx < batch.num_columns(); ++arrow_idx) {
        const auto& field = batch.schema()->field(arrow_idx);
        const auto output_it = _column_name_to_output_index.find(field->name());
        if (output_it == _column_name_to_output_index.end()) {
            if (!_projected_columns.empty()) {
                return Status::InternalError("Lance returned unknown column '{}'", field->name());
            }
            continue;
        }
        const auto output_index = output_it->second;
        DORIS_CHECK(output_index < output_block->columns());
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(output_index)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[output_index],
                                                             batch.column(arrow_idx).get(), 0,
                                                             batch.num_rows(), _ctz));
        } catch (const Exception& e) {
            return Status::InternalError(
                    "Failed to convert Lance Arrow column '{}' to Doris block: {}", field->name(),
                    e.what());
        }
        materialized_columns[output_index] = true;
    }
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        if (!materialized_columns[idx]) {
            return Status::InternalError("Lance did not return requested column '{}'",
                                         _projected_columns[idx].name);
        }
    }
    *rows = cast_set<size_t>(batch.num_rows());
    return Status::OK();
}

void LanceTableReader::_close_scanner() {
    if (_scanner != nullptr) {
        lance_scanner_close(_scanner);
        _scanner = nullptr;
    }
    _scanner_opened = false;
}

void LanceTableReader::_close_dataset() {
    if (_dataset != nullptr) {
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
    _dataset_key.reset();
}

LanceTableReader::DatasetKey LanceTableReader::_current_dataset_key() const {
    return DatasetKey {
            .uri = _dataset_uri(),
            .version = _dataset_version(),
            .storage_options = _storage_option_values(),
    };
}

std::string LanceTableReader::_dataset_uri() const {
    DORIS_CHECK(_current_file_range_desc.__isset.table_format_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.__isset.lance_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.lance_params.__isset.dataset_uri);
    DORIS_CHECK(!_current_file_range_desc.table_format_params.lance_params.dataset_uri.empty());
    return _current_file_range_desc.table_format_params.lance_params.dataset_uri;
}

uint64_t LanceTableReader::_dataset_version() const {
    DORIS_CHECK(_current_file_range_desc.__isset.table_format_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.__isset.lance_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.lance_params.__isset.version);
    const auto version = _current_file_range_desc.table_format_params.lance_params.version;
    DORIS_CHECK(version >= 0);
    if (version == 0) {
        return 0;
    }
    return cast_set<uint64_t>(version);
}

Status LanceTableReader::_fragment_ids(std::vector<uint64_t>* ids) const {
    DORIS_CHECK(ids != nullptr);
    DORIS_CHECK(_current_file_range_desc.__isset.table_format_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.__isset.lance_params);
    DORIS_CHECK(_current_file_range_desc.table_format_params.lance_params.__isset.fragment_ids);
    DORIS_CHECK(!_current_file_range_desc.table_format_params.lance_params.fragment_ids.empty());
    ids->clear();
    ids->reserve(_current_file_range_desc.table_format_params.lance_params.fragment_ids.size());
    for (const auto id : _current_file_range_desc.table_format_params.lance_params.fragment_ids) {
        DORIS_CHECK(id >= 0);
        ids->push_back(cast_set<uint64_t>(id));
    }
    return Status::OK();
}

std::vector<std::string> LanceTableReader::_storage_option_values() const {
    std::vector<std::string> values;
    if (_scan_params == nullptr || !_scan_params->__isset.properties) {
        return values;
    }

    static const std::vector<std::pair<std::string, std::string>> s3_key_mapping = {
            {"AWS_ACCESS_KEY", "aws_access_key_id"},
            {"AWS_SECRET_KEY", "aws_secret_access_key"},
            {"AWS_TOKEN", "aws_session_token"},
            {"AWS_ENDPOINT", "aws_endpoint"},
            {"AWS_REGION", "aws_region"},
    };

    values.reserve(_scan_params->properties.size() * 2);
    for (const auto& [key, value] : _scan_params->properties) {
        auto mapped_key = key;
        const auto mapping_it = std::ranges::find_if(
                s3_key_mapping, [&](const auto& mapping) { return mapping.first == key; });
        if (mapping_it != s3_key_mapping.end()) {
            mapped_key = mapping_it->second;
        }
        values.push_back(std::move(mapped_key));
        values.push_back(value);
    }
    return values;
}

bool LanceTableReader::_dataset_key_equal(const DatasetKey& lhs, const DatasetKey& rhs) {
    return lhs.uri == rhs.uri && lhs.version == rhs.version &&
           lhs.storage_options == rhs.storage_options;
}

Status LanceTableReader::_lance_error(std::string_view operation) {
    const auto code = lance_last_error_code();
    const char* raw_message = lance_last_error_message();
    std::string message = raw_message == nullptr ? "" : raw_message;
    if (raw_message != nullptr) {
        lance_free_string(raw_message);
    }
    if (message.empty()) {
        message = fmt::format("error_code={}", static_cast<int>(code));
    }
    return Status::InternalError("{} failed: {}", operation, message);
}

} // namespace doris::format::lance
