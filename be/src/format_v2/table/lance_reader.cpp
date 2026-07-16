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

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <lance/lance.h>

namespace doris::format::lance {
namespace {

struct LanceScannerDeleter {
    void operator()(LanceScanner* scanner) const { lance_scanner_close(scanner); }
};

struct LanceBatchDeleter {
    void operator()(LanceBatch* batch) const { lance_batch_free(batch); }
};

} // namespace

LanceTableReader::~LanceTableReader() {
    static_cast<void>(close());
}

Status LanceTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    DCHECK(_runtime_state != nullptr);
    _ctz = _runtime_state->timezone_obj();

    _output_name_to_idx.clear();
    _output_name_to_idx.reserve(_projected_columns.size());
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        const auto& column = _projected_columns[idx];
        if (column.type == nullptr) {
            return Status::InvalidArgument("Lance projected column '{}' has no type", column.name);
        }
        if (!_output_name_to_idx.emplace(column.name, idx).second) {
            return Status::InvalidArgument("duplicate Lance projected column: {}", column.name);
        }
    }
    return Status::OK();
}

Status LanceTableReader::prepare_split(const SplitReadOptions& options) {
    RETURN_IF_ERROR(_validate_split(options.current_range));
    _close_scanner();
    _eof = false;

    RETURN_IF_ERROR(TableReader::prepare_split(options));
    // Lance does not currently provide metadata aggregate pushdown. Do not let a generic
    // table-level count supplied by a future planner bypass fragment reads.
    _remaining_table_level_count = -1;
    if (current_split_pruned()) {
        return Status::OK();
    }

    const auto key = _dataset_key(options.current_range);
    if (_dataset == nullptr) {
        RETURN_IF_ERROR(_open_dataset(key));
        _opened_dataset_key = key;
    } else if (!_opened_dataset_key.has_value() || *_opened_dataset_key != key) {
        return Status::InvalidArgument(
                "Lance reader cannot mix dataset snapshots or storage options in one scan");
    }

    RETURN_IF_ERROR(_prepare_conjuncts());
    return _open_scanner(options.current_range);
}

Status LanceTableReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    *eos = false;

    if (_eof) {
        *eos = true;
        return Status::OK();
    }
    if (_scanner == nullptr) {
        return Status::InternalError("Lance scanner is not initialized for the current split");
    }

    const auto target_rows = std::max<size_t>(1, _scanner_batch_size);
    while (true) {
        block->clear_column_data(_projected_columns.size());
        size_t raw_rows = 0;
        while (raw_rows < target_rows) {
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                _eof = true;
                _close_scanner();
                *eos = true;
                return Status::OK();
            }

            LanceBatch* raw_batch = nullptr;
            const int32_t scan_status = lance_scanner_next(_scanner, &raw_batch);
            if (scan_status == 1) {
                _eof = true;
                _close_scanner();
                break;
            }
            if (scan_status != 0 || raw_batch == nullptr) {
                return _lance_error("read next Lance batch");
            }

            std::unique_ptr<LanceBatch, LanceBatchDeleter> batch(raw_batch);
            size_t rows = 0;
            RETURN_IF_ERROR(_fill_block_from_arrow(batch.get(), block, &rows));
            _record_scan_rows(rows);
            raw_rows += rows;
        }

        if (raw_rows == 0) {
            DORIS_CHECK(_eof);
            *eos = true;
            return Status::OK();
        }

        if (!_conjuncts.empty()) {
            RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
        }
        if (block->rows() > 0) {
            // Preserve a non-empty final block. The next get_block() observes `_eof` and reports
            // split EOF, matching the generic reader contract.
            return Status::OK();
        }
        if (_eof) {
            *eos = true;
            return Status::OK();
        }
    }
}

Status LanceTableReader::abort_split() {
    _close_scanner();
    _eof = true;
    return TableReader::abort_split();
}

Status LanceTableReader::close() {
    _close_scanner();
    _close_dataset();
    _opened_dataset_key.reset();
    _eof = true;
    return TableReader::close();
}

Status LanceTableReader::_validate_split(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.lance_params) {
        return Status::InvalidArgument("Lance split requires lance_params in table format params");
    }
    const auto& params = range.table_format_params.lance_params;
    if (!params.__isset.dataset_uri || params.dataset_uri.empty()) {
        return Status::InvalidArgument("Lance split requires a non-empty dataset_uri");
    }
    if (!params.__isset.version || params.version < 0) {
        return Status::InvalidArgument("Lance split requires a non-negative dataset version");
    }
    if (!params.__isset.fragment_ids || params.fragment_ids.empty()) {
        return Status::InvalidArgument("Lance split requires non-empty fragment_ids");
    }
    std::unordered_set<int64_t> unique_ids;
    for (const auto fragment_id : params.fragment_ids) {
        if (fragment_id < 0) {
            return Status::InvalidArgument("Lance fragment id must be non-negative: {}",
                                           fragment_id);
        }
        if (!unique_ids.emplace(fragment_id).second) {
            return Status::InvalidArgument("Lance split contains duplicate fragment id: {}",
                                           fragment_id);
        }
    }
    return Status::OK();
}

Status LanceTableReader::_open_dataset(const DatasetKey& key) {
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(key.storage_options.size() + 1);
    for (const auto& option : key.storage_options) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    _dataset = lance_dataset_open(
            key.uri.c_str(), key.storage_options.empty() ? nullptr : storage_option_ptrs.data(),
            static_cast<uint64_t>(key.version));
    if (_dataset == nullptr) {
        return _lance_error("open Lance dataset");
    }
    return Status::OK();
}

Status LanceTableReader::_open_scanner(const TFileRangeDesc& range) {
    std::vector<const char*> columns;
    columns.reserve(_projected_columns.size() + 1);
    for (const auto& column : _projected_columns) {
        columns.emplace_back(column.name.c_str());
    }
    columns.emplace_back(nullptr);

    LanceScanner* scanner = lance_scanner_new(
            _dataset, _projected_columns.empty() ? nullptr : columns.data(), nullptr);
    if (scanner == nullptr) {
        return _lance_error("create Lance scanner");
    }
    std::unique_ptr<LanceScanner, LanceScannerDeleter> scanner_guard(scanner);

    const auto batch_size = _batch_size > 0 ? _batch_size : _runtime_state->batch_size();
    if (lance_scanner_set_batch_size(scanner, static_cast<int64_t>(batch_size)) != 0) {
        return _lance_error("set Lance scanner batch size");
    }

    const auto& thrift_ids = range.table_format_params.lance_params.fragment_ids;
    std::vector<uint64_t> fragment_ids;
    fragment_ids.reserve(thrift_ids.size());
    for (const auto fragment_id : thrift_ids) {
        fragment_ids.emplace_back(static_cast<uint64_t>(fragment_id));
    }
    if (lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
        return _lance_error("set Lance scanner fragment ids");
    }
    _scanner = scanner_guard.release();
    _scanner_batch_size = batch_size;
    return Status::OK();
}

void LanceTableReader::_close_scanner() {
    if (_scanner != nullptr) {
        lance_scanner_close(_scanner);
        _scanner = nullptr;
    }
    _scanner_batch_size = 0;
}

void LanceTableReader::_close_dataset() {
    if (_dataset != nullptr) {
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
}

Status LanceTableReader::_fill_block_from_arrow(LanceBatch* batch, Block* block, size_t* rows) {
    DORIS_CHECK(batch != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(rows != nullptr);
    ArrowArray array {};
    ArrowSchema schema {};
    if (lance_batch_to_arrow(batch, &array, &schema) != 0) {
        return _lance_error("export Lance batch to Arrow");
    }
    auto result = arrow::ImportRecordBatch(&array, &schema);
    if (!result.ok()) {
        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        return Status::InternalError("import Lance Arrow batch failed: {}",
                                     result.status().message());
    }

    const auto record_batch = std::move(result).ValueUnsafe();
    const auto row_count = static_cast<size_t>(record_batch->num_rows());
    std::unordered_set<std::string> materialized_columns;
    materialized_columns.reserve(record_batch->num_columns());
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int arrow_idx = 0; arrow_idx < record_batch->num_columns(); ++arrow_idx) {
        const auto& field = record_batch->schema()->field(arrow_idx);
        const auto output_it = _output_name_to_idx.find(field->name());
        if (output_it == _output_name_to_idx.end()) {
            return Status::InternalError("Lance returned unknown column '{}'", field->name());
        }
        if (!materialized_columns.emplace(field->name()).second) {
            return Status::InternalError("Lance returned duplicate column '{}'", field->name());
        }
        const auto output_idx = output_it->second;
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(output_idx)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[output_idx],
                                                             record_batch->column(arrow_idx).get(),
                                                             0, row_count, _ctz));
        } catch (const Exception& e) {
            return Status::InternalError("convert Lance Arrow column '{}' failed: {}",
                                         field->name(), e.what());
        }
    }
    for (const auto& column : _projected_columns) {
        if (!materialized_columns.contains(column.name)) {
            return Status::InternalError("Lance did not return requested column '{}'", column.name);
        }
    }
    *rows = row_count;
    return Status::OK();
}

Status LanceTableReader::_prepare_conjuncts() {
    RowDescriptor row_desc;
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    return Status::OK();
}

std::vector<std::string> LanceTableReader::_storage_options() const {
    if (_scan_params == nullptr || !_scan_params->__isset.properties) {
        return {};
    }
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 5> kStorageKeys = {
            {{"AWS_ACCESS_KEY", "aws_access_key_id"},
             {"AWS_SECRET_KEY", "aws_secret_access_key"},
             {"AWS_TOKEN", "aws_session_token"},
             {"AWS_ENDPOINT", "aws_endpoint"},
             {"AWS_REGION", "aws_region"}}};
    std::vector<std::string> options;
    options.reserve(kStorageKeys.size() * 2);
    for (const auto& [doris_key, lance_key] : kStorageKeys) {
        const auto it = _scan_params->properties.find(std::string(doris_key));
        if (it != _scan_params->properties.end() && !it->second.empty()) {
            options.emplace_back(lance_key);
            options.emplace_back(it->second);
        }
    }
    return options;
}

LanceTableReader::DatasetKey LanceTableReader::_dataset_key(const TFileRangeDesc& range) const {
    const auto& params = range.table_format_params.lance_params;
    return {
            .uri = params.dataset_uri,
            .version = params.version,
            .storage_options = _storage_options(),
    };
}

Status LanceTableReader::_lance_error(std::string_view operation) {
    const char* raw_message = lance_last_error_message();
    std::string message = raw_message == nullptr ? "" : raw_message;
    if (raw_message != nullptr) {
        lance_free_string(raw_message);
    }
    if (message.empty()) {
        message = fmt::format("error_code={}", static_cast<int>(lance_last_error_code()));
    }
    return Status::InternalError("{} failed: {}", operation, message);
}

} // namespace doris::format::lance
