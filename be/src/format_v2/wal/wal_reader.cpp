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

#include "format_v2/wal/wal_reader.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <ranges>
#include <unordered_set>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "load/group_commit/wal/wal_file_reader.h"
#include "load/group_commit/wal/wal_manager.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris::format::wal {
namespace {

class WalColumnMapper final : public TableColumnMapper {
public:
    using TableColumnMapper::TableColumnMapper;

    Status create_mapping(const std::vector<ColumnDefinition>& projected_columns,
                          const std::map<std::string, Field>& partition_values,
                          const std::vector<ColumnDefinition>& file_schema) override {
        for (const auto& projected : projected_columns) {
            if (!projected.has_identifier_field_id()) {
                return Status::InternalError("WAL projected column {} has no unique id",
                                             projected.name);
            }
            const auto found = std::ranges::find_if(file_schema, [&](const auto& file_column) {
                return file_column.has_identifier_field_id() &&
                       file_column.get_identifier_field_id() == projected.get_identifier_field_id();
            });
            if (found == file_schema.end()) {
                return Status::InternalError("WAL does not contain column unique id {} ({})",
                                             projected.get_identifier_field_id(), projected.name);
            }
        }
        return TableColumnMapper::create_mapping(projected_columns, partition_values, file_schema);
    }

protected:
    bool enable_lazy_materialization() const override { return false; }
    bool force_full_complex_scan_projection() const override { return true; }
};

} // namespace

Status parse_wal_column_ids(const std::string& encoded, std::vector<int32_t>* column_ids) {
    DORIS_CHECK(column_ids != nullptr);
    column_ids->clear();
    if (encoded.empty()) {
        return Status::Corruption("WAL header contains no column ids");
    }

    std::unordered_set<int32_t> seen;
    for (const absl::string_view token : absl::StrSplit(encoded, ',')) {
        int32_t column_id = 0;
        if (token.empty() || !absl::SimpleAtoi(token, &column_id)) {
            return Status::Corruption("invalid WAL column id '{}'", std::string(token));
        }
        if (!seen.emplace(column_id).second) {
            return Status::Corruption("duplicate WAL column id {}", column_id);
        }
        column_ids->push_back(column_id);
    }
    return Status::OK();
}

WalReader::WalReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                     std::unique_ptr<io::FileDescription>& file_description,
                     std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                     const std::vector<ColumnDefinition>& projected_columns)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _projected_columns(projected_columns) {}

WalReader::~WalReader() {
    static_cast<void>(close());
}

Status WalReader::init(RuntimeState* state) {
    if (state == nullptr || state->exec_env() == nullptr ||
        state->exec_env()->wal_mgr() == nullptr) {
        return Status::InvalidArgument("WAL v2 reader requires a runtime WAL manager");
    }
    RETURN_IF_ERROR(state->exec_env()->wal_mgr()->get_wal_path(state->wal_id(), _wal_path));
    _wal_reader = std::make_shared<doris::WalFileReader>(_wal_path);
    RETURN_IF_ERROR(_wal_reader->init());

    std::string encoded_column_ids;
    RETURN_IF_ERROR(_wal_reader->read_header(_version, encoded_column_ids));
    RETURN_IF_ERROR(parse_wal_column_ids(encoded_column_ids, &_column_ids));
    _reader_eof = false;
    _eof = false;
    return Status::OK();
}

Status WalReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("WAL v2 file_schema is null");
    }
    RETURN_IF_ERROR(_ensure_schema_loaded());
    *file_schema = _file_schema;
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> WalReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<WalColumnMapper>(std::move(options));
}

Status WalReader::open(std::shared_ptr<FileScanRequest> request) {
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    _first_block_consumed = false;
    _eof = false;
    return Status::OK();
}

Status WalReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_request == nullptr) {
        return Status::InternalError("WAL v2 reader is not open");
    }

    *rows = 0;
    *eof = false;
    if (_reader_eof) {
        *eof = true;
        _eof = true;
        return Status::OK();
    }

    PBlock pblock;
    if (_first_block_loaded && !_first_block_consumed) {
        pblock = _first_block;
        _first_block_consumed = true;
    } else {
        auto status = _wal_reader->read_block(pblock);
        if (status.is<ErrorCode::END_OF_FILE>()) {
            _reader_eof = true;
            *eof = true;
            _eof = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(status);
    }
    RETURN_IF_ERROR(_validate_block_version(pblock));

    Block source_block;
    size_t uncompressed_size = 0;
    int64_t decompress_time = 0;
    RETURN_IF_ERROR(source_block.deserialize(pblock, &uncompressed_size, &decompress_time));
    if (source_block.columns() != _column_ids.size()) {
        return Status::Corruption("WAL block has {} columns but header declares {}",
                                  source_block.columns(), _column_ids.size());
    }
    RETURN_IF_ERROR(_materialize_requested_columns(source_block, file_block));
    *rows = file_block->rows();
    _record_scan_rows(cast_set<int64_t>(*rows));
    RETURN_IF_ERROR(
            apply_materialized_reader_filters(_request.get(), _io_ctx.get(), file_block, rows));
    return Status::OK();
}

Status WalReader::close() {
    _request.reset();
    _reader_eof = true;
    _eof = true;
    if (_wal_reader == nullptr) {
        return Status::OK();
    }
    auto status = _wal_reader->finalize();
    if (status.ok()) {
        _wal_reader.reset();
    }
    return status;
}

Status WalReader::_ensure_schema_loaded() const {
    if (_schema_inited) {
        return Status::OK();
    }

    auto status = _wal_reader->read_block(_first_block);
    if (status.is<ErrorCode::END_OF_FILE>()) {
        // An empty WAL still has a complete unique-id header. Use only matching projected types;
        // there is no data block from which unprojected physical types could be inferred.
        return _init_schema_from_block(nullptr);
    }
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(_validate_block_version(_first_block));
    _first_block_loaded = true;
    return _init_schema_from_block(&_first_block);
}

Status WalReader::_validate_block_version(const PBlock& pblock) const {
    const int version = pblock.has_be_exec_version() ? pblock.be_exec_version() : 0;
    if (!BeExecVersionManager::check_be_exec_version(version)) {
        return Status::DataQualityError("unsupported BE execution version {} in WAL", version);
    }
    return Status::OK();
}

Status WalReader::_init_schema_from_block(const PBlock* pblock) const {
    if (pblock != nullptr && cast_set<size_t>(pblock->column_metas_size()) != _column_ids.size()) {
        return Status::Corruption("WAL block schema has {} columns but header declares {}",
                                  pblock->column_metas_size(), _column_ids.size());
    }

    _file_schema.clear();
    for (size_t idx = 0; idx < _column_ids.size(); ++idx) {
        ColumnDefinition field;
        field.identifier = Field::create_field<TYPE_INT>(_column_ids[idx]);
        field.local_id = cast_set<int32_t>(idx);
        if (pblock != nullptr) {
            const auto& meta = pblock->column_metas(cast_set<int>(idx));
            field.name = meta.name();
            field.type = make_nullable(DataTypeFactory::instance().create_data_type(meta));
        } else {
            const auto projected =
                    std::ranges::find_if(_projected_columns, [&](const auto& candidate) {
                        return candidate.has_identifier_field_id() &&
                               candidate.get_identifier_field_id() == _column_ids[idx];
                    });
            if (projected == _projected_columns.end()) {
                continue;
            }
            field.name = projected->name;
            field.type = projected->type;
        }
        _file_schema.push_back(std::move(field));
    }
    _schema_inited = true;
    return Status::OK();
}

Status WalReader::_materialize_requested_columns(const Block& source_block,
                                                 Block* file_block) const {
    for (const auto& [file_column_id, block_position] : _request->local_positions) {
        const auto source_idx = file_column_id.value();
        if (source_idx < 0 || cast_set<size_t>(source_idx) >= source_block.columns()) {
            return Status::Corruption("WAL request refers to invalid local column {}", source_idx);
        }
        if (block_position.value() >= file_block->columns()) {
            return Status::InternalError("WAL request has invalid block position {}",
                                         block_position.value());
        }
        const auto& target = file_block->get_by_position(block_position.value());
        auto column = source_block.get_by_position(source_idx).column;
        column = make_column_nullable_if_needed(std::move(column), target.type);
        file_block->replace_by_position(block_position.value(), IColumn::mutate(std::move(column)));
    }
    return Status::OK();
}

} // namespace doris::format::wal
