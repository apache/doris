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

#include "format/reader/table_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <cstring>
#include <set>
#include <stdexcept>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "exec/common/endian.h"
#include "exprs/vslot_ref.h"
#include "format/new_parquet/parquet_reader.h"
#include "format/reader/column_mapper.h"
#include "format/table/deletion_vector_reader.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"

namespace doris::reader {
namespace {

void collect_table_slot_ids(const VExprSPtr& expr, std::set<int>* slot_ids) {
    if (expr == nullptr) {
        return;
    }
    if (expr->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr.get());
        slot_ids->insert(slot_ref->slot_id());
    }
    for (const auto& child : expr->children()) {
        collect_table_slot_ids(child, slot_ids);
    }
}

Status build_table_filters_from_conjunct(const VExprContextSPtr& conjunct, RuntimeState* state,
                                         std::vector<TableFilter>* table_filters) {
    if (conjunct == nullptr) {
        return Status::OK();
    }
    std::set<int> slot_ids;
    collect_table_slot_ids(conjunct->root(), &slot_ids);
    if (!slot_ids.empty()) {
        TableFilter table_filter;
        table_filter.conjunct = nullptr;
        RETURN_IF_ERROR(conjunct->clone(state, table_filter.conjunct));
        table_filter.slot_ids.assign(slot_ids.begin(), slot_ids.end());
        table_filters->push_back(std::move(table_filter));
    }
    return Status::OK();
}

Status parse_deletion_vector(const char* buf, size_t buffer_size, DeleteFileDesc::Format format,
                             DeleteRows* delete_rows) {
    DORIS_CHECK(buf != nullptr);
    DORIS_CHECK(delete_rows != nullptr);
    DORIS_CHECK(format == DeleteFileDesc::Format::PAIMON ||
                format == DeleteFileDesc::Format::ICEBERG);

    const size_t checksum_size = format == DeleteFileDesc::Format::ICEBERG ? 4 : 0;
    if (buffer_size < 8 + checksum_size) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    auto total_length = BigEndian::Load32(buf);
    if (total_length + 4 + checksum_size != buffer_size) [[unlikely]] {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        total_length + 4 + checksum_size, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf + sizeof(total_length), MAGIC_NUMBER, 4) != 0) [[unlikely]] {
        return Status::DataQualityError("Deletion vector magic number mismatch");
    }

    const char* bitmap_buf = buf + 8;
    const size_t bitmap_size = buffer_size - 8 - checksum_size;
    if (format == DeleteFileDesc::Format::PAIMON) {
        roaring::Roaring bitmap;
        try {
            bitmap = roaring::Roaring::readSafe(bitmap_buf, bitmap_size);
        } catch (const std::runtime_error& e) {
            return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
        }

        delete_rows->reserve(bitmap.cardinality());
        for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        return Status::OK();
    }

    roaring::Roaring64Map bitmap;
    try {
        bitmap = roaring::Roaring64Map::readSafe(bitmap_buf, bitmap_size);
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
    }

    delete_rows->reserve(bitmap.cardinality());
    for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
        delete_rows->push_back(cast_set<int64_t>(*it));
    }
    return Status::OK();
}

} // namespace

std::shared_ptr<io::FileSystemProperties> create_system_properties(
        const TFileScanRangeParams* scan_params) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    if (scan_params == nullptr || !scan_params->__isset.file_type) {
        system_properties->system_type = TFileType::FILE_LOCAL;
        return system_properties;
    }
    system_properties->system_type = scan_params->file_type;
    system_properties->properties = scan_params->properties;
    system_properties->hdfs_params = scan_params->hdfs_params;
    if (scan_params->__isset.broker_addresses) {
        system_properties->broker_addresses.assign(scan_params->broker_addresses.begin(),
                                                   scan_params->broker_addresses.end());
    }
    return system_properties;
}

Status TableReader::init(TableReadOptions&& options) {
    _scan_params = options.scan_params;
    _format = options.format;
    _io_ctx = options.io_ctx;
    _runtime_state = options.runtime_state;
    _scanner_profile = options.scanner_profile;
    _push_down_agg_type = options.push_down_agg_type;
    _projected_columns = std::move(options.projected_columns);
    _system_properties = create_system_properties(_scan_params);
    _profile = std::move(options.profile);
    TableColumnMapperOptions mapper_options;
    mapper_options.mode = TableColumnMappingMode::BY_FIELD_ID;
    mapper_options.allow_missing_columns = options.allow_missing_columns;
    _data_reader.column_mapper = TableColumnMapper(mapper_options);
    _conjuncts = std::move(options.conjuncts);
    _table_column_predicates = std::move(options.column_predicates);
    return Status::OK();
}

Status TableReader::_build_table_filters_from_conjuncts() {
    _table_filters.clear();
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(
                build_table_filters_from_conjunct(conjunct, _runtime_state, &_table_filters));
    }
    return Status::OK();
}

Status TableReader::_open_local_filter_exprs(const FileScanRequest& file_request) {
    RowDescriptor row_desc;
    for (const auto& conjunct : file_request.conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    for (const auto& delete_conjunct : file_request.delete_conjuncts) {
        RETURN_IF_ERROR(delete_conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(delete_conjunct->open(_runtime_state));
    }
    return Status::OK();
}

Status TableReader::create_next_reader(bool* eos) {
    DCHECK(_data_reader.reader == nullptr);
    if (_current_task == nullptr) {
        *eos = true;
        return Status::OK();
    }

    switch (_format) {
    case FileFormat::PARQUET: {
        _data_reader.reader = std::make_unique<parquet::ParquetReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile);
        break;
    }
    case FileFormat::ORC:
    case FileFormat::CSV:
        return Status::NotSupported("TableReader does not support file format {}",
                                    static_cast<int>(_format));
    }

    RETURN_IF_ERROR(_data_reader.reader->init(_runtime_state));
    RETURN_IF_ERROR(open_reader());
    *eos = false;
    return Status::OK();
}

std::unique_ptr<io::FileDescription> create_file_description(const TFileRangeDesc& range) {
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = range.path;
    file_description->file_size = range.__isset.file_size ? range.file_size : -1;
    file_description->range_start_offset = range.__isset.start_offset ? range.start_offset : 0;
    file_description->range_size = range.__isset.size ? range.size : -1;
    if (range.__isset.fs_name) {
        file_description->fs_name = range.fs_name;
    }
    if (range.__isset.file_cache_admission) {
        file_description->file_cache_admission = range.file_cache_admission;
    }
    return file_description;
}

Status TableReader::prepare_split(const SplitReadOptions& options) {
    _partition_values = std::move(options.partition_values);
    _current_task = std::make_unique<ScanTask>();
    _current_task->data_file = create_file_description(options.current_range);
    _delete_rows = nullptr;
    _aggregate_pushdown_tried = false;
    return _parse_delete_predicates(options);
}

Status TableReader::_parse_delete_predicates(const SplitReadOptions& options) {
    DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
    bool has_delete_file = false;
    RETURN_IF_ERROR(_parse_deletion_vector_file(options.current_range.table_format_params, &desc,
                                                &has_delete_file));
    if (has_delete_file) {
        DORIS_CHECK(options.cache != nullptr);
        Status create_status = Status::OK();

        _delete_rows = options.cache->get<DeleteRows>(desc.key, [&]() -> DeleteRows* {
            auto* delete_rows = new DeleteRows;

            DeletionVectorReader dv_reader(_runtime_state, _scanner_profile, *_scan_params, desc,
                                           _io_ctx.get());
            create_status = dv_reader.open();
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }

            size_t bytes_read = desc.size;
            std::vector<char> buffer(bytes_read);
            create_status = dv_reader.read_at(desc.start_offset, {buffer.data(), bytes_read});
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }

            const char* buf = buffer.data();
            SCOPED_TIMER(_profile->parse_delete_file_time);
            create_status = parse_deletion_vector(buf, bytes_read, desc.format, delete_rows);
            if (!create_status.ok()) [[unlikely]] {
                return nullptr;
            }
            COUNTER_UPDATE(_profile->num_delete_rows, delete_rows->size());
            return delete_rows;
        });
        RETURN_IF_ERROR(create_status);
    }

    return Status::OK();
}
} // namespace doris::reader
