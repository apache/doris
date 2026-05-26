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

#include <vector>

#include "common/status.h"
#include "format/new_parquet/parquet_reader.h"
#include "format/reader/column_mapper.h"
#include "format/table/deletion_vector_reader.h"
#include "io/io_common.h"

namespace doris::reader {

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

Status TableReader::init(TableReadOptions options) {
    _scan_params = options.scan_params;
    _format = options.format;
    _io_ctx = options.io_ctx;
    _runtime_state = options.runtime_state;
    _scanner_profile = options.scanner_profile;
    _projected_columns = std::move(options.projected_columns);
    _system_properties = create_system_properties(_scan_params);
    _profile = std::move(options.profile);
    TableColumnMapperOptions mapper_options;
    mapper_options.mode = TableColumnMappingMode::BY_FIELD_ID;
    _data_reader.column_mapper = TableColumnMapper(mapper_options);
    // TODO:
    // _table_filters = build_table_filters_from_conjuncts(options.conjuncts);
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
    return _parse_delete_predicates(options);
}

Status TableReader::_parse_delete_predicates(const SplitReadOptions& options) {
    DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
    if (_parse_delete_file(options.current_range.table_format_params, desc)) {
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
            uint32_t actual_length;
            std::memcpy(reinterpret_cast<char*>(&actual_length), buf, 4);
            std::reverse(reinterpret_cast<char*>(&actual_length),
                         reinterpret_cast<char*>(&actual_length) + 4);
            buf += 4;
            if (actual_length != bytes_read - 4) [[unlikely]] {
                create_status = Status::RuntimeError(
                        "DeletionVector deserialize error: length not match, "
                        "actual length: {}, expect length: {}",
                        actual_length, bytes_read - 4);
                return nullptr;
            }
            uint32_t magic_number;
            std::memcpy(reinterpret_cast<char*>(&magic_number), buf, 4);
            std::reverse(reinterpret_cast<char*>(&magic_number),
                         reinterpret_cast<char*>(&magic_number) + 4);
            buf += 4;
            const static uint32_t MAGIC_NUMBER = 1581511376;
            if (magic_number != MAGIC_NUMBER) [[unlikely]] {
                create_status = Status::RuntimeError(
                        "DeletionVector deserialize error: invalid magic number {}", magic_number);
                return nullptr;
            }

            roaring::Roaring roaring_bitmap;
            SCOPED_TIMER(_profile->parse_delete_file_time);
            try {
                roaring_bitmap = roaring::Roaring::readSafe(buf, bytes_read - 4);
            } catch (const std::runtime_error& e) {
                create_status = Status::RuntimeError(
                        "DeletionVector deserialize error: failed to deserialize roaring bitmap, "
                        "{}",
                        e.what());
                return nullptr;
            }
            delete_rows->reserve(roaring_bitmap.cardinality());
            for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); it++) {
                delete_rows->push_back(*it);
            }
            COUNTER_UPDATE(_profile->num_delete_rows, delete_rows->size());
            return delete_rows;
        });
        RETURN_IF_ERROR(create_status);
    }

    return Status::OK();
}
} // namespace doris::reader
