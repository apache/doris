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

#include <vector>

#include "common/status.h"
#include "format/reader/column_mapper.h"
#include "format/table/deletion_vector_reader.h"

namespace doris::reader {

Status TableReader::prepare_split(const SplitReadOptions& options) {
    _partition_values = std::move(options.partition_values);
    return _parse_delete_predicates(options);
}

Status TableReader::_parse_delete_predicates(const SplitReadOptions& options) {
    DeleteFileDesc desc {.fs_name = options.current_range.fs_name};
    if (_parse_delete_file(options.current_range.table_format_params, desc)) {
        Status create_status = Status::OK();

        _delete_rows = options.cache->get<DeleteRows>(desc.key, [&]() -> DeleteRows* {
            auto* delete_rows = new DeleteRows;

            DeletionVectorReader dv_reader(_runtime_state, _scanner_profile, *_scan_params, desc,
                                           _io_ctx);
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
