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

#include "paimon_reader.h"

#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/table/deletion_vector_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
PaimonReader::PaimonReader(std::unique_ptr<GenericReader> file_format_reader,
                           RuntimeProfile* profile, RuntimeState* state,
                           const TFileScanRangeParams& params, const TFileRangeDesc& range,
                           ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                           FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache),
          _kv_cache(kv_cache) {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(_profile, paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", paimon_profile);
    _paimon_profile.parse_deletion_vector_time =
            ADD_CHILD_TIMER(_profile, "ParseDeletionVectorTime", paimon_profile);
}

Status PaimonReader::init_row_filters() {
    const auto& table_desc = _range.table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    // set push down agg type to NONE because we can not do count push down opt
    // if there are delete files.
    if (!_range.table_format_params.paimon_params.__isset.row_count) {
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }
    const auto& deletion_file = table_desc.deletion_file;

    Status create_status = Status::OK();

    std::string key;
    key.resize(deletion_file.path.size() + sizeof(deletion_file.offset));
    memcpy(key.data(), deletion_file.path.data(), deletion_file.path.size());
    memcpy(key.data() + deletion_file.path.size(), &deletion_file.offset,
           sizeof(deletion_file.offset));

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(key, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = deletion_file.path;
        delete_range.start_offset = deletion_file.offset;
        delete_range.size = deletion_file.length + 4;
        delete_range.file_size = -1;

        DeletionVectorReader dv_reader(_state, _profile, _params, delete_range, _io_ctx);
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        // the reason of adding 4: https://github.com/apache/paimon/issues/3313
        size_t bytes_read = deletion_file.length + 4;
        // TODO: better way to alloc memeory
        std::vector<char> buffer(bytes_read);
        create_status = dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        // parse deletion vector
        const char* buf = buffer.data();
        uint32_t actual_length;
        std::memcpy(reinterpret_cast<char*>(&actual_length), buf, 4);
        // change byte order to big endian
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
        // change byte order to big endian
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
        SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
        try {
            roaring_bitmap = roaring::Roaring::readSafe(buf, bytes_read - 4);
        } catch (const std::runtime_error& e) {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                    e.what());
            return nullptr;
        }
        delete_rows->reserve(roaring_bitmap.cardinality());
        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        COUNTER_UPDATE(_paimon_profile.num_delete_rows, delete_rows->size());
        return delete_rows;
    });
    RETURN_IF_ERROR(create_status);
    if (!_delete_rows->empty()) [[likely]] {
        set_delete_rows();
    }
    return Status::OK();
}

Status PaimonReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
