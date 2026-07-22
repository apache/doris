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

#include "format/table/paimon_reader.h"

#include <cstring>
#include <vector>

#include "common/status.h"
#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "runtime/runtime_state.h"

namespace doris {
#include "common/compile_check_begin.h"

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    // A shared file can contain multiple vectors, so the cache identity must include its range.
    return deletion_file.path + "#" + std::to_string(deletion_file.offset) + "#" +
           std::to_string(deletion_file.length);
}

Status validate_paimon_deletion_vector_descriptor(const TPaimonDeletionFileDesc& deletion_file,
                                                  size_t& bytes_read) {
    if (!deletion_file.__isset.path || !deletion_file.__isset.offset ||
        !deletion_file.__isset.length) {
        return Status::DataQualityError(
                "Paimon deletion file descriptor misses path/offset/length");
    }
    return validate_paimon_deletion_vector_read_range(deletion_file.offset, deletion_file.length,
                                                      bytes_read);
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("deletion vector output must not be null");
    }
    if (buf == nullptr) {
        return Status::DataQualityError("Paimon deletion vector blob is null");
    }
    if (buffer_size < 8) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const uint32_t actual_length = BigEndian::Load32(buf);
    if (static_cast<uint64_t>(actual_length) + 4 != buffer_size) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector length mismatch, expected: {}, actual: {}",
                static_cast<uint64_t>(actual_length) + 4, buffer_size);
    }

    // Paimon deletion vectors always prefix the portable Roaring payload with this magic value.
    constexpr char paimon_bitmap_magic[] = {'\x5E', '\x43', '\xF2', '\xD0'};
    if (memcmp(buf + sizeof(actual_length), paimon_bitmap_magic, 4) != 0) [[unlikely]] {
        return Status::DataQualityError(
                "Paimon deletion vector magic number mismatch, expected: {}, actual: {}",
                BigEndian::Load32(paimon_bitmap_magic),
                BigEndian::Load32(buf + sizeof(actual_length)));
    }

    roaring::Roaring roaring_bitmap;
    try {
        roaring_bitmap = roaring::Roaring::readSafe(buf + 8, buffer_size - 8);
    } catch (const std::runtime_error& e) {
        return Status::RuntimeError(
                "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                e.what());
    }

    *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
    return Status::OK();
}

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
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_paimon_deletion_vector_descriptor(deletion_file, bytes_read));

    Status create_status = Status::OK();
    const std::string key = build_paimon_deletion_vector_cache_key(deletion_file);

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(key, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        // must use __set() method to make sure __isset is true
        delete_range.__set_fs_name(_range.fs_name);
        delete_range.path = deletion_file.path;
        delete_range.start_offset = deletion_file.offset;
        delete_range.size = static_cast<int64_t>(bytes_read);
        delete_range.file_size = -1;

        DeletionVectorReader dv_reader(_state, _profile, _params, delete_range, _io_ctx);
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        std::vector<char> buffer(bytes_read);
        create_status = dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        const uint32_t actual_length = BigEndian::Load32(buffer.data());
        if (static_cast<uint64_t>(actual_length) + 4 != bytes_read) [[unlikely]] {
            create_status = Status::DataQualityError(
                    "Paimon deletion vector length mismatch, expected: {}, actual: {}",
                    static_cast<uint64_t>(actual_length) + 4, bytes_read);
            return nullptr;
        }
        constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
        if (memcmp(buffer.data() + 4, PAIMON_BITMAP_MAGIC, 4) != 0) [[unlikely]] {
            create_status = Status::DataQualityError(
                    "Paimon deletion vector magic number mismatch, expected: {}, actual: {}",
                    BigEndian::Load32(PAIMON_BITMAP_MAGIC), BigEndian::Load32(buffer.data() + 4));
            return nullptr;
        }

        roaring::Roaring roaring_bitmap;
        SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
        try {
            roaring_bitmap = roaring::Roaring::readSafe(buffer.data() + 8, bytes_read - 8);
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
} // namespace doris
