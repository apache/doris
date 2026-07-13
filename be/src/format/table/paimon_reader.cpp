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

#include <fmt/format.h>

#include <cstring>
#include <vector>

#include "common/status.h"
#include "format/table/deletion_vector_reader.h"
#include "runtime/runtime_state.h"
#include "exec/common/endian.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace {
constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
} // namespace

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    return fmt::format("paimon_dv_{}#{}#{}", deletion_file.path, deletion_file.offset,
                       deletion_file.length);
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("deletion_vector must not be null");
    }
    if (buffer_size < 8) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }
    const uint32_t actual_length = BigEndian::Load32(buf);
    if (actual_length + 4 != buffer_size) [[unlikely]] {
        return Status::RuntimeError(
                "DeletionVector deserialize error: length not match, actual length: {}, expect length: {}",
                actual_length, buffer_size - 4);
    }
    if (memcmp(buf + sizeof(actual_length), PAIMON_BITMAP_MAGIC, 4) != 0) [[unlikely]] {
        return Status::RuntimeError("DeletionVector deserialize error: invalid magic number {}",
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
    _paimon_profile.decoded_cache_hit_count = ADD_CHILD_COUNTER(
            _profile, "DeletionVectorDecodedCacheHitCount", TUnit::UNIT, paimon_profile);
    _paimon_profile.decoded_cache_miss_count = ADD_CHILD_COUNTER(
            _profile, "DeletionVectorDecodedCacheMissCount", TUnit::UNIT, paimon_profile);
    _paimon_profile.file_cache_hit_count = ADD_CHILD_COUNTER(
            _profile, "DeletionVectorFileCacheHitCount", TUnit::UNIT, paimon_profile);
    _paimon_profile.file_cache_miss_count = ADD_CHILD_COUNTER(
            _profile, "DeletionVectorFileCacheMissCount", TUnit::UNIT, paimon_profile);
    _paimon_profile.file_cache_peer_read_count = ADD_CHILD_COUNTER(
            _profile, "DeletionVectorFileCachePeerReadCount", TUnit::UNIT, paimon_profile);
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

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    bool decoded_cache_hit = false;
    _deletion_vector = _kv_cache->get<DeletionVector>(
            build_paimon_deletion_vector_cache_key(deletion_file), [&]() -> DeletionVector* {
        auto* deletion_vector = new DeletionVector;

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
        const auto& file_cache_stats = dv_reader.file_cache_statistics();
        COUNTER_UPDATE(_paimon_profile.file_cache_hit_count,
                       file_cache_stats.num_local_io_total);
        COUNTER_UPDATE(_paimon_profile.file_cache_miss_count,
                       file_cache_stats.num_remote_io_total);
        COUNTER_UPDATE(_paimon_profile.file_cache_peer_read_count,
                       file_cache_stats.num_peer_io_total);
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
        *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
        COUNTER_UPDATE(_paimon_profile.num_delete_rows, deletion_vector->cardinality());
        return deletion_vector;
    }, &decoded_cache_hit);
    RETURN_IF_ERROR(create_status);
    COUNTER_UPDATE(decoded_cache_hit ? _paimon_profile.decoded_cache_hit_count
                                     : _paimon_profile.decoded_cache_miss_count,
                   1);
    if (!_deletion_vector->isEmpty()) [[likely]] {
        set_deletion_vector();
    }
    return Status::OK();
}

Status PaimonReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris
