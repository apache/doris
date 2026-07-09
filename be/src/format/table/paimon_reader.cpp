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
#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

constexpr static char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};

} // namespace

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    return fmt::format("paimon_dv_{}#{}#{}", deletion_file.path, deletion_file.offset,
                       deletion_file.length);
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            std::vector<int64_t>* delete_rows) {
    if (buffer_size < 8) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const uint32_t actual_length = BigEndian::Load32(buf);
    if (actual_length + 4 != buffer_size) [[unlikely]] {
        return Status::RuntimeError(
                "DeletionVector deserialize error: length not match, "
                "actual length: {}, expect length: {}",
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

    delete_rows->reserve(roaring_bitmap.cardinality());
    for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); it++) {
        delete_rows->push_back(*it);
    }
    return Status::OK();
}

// ============================================================================
// PaimonOrcReader
// ============================================================================
void PaimonOrcReader::_init_paimon_profile() {
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

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(
            build_paimon_deletion_vector_cache_key(deletion_file), [&]() -> DeleteRows* {
                auto delete_rows = std::make_unique<DeleteRows>();

                TFileRangeDesc delete_range;
                delete_range.__set_fs_name(get_scan_range().fs_name);
                delete_range.path = deletion_file.path;
                delete_range.start_offset = deletion_file.offset;
                delete_range.size = deletion_file.length + 4;
                delete_range.file_size = -1;

                DeletionVectorReader dv_reader(get_state(), get_profile(), get_scan_params(),
                                               delete_range, get_io_ctx());
                create_status = dv_reader.open();
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                size_t bytes_read = deletion_file.length + 4;
                std::vector<char> buffer(bytes_read);
                create_status =
                        dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
                create_status = decode_paimon_deletion_vector_buffer(buffer.data(), bytes_read,
                                                                     delete_rows.get());
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }
                COUNTER_UPDATE(_paimon_profile.num_delete_rows, delete_rows->size());
                return delete_rows.release();
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

Status PaimonParquetReader::on_after_init_reader(ReaderInitContext* /*ctx*/) {
    return _init_deletion_vector();
}

Status PaimonParquetReader::_init_deletion_vector() {
    const auto& table_desc = get_scan_range().table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    if (!get_scan_range().table_format_params.paimon_params.__isset.row_count) {
        set_push_down_agg_type(TPushAggOp::NONE);
    }
    const auto& deletion_file = table_desc.deletion_file;

    Status create_status = Status::OK();

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(
            build_paimon_deletion_vector_cache_key(deletion_file), [&]() -> DeleteRows* {
                auto delete_rows = std::make_unique<DeleteRows>();

                TFileRangeDesc delete_range;
                delete_range.__set_fs_name(get_scan_range().fs_name);
                delete_range.path = deletion_file.path;
                delete_range.start_offset = deletion_file.offset;
                delete_range.size = deletion_file.length + 4;
                delete_range.file_size = -1;

                DeletionVectorReader dv_reader(get_state(), get_profile(), get_scan_params(),
                                               delete_range, get_io_ctx());
                create_status = dv_reader.open();
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                size_t bytes_read = deletion_file.length + 4;
                std::vector<char> buffer(bytes_read);
                create_status =
                        dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
                create_status = decode_paimon_deletion_vector_buffer(buffer.data(), bytes_read,
                                                                     delete_rows.get());
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }
                COUNTER_UPDATE(_paimon_profile.num_delete_rows, delete_rows->size());
                return delete_rows.release();
            });
    RETURN_IF_ERROR(create_status);
    if (!_delete_rows->empty()) [[likely]] {
        ParquetReader::set_delete_rows(_delete_rows);
    }
    return Status::OK();
}

} // namespace doris
