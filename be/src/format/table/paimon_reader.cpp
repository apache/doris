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
#include <utility>
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

    *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
    return Status::OK();
}

namespace {

template <typename Profile>
void init_deletion_vector_cache_profile(RuntimeProfile* profile, const char* parent,
                                        Profile* counters) {
    counters->decoded_cache_hit_count =
            ADD_CHILD_COUNTER(profile, "DeletionVectorDecodedCacheHitCount", TUnit::UNIT, parent);
    counters->decoded_cache_miss_count =
            ADD_CHILD_COUNTER(profile, "DeletionVectorDecodedCacheMissCount", TUnit::UNIT, parent);
    counters->file_cache_hit_count =
            ADD_CHILD_COUNTER(profile, "DeletionVectorFileCacheHitCount", TUnit::UNIT, parent);
    counters->file_cache_miss_count =
            ADD_CHILD_COUNTER(profile, "DeletionVectorFileCacheMissCount", TUnit::UNIT, parent);
    counters->file_cache_peer_read_count =
            ADD_CHILD_COUNTER(profile, "DeletionVectorFileCachePeerReadCount", TUnit::UNIT, parent);
}

template <typename Profile>
void update_deletion_vector_file_cache_profile(const DeletionVectorReader& reader,
                                               Profile* counters) {
    const auto& stats = reader.file_cache_statistics();
    COUNTER_UPDATE(counters->file_cache_hit_count, stats.num_local_io_total);
    COUNTER_UPDATE(counters->file_cache_miss_count, stats.num_remote_io_total);
    COUNTER_UPDATE(counters->file_cache_peer_read_count, stats.num_peer_io_total);
}

} // namespace

// ============================================================================
// PaimonOrcReader
// ============================================================================
void PaimonOrcReader::_init_paimon_profile() {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(get_profile(), paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(get_profile(), "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(get_profile(), "DeleteFileReadTime", paimon_profile);
    _paimon_profile.parse_deletion_vector_time =
            ADD_CHILD_TIMER(get_profile(), "ParseDeletionVectorTime", paimon_profile);
    init_deletion_vector_cache_profile(get_profile(), paimon_profile, &_paimon_profile);
}

Status PaimonOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                              _fill_partition_values,
                                              &_fill_partition_value_is_null));
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));

    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.paimon_params.schema_id,
            get_tuple_descriptor(), orc_type_ptr));
    ctx->table_info_node = table_info_node_ptr;

    for (const auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
    return Status::OK();
}

Status PaimonOrcReader::on_after_init_reader(ReaderInitContext* /*ctx*/) {
    return _init_deletion_vector();
}

Status PaimonOrcReader::_init_deletion_vector() {
    const auto& table_desc = get_scan_range().table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    // Cannot do count push down if there are delete files
    if (!get_scan_range().table_format_params.paimon_params.__isset.row_count) {
        set_push_down_agg_type(TPushAggOp::NONE);
    }
    const auto& deletion_file = table_desc.deletion_file;

    Status create_status = Status::OK();

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    bool decoded_cache_hit = false;
    _deletion_vector = _kv_cache->get<DeletionVector>(
            build_paimon_deletion_vector_cache_key(deletion_file),
            [&]() -> DeletionVector* {
                auto deletion_vector = std::make_unique<DeletionVector>();

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
                update_deletion_vector_file_cache_profile(dv_reader, &_paimon_profile);
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
                create_status = decode_paimon_deletion_vector_buffer(buffer.data(), bytes_read,
                                                                     deletion_vector.get());
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }
                COUNTER_UPDATE(_paimon_profile.num_delete_rows, deletion_vector->cardinality());
                return deletion_vector.release();
            },
            &decoded_cache_hit);
    RETURN_IF_ERROR(create_status);
    COUNTER_UPDATE(decoded_cache_hit ? _paimon_profile.decoded_cache_hit_count
                                     : _paimon_profile.decoded_cache_miss_count,
                   1);
    if (!_deletion_vector->isEmpty()) [[likely]] {
        set_deletion_vector(_deletion_vector);
    }
    return Status::OK();
}

// ============================================================================
// PaimonParquetReader
// ============================================================================
void PaimonParquetReader::_init_paimon_profile() {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(get_profile(), paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(get_profile(), "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(get_profile(), "DeleteFileReadTime", paimon_profile);
    _paimon_profile.parse_deletion_vector_time =
            ADD_CHILD_TIMER(get_profile(), "ParseDeletionVectorTime", paimon_profile);
    init_deletion_vector_cache_profile(get_profile(), paimon_profile, &_paimon_profile);
}

Status PaimonParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                              _fill_partition_values,
                                              &_fill_partition_value_is_null));
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.paimon_params.schema_id,
            get_tuple_descriptor(), *field_desc));
    ctx->table_info_node = table_info_node_ptr;

    for (const auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR ||
            desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
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
    bool decoded_cache_hit = false;
    _deletion_vector = _kv_cache->get<DeletionVector>(
            build_paimon_deletion_vector_cache_key(deletion_file),
            [&]() -> DeletionVector* {
                auto deletion_vector = std::make_unique<DeletionVector>();

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
                update_deletion_vector_file_cache_profile(dv_reader, &_paimon_profile);
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }

                SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
                create_status = decode_paimon_deletion_vector_buffer(buffer.data(), bytes_read,
                                                                     deletion_vector.get());
                if (!create_status.ok()) [[unlikely]] {
                    return nullptr;
                }
                COUNTER_UPDATE(_paimon_profile.num_delete_rows, deletion_vector->cardinality());
                return deletion_vector.release();
            },
            &decoded_cache_hit);
    RETURN_IF_ERROR(create_status);
    COUNTER_UPDATE(decoded_cache_hit ? _paimon_profile.decoded_cache_hit_count
                                     : _paimon_profile.decoded_cache_miss_count,
                   1);
    if (!_deletion_vector->isEmpty()) [[likely]] {
        ParquetReader::set_deletion_vector(_deletion_vector);
    }
    return Status::OK();
}

} // namespace doris
