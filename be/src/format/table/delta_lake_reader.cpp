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

#include "format/table/delta_lake_reader.h"

#include <cstring>
#include <vector>

#include "format/parquet/parquet_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "roaring/roaring64map.hh"
#include "util/endian.h"

namespace doris {
#include "common/compile_check_begin.h"

DeltaLakeParquetReader::DeltaLakeParquetReader(std::unique_ptr<GenericReader> file_format_reader,
                                               RuntimeProfile* profile, RuntimeState* state,
                                               const TFileScanRangeParams& params,
                                               const TFileRangeDesc& range, io::IOContext* io_ctx,
                                               FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache) {
    _delta_profile.num_delete_rows =
            ADD_COUNTER_WITH_LEVEL(profile, "DeltaLakeNumDeleteRows", TUnit::UNIT, 1);
    _delta_profile.delete_files_read_time = ADD_TIMER_WITH_LEVEL(profile, "DeltaLakeDVReadTime", 1);
    _delta_profile.parse_delete_file_time =
            ADD_TIMER_WITH_LEVEL(profile, "DeltaLakeDVParseTime", 1);
}

Status DeltaLakeParquetReader::init_reader(
        const std::vector<std::string>& file_col_names,
        std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
        const VExprContextSPtrs& conjuncts,
        phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                slot_id_to_predicates,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    return parquet_reader->init_reader(file_col_names, col_name_to_block_idx, conjuncts,
                                       slot_id_to_predicates, tuple_descriptor, row_descriptor,
                                       colname_to_slot_id, not_single_slot_filter_conjuncts,
                                       slot_id_to_filter_conjuncts);
}

Status DeltaLakeParquetReader::init_row_filters() {
    // Skip DV processing for table-level count push down
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _table_level_row_count > 0) {
        return Status::OK();
    }

    // Check if this file has a deletion vector
    if (_range.__isset.table_format_params &&
        _range.table_format_params.__isset.delta_lake_params &&
        _range.table_format_params.delta_lake_params.__isset.deletion_vector_desc) {
        _has_deletion_vector = true;
        RETURN_IF_ERROR(_read_deletion_vector());
    }

    return Status::OK();
}

Status DeltaLakeParquetReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    return _file_format_reader->get_next_block(block, read_rows, eof);
}

Status DeltaLakeParquetReader::_read_deletion_vector() {
    const auto& dv_desc = _range.table_format_params.delta_lake_params.deletion_vector_desc;

    SCOPED_TIMER(_delta_profile.delete_files_read_time);

    TFileRangeDesc dv_range;
    dv_range.__set_fs_name(_range.fs_name);
    dv_range.path = dv_desc.file_path;
    dv_range.start_offset = dv_desc.offset;
    dv_range.size = dv_desc.size_in_bytes;
    dv_range.file_size = -1;

    DeletionVectorReader dv_reader(_state, _profile, _params, dv_range, _io_ctx);
    RETURN_IF_ERROR(dv_reader.open());

    size_t buffer_size = dv_desc.size_in_bytes;
    std::vector<char> buf(buffer_size);
    if (buffer_size < 12) [[unlikely]] {
        // Minimum size: 4 bytes length + 4 bytes magic + 4 bytes CRC32
        return Status::DataQualityError("Delta Lake DV file size too small: {}", buffer_size);
    }

    RETURN_IF_ERROR(dv_reader.read_at(dv_desc.offset, {buf.data(), buffer_size}));

    // The serialized blob format (same as Iceberg DV / Puffin):
    //   4 bytes: combined length of vector and magic (big-endian)
    //   4 bytes: magic sequence D1 D3 39 64
    //   N bytes: serialized Roaring64Map
    //   4 bytes: CRC-32 checksum (big-endian)

    auto total_length = BigEndian::Load32(buf.data());
    if (total_length + 8 != buffer_size) [[unlikely]] {
        return Status::DataQualityError("Delta Lake DV length mismatch, expected: {}, actual: {}",
                                        total_length + 8, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf.data() + sizeof(total_length), MAGIC_NUMBER, 4)) [[unlikely]] {
        return Status::DataQualityError("Delta Lake DV magic number mismatch");
    }

    roaring::Roaring64Map bitmap;
    {
        SCOPED_TIMER(_delta_profile.parse_delete_file_time);
        try {
            bitmap = roaring::Roaring64Map::readSafe(buf.data() + 8, buffer_size - 12);
        } catch (const std::runtime_error& e) {
            return Status::DataQualityError("Decode Delta Lake DV roaring bitmap failed, {}",
                                            e.what());
        }
    }

    // Convert bitmap to sorted delete row positions and set on the parquet reader
    auto delete_rows = std::make_unique<std::vector<int64_t>>();
    delete_rows->reserve(bitmap.cardinality());
    for (auto it = bitmap.begin(); it != bitmap.end(); it++) {
        delete_rows->push_back(*it);
    }

    COUNTER_UPDATE(_delta_profile.num_delete_rows, delete_rows->size());

    if (!delete_rows->empty()) {
        auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
        _delete_rows = std::move(delete_rows);
        parquet_reader->set_delete_rows(_delete_rows.get());
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
