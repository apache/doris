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

#pragma once

#include <gen_cpp/PlanNodes_types.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/table_format_reader.h"

namespace doris {
#include "common/compile_check_begin.h"
class PaimonReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    PaimonReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                 RuntimeState* state, const TFileScanRangeParams& params,
                 const TFileRangeDesc& range, ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                 FileMetaCache* meta_cache);

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file);

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            std::vector<int64_t>* delete_rows);

// PaimonOrcReader: directly inherits OrcReader (no composition wrapping).
// Schema mapping in on_before_init_reader, deletion vector reading in on_after_init_reader.
class PaimonOrcReader final : public OrcReader, public TableSchemaChangeHelper {
public:
    ENABLE_FACTORY_CREATOR(PaimonOrcReader);
    PaimonOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                    RuntimeState* state, const TFileScanRangeParams& params,
                    const TFileRangeDesc& range, ShardedKVCache* kv_cache, io::IOContext* io_ctx,
                    FileMetaCache* meta_cache)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, kv_cache,
                           io_ctx, meta_cache) {};
    ~PaimonOrcReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<OrcReader*>(_file_format_reader.get()))
                ->set_position_delete_rowids(_delete_rows);
    }

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
        auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
        const orc::Type* orc_type_ptr = nullptr;
        RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
        RETURN_IF_ERROR(gen_table_info_node_by_field_id(
                _params, _range.table_format_params.paimon_params.schema_id, tuple_descriptor,
                orc_type_ptr));

        return orc_reader->init_reader(&read_table_col_names, col_name_to_block_idx, conjuncts,
                                       false, tuple_descriptor, row_descriptor,
                                       not_single_slot_filter_conjuncts,
                                       slot_id_to_filter_conjuncts, table_info_node_ptr);
    }
};

class PaimonParquetReader final : public PaimonReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonParquetReader);
    PaimonParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                        RuntimeState* state, const TFileScanRangeParams& params,
                        const TFileRangeDesc& range, ShardedKVCache* kv_cache,
                        io::IOContext* io_ctx, FileMetaCache* meta_cache)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, kv_cache,
                           io_ctx, meta_cache) {};
    ~PaimonParquetReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<ParquetReader*>(_file_format_reader.get()))
                ->set_delete_rows(_delete_rows);
    }

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
            const VExprContextSPtrs& conjuncts,
            phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                    slot_id_to_predicates,
            const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
        auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());

        const FieldDescriptor* field_desc = nullptr;
        RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&field_desc));
        DCHECK(field_desc != nullptr);

        RETURN_IF_ERROR(gen_table_info_node_by_field_id(
                _params, _range.table_format_params.paimon_params.schema_id, tuple_descriptor,
                *field_desc));

        return parquet_reader->init_reader(read_table_col_names, col_name_to_block_idx, conjuncts,
                                           slot_id_to_predicates, tuple_descriptor, row_descriptor,
                                           colname_to_slot_id, not_single_slot_filter_conjuncts,
                                           slot_id_to_filter_conjuncts, table_info_node_ptr);
    }
};
#include "common/compile_check_end.h"
} // namespace doris
