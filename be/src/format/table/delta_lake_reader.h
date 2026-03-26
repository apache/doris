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

#include <memory>
#include <vector>

#include "common/status.h"
#include "format/table/table_format_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

class ParquetReader;

/**
 * Delta Lake table reader that wraps a ParquetReader and applies
 * Deletion Vector (DV) filtering.
 *
 * Delta Lake data files are always Parquet. The DV format used by
 * Delta Lake is the same Puffin-based Roaring64Map format used by
 * Iceberg V3, so this reader reuses the DeletionVectorReader
 * infrastructure already present in the Doris BE.
 *
 * For files without a DV, this reader simply delegates to the
 * underlying ParquetReader with no overhead.
 */
class DeltaLakeParquetReader final : public TableFormatReader {
public:
    ENABLE_FACTORY_CREATOR(DeltaLakeParquetReader);

    DeltaLakeParquetReader(std::unique_ptr<GenericReader> file_format_reader,
                           RuntimeProfile* profile, RuntimeState* state,
                           const TFileScanRangeParams& params, const TFileRangeDesc& range,
                           io::IOContext* io_ctx, FileMetaCache* meta_cache);

    ~DeltaLakeParquetReader() override = default;

    Status init_reader(
            const std::vector<std::string>& file_col_names,
            std::unordered_map<std::string, uint32_t>* col_name_to_block_idx,
            const VExprContextSPtrs& conjuncts,
            phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                    slot_id_to_predicates,
            const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);

    Status init_row_filters() final;
    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    bool has_delete_operations() const override { return _has_deletion_vector; }

private:
    // Read and apply the deletion vector for the current data file.
    Status _read_deletion_vector();

    struct DeltaLakeProfile {
        RuntimeProfile::Counter* num_delete_rows = nullptr;
        RuntimeProfile::Counter* delete_files_read_time = nullptr;
        RuntimeProfile::Counter* parse_delete_file_time = nullptr;
    };

    DeltaLakeProfile _delta_profile;
    bool _has_deletion_vector = false;
    // Owned by this reader — freed on destruction.
    std::unique_ptr<std::vector<int64_t>> _delete_rows;
};

#include "common/compile_check_end.h"
} // namespace doris
