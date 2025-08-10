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

#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/table_format_reader.h"
namespace doris::vectorized {
#include "common/compile_check_begin.h"

// By holding a parquet/orc reader, used to read the parquet/orc table of hive.
class HiveReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    HiveReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
               RuntimeState* state, const TFileScanRangeParams& params, const TFileRangeDesc& range,
               io::IOContext* io_ctx, const std::set<TSlotId>* is_file_slot, FileMetaCache* meta_cache)
            : TableFormatReader(std::move(file_format_reader), state, profile, params, range,
                                io_ctx, meta_cache),
              _is_file_slot(is_file_slot) {};

    ~HiveReader() override = default;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    Status init_row_filters() final { return Status::OK(); };

protected:
    // https://github.com/apache/doris/pull/23369
    const std::set<TSlotId>* _is_file_slot = nullptr;
};

class HiveOrcReader final : public HiveReader {
public:
    ENABLE_FACTORY_CREATOR(HiveOrcReader);
    HiveOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                  RuntimeState* state, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, io::IOContext* io_ctx,
                  const std::set<TSlotId>* is_file_slot, FileMetaCache* meta_cache)
            : HiveReader(std::move(file_format_reader), profile, state, params, range, io_ctx,
                         is_file_slot, meta_cache) {};
    ~HiveOrcReader() final = default;

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const std::unordered_map<std::string, ColumnValueRangeType>*
                    table_col_name_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);
};

class HiveParquetReader final : public HiveReader {
public:
    ENABLE_FACTORY_CREATOR(HiveParquetReader);
    HiveParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                      RuntimeState* state, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx,
                      const std::set<TSlotId>* is_file_slot, FileMetaCache* meta_cache)
            : HiveReader(std::move(file_format_reader), profile, state, params, range, io_ctx,
                         is_file_slot, meta_cache) {};
    ~HiveParquetReader() final = default;

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const std::unordered_map<std::string, ColumnValueRangeType>*
                    table_col_name_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized