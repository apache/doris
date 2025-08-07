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
class HudiReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    HudiReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
               RuntimeState* state, const TFileScanRangeParams& params, const TFileRangeDesc& range,
               io::IOContext* io_ctx)
            : TableFormatReader(std::move(file_format_reader), state, profile, params, range,
                                io_ctx) {};

    ~HudiReader() override = default;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    Status init_row_filters() final { return Status::OK(); };
};

class HudiParquetReader final : public HudiReader {
public:
    ENABLE_FACTORY_CREATOR(HudiParquetReader);
    HudiParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                      RuntimeState* state, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx)
            : HudiReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~HudiParquetReader() final = default;

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

class HudiOrcReader final : public HudiReader {
public:
    ENABLE_FACTORY_CREATOR(HudiOrcReader);
    HudiOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                  RuntimeState* state, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, io::IOContext* io_ctx)
            : HudiReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~HudiOrcReader() final = default;

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const std::unordered_map<std::string, ColumnValueRangeType>*
                    table_col_name_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
        auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
        const orc::Type* orc_type_ptr = nullptr;
        RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));
        RETURN_IF_ERROR(gen_table_info_node_by_field_id(
                _params, _range.table_format_params.hudi_params.schema_id, tuple_descriptor,
                orc_type_ptr));

        return orc_reader->init_reader(&read_table_col_names, table_col_name_to_value_range,
                                       conjuncts, false, tuple_descriptor, row_descriptor,
                                       not_single_slot_filter_conjuncts,
                                       slot_id_to_filter_conjuncts, table_info_node_ptr);
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized