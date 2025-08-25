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

#include "hudi_reader.h"

#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status HudiReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
};

Status HudiParquetReader::init_reader(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    auto parquet_fields_schema = field_desc->get_fields_schema();
    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            _params, _range.table_format_params.hudi_params.schema_id, tuple_descriptor,
            *field_desc));
    return parquet_reader->init_reader(read_table_col_names, table_col_name_to_value_range,
                                       conjuncts, tuple_descriptor, row_descriptor,
                                       colname_to_slot_id, not_single_slot_filter_conjuncts,
                                       slot_id_to_filter_conjuncts, table_info_node_ptr);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
