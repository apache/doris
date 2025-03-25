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
class PaimonReader : public TableFormatReader, public TableSchemaChangeHelper {
public:
    PaimonReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                 RuntimeState* state, const TFileScanRangeParams& params,
                 const TFileRangeDesc& range, io::IOContext* io_ctx);

    ~PaimonReader() override = default;

    Status init_row_filters() final;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

    Status get_file_col_id_to_name(bool& exist_schema,
                                   std::map<int, std::string>& file_col_id_to_name) final;

protected:
    struct PaimonProfile {
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
    };
    std::vector<int64_t> _delete_rows;
    PaimonProfile _paimon_profile;

    virtual void set_delete_rows() = 0;
};

class PaimonOrcReader final : public PaimonReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonOrcReader);
    PaimonOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                    RuntimeState* state, const TFileScanRangeParams& params,
                    const TFileRangeDesc& range, io::IOContext* io_ctx)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~PaimonOrcReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<OrcReader*>(_file_format_reader.get()))
                ->set_position_delete_rowids(&_delete_rows);
    }

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const std::unordered_map<int32_t, std::string>& table_col_id_table_name_map,
            std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
        RETURN_IF_ERROR(TableSchemaChangeHelper::init_schema_info(
                read_table_col_names, table_col_id_table_name_map, table_col_name_to_value_range));

        auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
        orc_reader->set_table_col_to_file_col(_table_col_to_file_col);
        return orc_reader->init_reader(
                &_all_required_col_names, _not_in_file_col_names, &_new_colname_to_value_range,
                conjuncts, false, tuple_descriptor, row_descriptor,
                not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    }
};

class PaimonParquetReader final : public PaimonReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonParquetReader);
    PaimonParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                        RuntimeState* state, const TFileScanRangeParams& params,
                        const TFileRangeDesc& range, io::IOContext* io_ctx)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~PaimonParquetReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<ParquetReader*>(_file_format_reader.get()))
                ->set_delete_rows(&_delete_rows);
    }

    Status init_reader(
            const std::vector<std::string>& read_table_col_names,
            const std::unordered_map<int32_t, std::string>& table_col_id_table_name_map,
            std::unordered_map<std::string, ColumnValueRangeType>* table_col_name_to_value_range,
            const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
            const RowDescriptor* row_descriptor,
            const std::unordered_map<std::string, int>* colname_to_slot_id,
            const VExprContextSPtrs* not_single_slot_filter_conjuncts,
            const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
        RETURN_IF_ERROR(TableSchemaChangeHelper::init_schema_info(
                read_table_col_names, table_col_id_table_name_map, table_col_name_to_value_range));
        auto* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
        parquet_reader->set_table_to_file_col_map(_table_col_to_file_col);

        return parquet_reader->init_reader(
                _all_required_col_names, _not_in_file_col_names, &_new_colname_to_value_range,
                conjuncts, tuple_descriptor, row_descriptor, colname_to_slot_id,
                not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
