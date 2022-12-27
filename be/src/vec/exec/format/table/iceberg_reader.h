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

#include <queue>

#include "table_format_reader.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

class IcebergTableReader : public TableFormatReader {
public:
    IcebergTableReader(GenericReader* file_format_reader, RuntimeProfile* profile,
                       RuntimeState* state, const TFileScanRangeParams& params);
    ~IcebergTableReader() override;

    Status init_row_filters(const TFileRangeDesc& range) override;

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContext*>& missing_columns) override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    enum { DATA, POSITION_DELETE, EQUALITY_DELETE };

private:
    struct IcebergProfile {
        RuntimeProfile::Counter* num_delete_files;
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
    };

    void _merge_sort(std::list<std::vector<int64_t>>& delete_rows_list, int64_t num_delete_rows);

    RuntimeProfile* _profile;
    RuntimeState* _state;
    const TFileScanRangeParams& _params;
    VExprContext* _data_path_conjunct_ctx = nullptr;
    IcebergProfile _iceberg_profile;
    std::vector<int64_t> _delete_rows;
};

} // namespace doris::vectorized
