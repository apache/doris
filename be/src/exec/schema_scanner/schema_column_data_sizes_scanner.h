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

#include <gen_cpp/Descriptors_types.h>

#include <memory>
#include <vector>

#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

class SchemaColumnDataSizesScanner : public SchemaScanner {
public:
    SchemaColumnDataSizesScanner();
    ~SchemaColumnDataSizesScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

    static std::unique_ptr<SchemaScanner> create_unique() {
        return std::make_unique<SchemaColumnDataSizesScanner>();
    }

private:
    void _collect_column_data_sizes_from_rowsets(const std::vector<RowsetSharedPtr>& rowsets,
                                                 int64_t table_id, int64_t index_id,
                                                 int64_t partition_id, int64_t tablet_id);
    Status _get_all_column_data_sizes();
    Status _fill_block_impl(vectorized::Block* block);

    struct ColumnDataSizeInfo {
        int64_t backend_id;
        int64_t table_id;
        int64_t index_id;
        int64_t partition_id;
        int64_t tablet_id;
        std::string rowset_id;
        uint32_t column_unique_id;
        std::string column_name;
        std::string column_type;
        uint64_t compressed_data_bytes;
        uint64_t uncompressed_data_bytes;
        uint64_t raw_data_bytes;
    };

    static std::vector<SchemaScanner::ColumnDesc> _s_tbls_columns;

    int64_t backend_id_;
    std::vector<ColumnDataSizeInfo> _column_data_sizes;
    size_t _column_data_sizes_idx;
};

} // namespace doris