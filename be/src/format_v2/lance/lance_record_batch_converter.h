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

#include <cctz/time_zone.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/data_type_serde/data_type_serde.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/table_reader.h"

namespace arrow {
class Array;
class MemoryPool;
class RecordBatch;
class Schema;
} // namespace arrow

namespace doris {
class RuntimeState;
}

namespace doris::format::lance {

enum class SearchKind { NORMAL, VECTOR, FULL_TEXT };

// Bridges one Lance Arrow record-batch stream into an existing Doris Block. It owns the stable
// output-column metadata and lazily binds an incoming Arrow schema once, so ordinary batches only
// perform positional conversion. Each Arrow stream has an independent schema binding.
class LanceRecordBatchConverter {
public:
    Status init(RuntimeState* runtime_state, const std::vector<ColumnDefinition>& projected_columns,
                SearchKind search_kind);

    Status convert_record_batch_to_block(
            const std::shared_ptr<arrow::RecordBatch>& record_batch, Block* block,
            const std::optional<GlobalRowIdContext>& global_rowid_context, size_t* rows);

    bool requires_global_rowid() const { return _global_rowid_output_index.has_value(); }
    bool is_global_rowid_output(size_t output_index) const {
        return _global_rowid_output_index.has_value() &&
               *_global_rowid_output_index == output_index;
    }
    void reset_schema();

private:
    struct OutputColumn {
        std::string name;
        DataTypeSerDeSPtr serde;
    };

    static arrow::MemoryPool* _query_arrow_memory_pool(RuntimeState* runtime_state);
    Status _bind_schema(const std::shared_ptr<arrow::Schema>& schema);
    Status _append_global_row_ids(const std::shared_ptr<arrow::Array>& row_ids,
                                  MutableColumnPtr& output_column,
                                  const GlobalRowIdContext& context) const;

    SearchKind _search_kind = SearchKind::NORMAL;
    cctz::time_zone _timezone;
    arrow::MemoryPool* _memory_pool = nullptr;
    std::vector<OutputColumn> _output_columns;
    std::unordered_map<std::string, size_t> _output_index_by_input_name;
    std::optional<size_t> _global_rowid_output_index;
    std::shared_ptr<arrow::Schema> _bound_schema;
    std::vector<size_t> _arrow_column_to_output_index;
    std::vector<LanceArrowArrayNormalizer> _array_normalizer_for_arrow_column;
};

} // namespace doris::format::lance
