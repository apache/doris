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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "core/column/column_array.h"
#include "core/column/column_vector.h"
#include "core/value/variant/variant_batch_builder.h"
#include "storage/segment/variant/v2/variant_assembler.h"

namespace doris::segment_v2::variant_v2::variant_assembler_detail {

// Borrowed materialized-column projection prepared once per batch. The nested record owns only
// another projection node, never the underlying batch column.
struct PreparedMaterializedColumn {
    const IColumn* data = nullptr;
    const uint8_t* nulls = nullptr;
    PrimitiveType primitive = INVALID_TYPE;
    uint8_t scale = 0;
    const ColumnArray* array = nullptr;
    std::unique_ptr<PreparedMaterializedColumn> nested;
    // Optional top-level batch owner. Only a fully clean mapped scalar lane receives one.
    MutableColumnPtr owner;

    bool is_null_at(size_t row) const noexcept { return nulls != nullptr && nulls[row] != 0; }
};

PreparedMaterializedColumn prepare_materialized_column(const DataTypePtr& type,
                                                       const IColumn* column, size_t rows);
bool is_materialized_value_visible(const PreparedMaterializedColumn& column, size_t row,
                                   bool preserve_logical_root);
Status append_materialized_value(const PreparedMaterializedColumn& column, size_t row,
                                 VariantBatchBuilder::Row& output, uint32_t depth);

} // namespace doris::segment_v2::variant_v2::variant_assembler_detail
