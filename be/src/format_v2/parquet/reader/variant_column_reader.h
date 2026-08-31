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
#include <optional>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"

namespace doris::format::parquet {

struct ParquetColumnSchema;

// Projection-aligned view of the file schema. Complex nodes contain only the children decoded by
// NativeColumnReader. A VARIANT node may own a validated fully-shredded physical leaf projection.
struct VariantMaterializationNode {
    const ParquetColumnSchema* schema = nullptr;
    std::vector<std::unique_ptr<VariantMaterializationNode>> children;
    bool contains_variant = false;
    std::optional<format::LocalColumnIndex> variant_projection;
    std::shared_ptr<const ParquetColumnSchema> variant_state_schema;
};

// Builds the immutable schema retained by a shredded state in the exact order of the decoded
// physical projection.
std::shared_ptr<const ParquetColumnSchema> create_variant_state_schema(
        const ParquetColumnSchema& schema, const format::LocalColumnIndex* projection = nullptr);

// Converts one physical Parquet Variant wrapper column to ColumnVariantV2 and appends it to output.
// SQL NULL is represented by the wrapper's outer null map; a present wrapper with neither value nor
// typed_value is the Variant null value.
Status materialize_variant_rows(const ParquetColumnSchema& schema, const IColumn& physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile = {});
Status materialize_variant_rows(const ParquetColumnSchema& schema, ColumnPtr physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile = {});

// Recursively replaces projected VARIANT nodes inside STRUCT/LIST/MAP columns while preserving the
// surrounding column shape, offsets, and null maps. The destination is unchanged on decode errors.
Status materialize_variant_columns(const VariantMaterializationNode& plan, const IColumn& physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile = {});
Status materialize_variant_columns(const VariantMaterializationNode& plan, ColumnPtr physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile = {});

} // namespace doris::format::parquet
