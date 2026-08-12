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

#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris {

class OlapBlockDataConvertor;
struct VariantColumnData;

namespace segment_v2 {

class VariantShredder;
enum class VariantShredderPhysicalLayout : uint8_t;
struct VariantShredderAppendStats;
struct VariantShredderOptions;

namespace variant_writer_helpers {

bool has_extracted_variant_columns(const TabletSchema& tablet_schema, int parent_column_unique_id);

Status validate_variant_v2_writer_layout(const TabletSchema& tablet_schema,
                                         const TabletColumn& parent_column);

Status make_variant_shredder_options(const TabletSchema& tablet_schema,
                                     const TabletColumn& parent_column,
                                     VariantShredderPhysicalLayout physical_layout,
                                     VariantShredderOptions* options);

Status classify_variant_writer_input(const VariantColumnData& column,
                                     VariantWriterInputFormat current_format,
                                     std::string_view writer_description,
                                     VariantWriterInputFormat* input_format);

Status append_variant_v2_to_shredder(VariantShredder* shredder, const VariantColumnData& column,
                                     size_t num_rows, std::span<const uint8_t> outer_nulls,
                                     VariantShredderAppendStats* append_stats = nullptr);
void record_variant_v2_shredded_writer_stats(const VariantShredderAppendStats& append_stats);

void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                      const ColumnWriterOptions& opts);

Status create_column_writer(uint32_t cid, const TabletColumn& column,
                            const TabletSchemaSPtr& tablet_schema,
                            IndexFileWriter* inverted_index_file_writer,
                            std::unique_ptr<ColumnWriter>* writer, TabletIndexes& subcolumn_indexes,
                            ColumnWriterOptions* opt, int64_t none_null_value_size,
                            bool need_record_none_null_value_size);

Status convert_and_write_column(OlapBlockDataConvertor* converter, const TabletColumn& column,
                                DataTypePtr data_type, ColumnWriter* writer,
                                const ColumnPtr& src_column, size_t num_rows, int column_id);

// Converts only present values. Missing logical rows are appended as NULL; ARRAY uses a one-path
// full-column fallback because its converted representation is not fixed-width/strided.
Status append_sparse_converted_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                      OlapBlockDataConvertor* converter, int column_id,
                                      const DataTypePtr& type, const ColumnPtr& values_column,
                                      std::span<const uint32_t> rowids, size_t total_rows);

void maybe_remove_root_jsonb_with_empty_defaults(MutableColumnPtr* root_column, size_t num_rows,
                                                 bool remove_root_jsonb);

Status prepare_subcolumn_writer_target(
        const ColumnWriterOptions& base_opts, const TabletColumn& parent_column,
        int current_column_id, const PathInData& relative_path, const DataTypePtr& current_type,
        int64_t none_null_value_size, size_t num_rows,
        const TabletSchema::SubColumnInfo* existing_subcolumn_info, bool check_storage_type,
        TabletIndexes* out_subcolumn_indexes, ColumnWriterOptions* out_subcolumn_opts,
        std::unique_ptr<ColumnWriter>* out_writer, TabletColumn* out_tablet_column);

} // namespace variant_writer_helpers

} // namespace segment_v2
} // namespace doris
