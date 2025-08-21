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

#include "olap/rowset/segment_v2/variant_stats_calculator.h"

#include <gen_cpp/segment_v2.pb.h>

#include "common/logging.h"
#include "util/simd/bits.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/schema_util.h"

namespace doris::segment_v2 {

VariantStatsCaculator::VariantStatsCaculator(SegmentFooterPB* footer,
                                             TabletSchemaSPtr tablet_schema,
                                             const std::vector<uint32_t>& column_ids)
        : _footer(footer), _tablet_schema(tablet_schema), _column_ids(column_ids) {
    // Build the path to footer index mapping during initialization
    for (size_t i = 0; i < _footer->columns_size(); ++i) {
        const auto& column = _footer->columns(i);
        // path that need to record stats
        if (column.has_column_path_info() &&
            column.column_path_info().parrent_column_unique_id() > 0) {
            _path_to_footer_index[column.column_path_info().parrent_column_unique_id()]
                                 [column.column_path_info().path()] = i;
        }
    }
}

Status VariantStatsCaculator::calculate_variant_stats(const vectorized::Block* block,
                                                      size_t row_pos, size_t num_rows) {
    for (size_t i = 0; i < block->columns(); ++i) {
        const TabletColumn& tablet_column = _tablet_schema->column(_column_ids[i]);
        // Only process sub columns and sparse columns during compaction
        if (tablet_column.has_path_info() && tablet_column.path_info_ptr()->need_record_stats() &&
            tablet_column.parent_unique_id() > 0) {
            const std::string& column_path = tablet_column.path_info_ptr()->get_path();
            // Find the parent column in footer
            auto it = _path_to_footer_index.find(tablet_column.parent_unique_id());
            if (it == _path_to_footer_index.end()) {
                return Status::NotFound("Column path not found in footer: {}",
                                        tablet_column.path_info_ptr()->get_path());
            }
            size_t footer_index = it->second[column_path];
            ColumnMetaPB* column_meta = _footer->mutable_columns(footer_index);

            // Get the column from the block
            const auto& column = block->get_by_position(i).column;

            // Check if this is a sparse column or sub column
            if (column_path.ends_with("__DORIS_VARIANT_SPARSE__")) {
                // This is a sparse column from variant column
                _calculate_sparse_column_stats(
                        *column, column_meta,
                        tablet_column.variant_max_sparse_column_statistics_size(), row_pos,
                        num_rows);
            } else {
                // This is a sub column from variant column
                _calculate_sub_column_stats(*column, column_meta, row_pos, num_rows);
            }
        }
    }
    return Status::OK();
}

void VariantStatsCaculator::_calculate_sparse_column_stats(const vectorized::IColumn& column,
                                                           ColumnMetaPB* column_meta,
                                                           size_t max_sparse_column_statistics_size,
                                                           size_t row_pos, size_t num_rows) {
    // Get or create variant statistics
    VariantStatisticsPB* stats = column_meta->mutable_variant_statistics();

    // Use the same logic as the original calculate_variant_stats function
    vectorized::schema_util::VariantCompactionUtil::calculate_variant_stats(
            column, stats, max_sparse_column_statistics_size, row_pos, num_rows);

    VLOG_DEBUG << "Sparse column stats updated, non-null size count: "
               << stats->sparse_column_non_null_size_size();
}

void VariantStatsCaculator::_calculate_sub_column_stats(const vectorized::IColumn& column,
                                                        ColumnMetaPB* column_meta, size_t row_pos,
                                                        size_t num_rows) {
    // For sub columns, we need to calculate the non-null count
    const auto& nullable_column = assert_cast<const vectorized::ColumnNullable&>(column);
    const auto& null_data = nullable_column.get_null_map_data();
    const int8_t* start = reinterpret_cast<const int8_t*>(null_data.data()) + row_pos;

    // Count non-null values in the current block
    size_t current_non_null_count = simd::count_zero_num(start, num_rows);

    // Add to existing non-null count
    column_meta->set_none_null_size(current_non_null_count + column_meta->none_null_size());

    VLOG_DEBUG << "Sub column non-null count updated: " << column_meta->none_null_size()
               << " (added " << current_non_null_count << " from current block)";
}

} // namespace doris::segment_v2