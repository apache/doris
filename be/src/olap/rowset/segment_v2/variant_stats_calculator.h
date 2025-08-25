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

#include <string>
#include <unordered_map>

#include "gen_cpp/segment_v2.pb.h"
#include "olap/tablet_schema.h"
#include "vec/core/block.h"

namespace doris::segment_v2 {

class VariantStatsCaculator {
public:
    explicit VariantStatsCaculator(SegmentFooterPB* footer, TabletSchemaSPtr tablet_schema,
                                   const std::vector<uint32_t>& column_ids);

    // Calculate variant statistics for the given column and block
    Status calculate_variant_stats(const vectorized::Block* block, size_t row_pos, size_t num_rows);

private:
    // Map from column path to footer column index for fast lookup
    std::unordered_map<int32_t, std::unordered_map<std::string, size_t>> _path_to_footer_index;

    // Reference to the footer where we store the statistics
    SegmentFooterPB* _footer;
    TabletSchemaSPtr _tablet_schema;
    std::vector<uint32_t> _column_ids;

    // Helper method to calculate sparse column statistics
    void _calculate_sparse_column_stats(const vectorized::IColumn& column,
                                        ColumnMetaPB* column_meta,
                                        size_t max_sparse_column_statistics_size, size_t row_pos,
                                        size_t num_rows);

    // Helper method to calculate sub column statistics
    void _calculate_sub_column_stats(const vectorized::IColumn& column, ColumnMetaPB* column_meta,
                                     size_t row_pos, size_t num_rows);
};

} // namespace doris::segment_v2