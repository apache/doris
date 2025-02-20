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

#include <gen_cpp/segment_v2.pb.h>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"

namespace doris {

namespace vectorized {
class ColumnObject;
class OlapBlockDataConvertor;
} // namespace vectorized
namespace segment_v2 {

class ColumnWriter;
class ScalarColumnWriter;

struct VariantStatistics {
    // If reached the size of this, we should stop writing statistics for sparse data
    constexpr static size_t MAX_SPARSE_DATA_STATISTICS_SIZE = 10000;
    std::map<std::string, size_t> subcolumns_non_null_size;
    std::map<std::string, size_t> sparse_column_non_null_size;

    void to_pb(VariantStatisticsPB* stats) const;
    void from_pb(const VariantStatisticsPB& stats);
};

class VariantColumnWriterImpl {
public:
    VariantColumnWriterImpl(const ColumnWriterOptions& opts, const TabletColumn* column);
    Status finalize();
    Status init();
    bool is_finalized() const;

    Status append_data(const uint8_t** ptr, size_t num_rows);

    Status finish();
    Status write_data();
    Status write_ordinal_index();
    Status write_zone_map();
    Status write_bitmap_index();
    Status write_inverted_index();
    Status write_bloom_filter_index();
    uint64_t estimate_buffer_size();
    Status append_nullable(const uint8_t* null_map, const uint8_t** ptr, size_t num_rows);

private:
    // not including root column
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column);

    // subcolumn path from variant stats info to distinguish from sparse column
    Status _get_subcolumn_paths_from_stats(std::set<std::string>& paths);

    Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                                 const TabletColumn& parent_column,
                                 const TabletSchemaSPtr& tablet_schema);
    Status _process_root_column(vectorized::ColumnObject* ptr,
                                vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                int& column_id);
    Status _process_sparse_column(vectorized::ColumnObject* ptr,
                                  vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                  int& column_id);
    Status _process_subcolumns(vectorized::ColumnObject* ptr,
                               vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                               int& column_id);
    // prepare a column for finalize
    doris::vectorized::MutableColumnPtr _column;
    doris::vectorized::MutableColumnPtr _null_column;
    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    bool _is_finalized = false;
    // for root column
    std::unique_ptr<ColumnWriter> _root_writer;
    // for sparse column
    std::unique_ptr<ColumnWriter> _sparse_column_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;

    // staticstics which will be persisted in the footer
    VariantStatistics _statistics;

    std::vector<std::unique_ptr<TabletIndex>> _subcolumns_indexes;
};
} // namespace segment_v2
} // namespace doris