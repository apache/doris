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

namespace doris::segment_v2 {

class ColumnWriter;
class ScalarColumnWriter;

class VariantColumnWriterImpl {
public:
    VariantColumnWriterImpl(const ColumnWriterOptions& opts, const TabletColumn* column);
    Status finalize();

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
    void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column);

    Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                                 const TabletColumn& parent_column,
                                 const TabletSchemaSPtr& tablet_schema);
    // prepare a column for finalize
    doris::vectorized::MutableColumnPtr _column;
    doris::vectorized::MutableColumnPtr _null_column;
    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    bool _is_finalized = false;
    // for sparse column and root column
    std::unique_ptr<ColumnWriter> _root_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
};
} // namespace doris::segment_v2