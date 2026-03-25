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

#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/nested_group_streaming_write_plan.h"
#include "storage/segment/variant/variant_statistics.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class ColumnVariant;
namespace segment_v2 {

#include "common/compile_check_begin.h"

class VariantStreamingCompactionWriter {
public:
    enum class Phase : uint8_t {
        UNINITIALIZED = 0,
        INITIALIZED = 1,
        APPENDING = 2,
        CLOSED = 3,
    };

    VariantStreamingCompactionWriter(const ColumnWriterOptions& opts, const TabletColumn* column,
                                     NestedGroupWriteProvider* nested_group_provider,
                                     VariantStatistics* statistics);

    Status init();
    Status append_data(const uint8_t** ptr, size_t num_rows, const uint8_t* outer_null_map);
    bool is_initialized() const { return _phase != Phase::UNINITIALIZED; }
    bool is_finalized() const { return _phase == Phase::CLOSED; }
    Phase phase() const { return _phase; }

    uint64_t estimate_buffer_size() const;
    Status finish();
    Status write_data();
    Status write_ordinal_index();
    Status write_zone_map();
    Status write_inverted_index();
    Status write_bloom_filter_index();

private:
    struct StreamingRegularSubcolumnWriter {
        NestedGroupStreamingRegularSubcolumnPlan plan;
        TabletColumn tablet_column;
        std::unique_ptr<OlapBlockDataConvertor> converter;
    };

    Status _for_each_column_writer(const std::function<Status(ColumnWriter*)>& func);
    Status _init_root_writer();
    Status _init_regular_subcolumn_writers(int& column_id);
    Status _append_input_from_raw(const uint8_t** ptr, size_t num_rows,
                                  const uint8_t* outer_null_map);
    Status _append_input(const ColumnVariant& src, size_t row_pos, size_t num_rows,
                         const uint8_t* outer_null_map);
    Status _append_chunk(const ColumnVariant& chunk_variant, const uint8_t* outer_null_map);
    Status _append_root_column(const ColumnVariant& chunk_variant, const uint8_t* outer_null_map);
    Status _append_regular_subcolumns(const ColumnVariant& chunk_variant);
    Status _check_initialized(std::string_view action) const;
    Status _check_closed(std::string_view action) const;

    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    NestedGroupWriteProvider* _nested_group_provider = nullptr;
    VariantStatistics* _statistics = nullptr;
    Phase _phase = Phase::UNINITIALIZED;

    std::unique_ptr<ColumnWriter> _root_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
    std::vector<TabletIndexes> _subcolumns_indexes;
    NestedGroupStreamingWritePlan _streaming_plan;
    std::vector<StreamingRegularSubcolumnWriter> _streaming_regular_subcolumn_writers;
};

#include "common/compile_check_end.h"

} // namespace segment_v2
} // namespace doris
