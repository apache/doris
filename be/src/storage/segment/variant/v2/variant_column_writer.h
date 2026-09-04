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
#include <span>
#include <vector>

#include "common/status.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "storage/segment/column_writer.h"

namespace doris {

class OlapBlockDataConvertor;
struct VariantColumnData;

namespace segment_v2 {

class VariantBinaryWriter;
class VariantShredder;
struct VariantShreddedColumns;

// Native storage writer for ColumnVariantV2. Full values are incrementally shredded at each append;
// a parent with extracted children retains only its root JSONB column. The writer never retains a
// segment-sized copy of the input execution column.
class VariantV2ColumnWriter {
public:
    VariantV2ColumnWriter(ColumnWriterOptions opts, const TabletColumn* column);
    ~VariantV2ColumnWriter();

    Status init();
    Status append(const VariantColumnData& column, size_t num_rows,
                  std::span<const uint8_t> outer_nulls);
    Status finalize();
    bool is_finalized() const { return _is_finalized; }

    Status finish();
    Status write_data();
    Status write_ordinal_index();
    Status write_zone_map();
    Status write_inverted_index();
    Status write_bloom_filter_index();
    uint64_t estimate_buffer_size();

private:
    Status _write_root(const IColumn* root_jsonb, int& column_id);
    Status _write_materialized(const VariantShreddedColumns& shredded,
                               OlapBlockDataConvertor* converter, int& column_id);
    Status _write_binary(const VariantShreddedColumns& shredded, OlapBlockDataConvertor* converter,
                         int& column_id);
    Status _for_each_column_writer(const std::function<Status(ColumnWriter*)>& function);

    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    std::unique_ptr<VariantShredder> _shredder;
    ColumnString::MutablePtr _root_jsonb;
    ColumnUInt8::MutablePtr _outer_nulls;
    size_t _num_rows = 0;
    bool _root_only = false;
    bool _is_finalized = false;

    std::unique_ptr<ColumnWriter> _root_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
    std::vector<TabletIndexes> _subcolumn_indexes;
    std::unique_ptr<VariantBinaryWriter> _binary_writer;
};

} // namespace segment_v2
} // namespace doris
