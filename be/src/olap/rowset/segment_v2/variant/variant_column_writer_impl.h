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
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/rowset/segment_v2/variant/variant_statistics.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/common/schema_util.h"

namespace doris {

namespace vectorized {
class ColumnVariant;
class OlapBlockDataConvertor;
} // namespace vectorized
namespace segment_v2 {

#include "common/compile_check_begin.h"

class ColumnWriter;
class ScalarColumnWriter;

// Unifies writing of Variant sparse data in two modes:
// 1) Single sparse column: one Map(String,String) column `__DORIS_VARIANT_SPARSE__`
// 2) Bucketized sparse columns: N Map columns `__DORIS_VARIANT_SPARSE__.b{i}`
//
// Responsibilities:
// - Initialize column writers and metas (consuming column_id identically to previous logic)
// - Convert and append Variant's serialized sparse data to storage writers
// - Emit per-column (or per-bucket) sparse path statistics and set meta num_rows
//
// Invariants:
// - `_first_column_id` stores the column_id assigned to the first sparse meta created by init*
//   and is used to bind converter column ids deterministically.
class UnifiedSparseColumnWriter {
public:
    // Initialize single sparse column writer and consume one column_id.
    Status init_single(const TabletColumn& sparse_column, int& column_id,
                       const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    // Initialize N bucket writers and consume N column_ids.
    Status init_buckets(int bucket_num, const TabletColumn& parent_column, int& column_id,
                        const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    bool has_single() const { return static_cast<bool>(_single_writer); }
    bool has_buckets() const { return !_bucket_writers.empty(); }
    bool is_bucket_mode() const { return has_buckets(); }
    int bucket_num() const { return static_cast<int>(_bucket_writers.size()); }

    ColumnWriter* single_writer() const { return _single_writer.get(); }
    ColumnWriterOptions* single_opts() { return &_single_opts; }
    const ColumnWriterOptions* single_opts() const { return &_single_opts; }

    std::vector<std::unique_ptr<ColumnWriter>>& bucket_writers() { return _bucket_writers; }
    const std::vector<std::unique_ptr<ColumnWriter>>& bucket_writers() const {
        return _bucket_writers;
    }
    std::vector<ColumnWriterOptions>& bucket_opts() { return _bucket_opts; }
    const std::vector<ColumnWriterOptions>& bucket_opts() const { return _bucket_opts; }

    // Delegate lifecycle operations to the underlying writer(s).
    uint64_t estimate_buffer_size() const;
    Status finish();
    Status write_data();
    Status write_ordinal_index();

    // Append data from ColumnVariant into single/bucket writers and fill statistics/meta.
    // - For single mode: updates out_statistics and writes stats to single meta, sets num_rows
    // - For bucket mode: writes per-bucket stats to each bucket meta, sets num_rows
    // Convert and append Variant's sparse data:
    // - Single mode: convert src.get_sparse_column() and append to the single writer, populate
    //   out_stats (merged) and write stats to meta
    // - Bucket mode: materialize N ColumnMap temporaries, distribute entries by
    //   schema_util::variant_sparse_shard_of(path), convert and append per bucket, and write
    //   per-bucket stats to metas
    Status append_from_variant(const vectorized::ColumnVariant& src, size_t num_rows,
                               vectorized::OlapBlockDataConvertor* converter,
                               const TabletColumn& tablet_column, VariantStatistics* out_stats);

private:
    // Single sparse writer and its options/meta
    std::unique_ptr<ColumnWriter> _single_writer;
    ColumnWriterOptions _single_opts;
    // Bucketized sparse writers and their options/metas (size == bucket_num)
    std::vector<std::unique_ptr<ColumnWriter>> _bucket_writers;
    std::vector<ColumnWriterOptions> _bucket_opts;
    // remember assigned column ids for conversion
    int _first_column_id = -1;
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
    Status _process_root_column(vectorized::ColumnVariant* ptr,
                                vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                int& column_id);
    Status _process_sparse_column(vectorized::ColumnVariant* ptr,
                                  vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                  int& column_id);
    Status _process_subcolumns(vectorized::ColumnVariant* ptr,
                               vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                               int& column_id);
    // prepare a column for finalize
    doris::vectorized::MutableColumnPtr _column;
    doris::vectorized::ColumnUInt8 _null_column;
    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    bool _is_finalized = false;
    // for root column
    std::unique_ptr<ColumnWriter> _root_writer;
    // unified sparse writers (single or bucket mode)
    UnifiedSparseColumnWriter _sparse_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;

    // staticstics which will be persisted in the footer
    VariantStatistics _statistics;

    // hold the references of subcolumns indexes
    std::vector<TabletIndexes> _subcolumns_indexes;

    // hold the references of subcolumns info
    std::unordered_map<std::string, TabletSchema::SubColumnInfo> _subcolumns_info;
};

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       CompressionTypePB compression_type);

#include "common/compile_check_end.h"

} // namespace segment_v2
} // namespace doris