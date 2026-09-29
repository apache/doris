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

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "exec/common/variant_util.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/nested_group_routing_plan.h"
#include "storage/segment/variant/variant_statistics.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace segment_v2 {

class ColumnWriter;
class ScalarColumnWriter;
class VariantV2ColumnWriter;
class VariantShredder;
struct VariantShreddedColumns;

// Write already serialized binary data of variant columns into storage.
class VariantBinaryWriter {
public:
    virtual ~VariantBinaryWriter() = default;
    virtual Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                        const ColumnWriterOptions& opts, SegmentFooterPB* footer) = 0;
    virtual Status append_shredded(const VariantShreddedColumns& shredded, size_t num_rows) = 0;
    virtual Status finish() = 0;
    virtual Status write_data() = 0;
    virtual Status write_ordinal_index() = 0;
    virtual Status write_zone_map() = 0;
    virtual Status write_inverted_index() = 0;
    virtual Status write_bloom_filter_index() = 0;
    virtual uint64_t estimate_buffer_size() const = 0;
    virtual void merge_stats_to(VariantStatistics* stats) const = 0;
};

class VariantDocWriter : public VariantBinaryWriter {
public:
    ~VariantDocWriter() override = default;
    Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                const ColumnWriterOptions& opts, SegmentFooterPB* footer) override;
    Status append_shredded(const VariantShreddedColumns& shredded, size_t num_rows) override;
    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    uint64_t estimate_buffer_size() const override;
    void merge_stats_to(VariantStatistics* stats) const override;

private:
    const TabletColumn* _parent_column = nullptr;
    ColumnWriterOptions _opts;
    int _bucket_num = 0;
    std::vector<std::unique_ptr<ColumnWriter>> _doc_value_column_writers;
    std::vector<ColumnWriterOptions> _doc_value_column_opts;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<TabletIndexes> _subcolumns_indexes;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
    VariantStatistics _stats;
};

// Unifies writing of Variant sparse data in two modes:
// 1) Single sparse column: one Map(String,String) column `__DORIS_VARIANT_SPARSE__`
// 2) Bucketized sparse columns: N Map columns `__DORIS_VARIANT_SPARSE__.b{i}`
//
// Responsibilities:
// - Initialize column writers and metas (consuming column_id identically to previous logic)
// - Append the shredder's binary buckets to their writers
// - Emit per-column (or per-bucket) sparse path statistics and set meta num_rows
class UnifiedSparseColumnWriter : public VariantBinaryWriter {
public:
    ~UnifiedSparseColumnWriter() override = default;
    Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                const ColumnWriterOptions& opts, SegmentFooterPB* footer) override;
    Status append_shredded(const VariantShreddedColumns& shredded, size_t num_rows) override;
    uint64_t estimate_buffer_size() const override;
    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    void merge_stats_to(VariantStatistics* stats) const override;

private:
    // Initialize single sparse column writer and consume one column_id.
    Status init_single(const TabletColumn& sparse_column, int& column_id,
                       const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    // Initialize N bucket writers and consume N column_ids.
    Status init_buckets(int bucket_num, const TabletColumn& parent_column, int& column_id,
                        const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    // Single sparse writer and its options/meta
    std::unique_ptr<ColumnWriter> _single_writer;
    ColumnWriterOptions _single_opts;
    // Bucketized sparse writers and their options/metas (size == bucket_num)
    std::vector<std::unique_ptr<ColumnWriter>> _bucket_writers;
    std::vector<ColumnWriterOptions> _bucket_opts;
    int _bucket_num = 0;
    VariantStatistics _stats;
};

// VARIANT storage accepts only the V2 execution column and persists the compatible shredded layout.
class VariantColumnWriterImpl {
public:
    VariantColumnWriterImpl(ColumnWriterOptions opts, const TabletColumn* column);
    ~VariantColumnWriterImpl();

    Status finalize();
    Status init();
    bool is_finalized() const;
    bool has_streaming_compaction_writer_for_test() const;

    // null_map is one byte per row (1 = null) or nullptr when the input column
    // is not nullable.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows, const uint8_t* null_map);

    Status finish();
    Status write_data();
    Status write_ordinal_index();
    Status write_zone_map();
    Status write_inverted_index();
    Status write_bloom_filter_index();
    uint64_t estimate_buffer_size();

private:
    Status _ensure_writer();

    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    bool _initialized = false;
    std::unique_ptr<VariantV2ColumnWriter> _v2_writer;
};

class VariantDocCompactWriter : public ColumnWriter {
public:
    explicit VariantDocCompactWriter(const ColumnWriterOptions& opts, TabletColumnPtr column);

    ~VariantDocCompactWriter() override;

    Status init() override;
    bool is_finalized() const { return _is_finalized; }

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;

    Status write_zone_map() override;

    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _next_rowid; }

    uint64_t get_raw_data_bytes() const override {
        return 0; // TODO
    }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return 0; // TODO
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return 0; // TODO
    }

    Status append_nulls(size_t num_rows) override {
        return Status::NotSupported("variant writer can not append_nulls");
    }

    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    Status finish_current_page() override {
        return Status::NotSupported("variant writer has no data, can not finish_current_page");
    }

    Status finalize();

private:
    Status _append(const IColumn& column, size_t row_pos, size_t num_rows, const uint8_t* null_map);
    Status _ensure_input_format(const IColumn& column);
    Status _initialize_v2_shredder();
    Status _write_materialized_subcolumns(const TabletColumn& parent_column,
                                          const VariantShreddedColumns& shredded, size_t num_rows,
                                          int& column_id);
    Status _write_doc_value_column(const TabletColumn& parent_column, int bucket_value,
                                   const ColumnPtr& source_column, int column_id, size_t num_rows);
    Status _finalize_v2(const TabletColumn& parent_column, size_t num_rows, int& column_id);

    ordinal_t _next_rowid = 0;
    VariantWriterInputFormat _input_format = VariantWriterInputFormat::UNSET;
    std::unique_ptr<VariantShredder> _v2_shredder;
    size_t _num_rows = 0;
    ColumnWriterOptions _opts;
    bool _is_finalized = false;
    bool _data_written = false;
    std::unique_ptr<ColumnWriter> _doc_value_column_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<TabletIndexes> _subcolumns_indexes;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
};

// Legacy test/helper entrypoint. New writer code should use
// variant_writer_helpers::init_column_meta directly.
void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       const ColumnWriterOptions& opts);

} // namespace segment_v2
} // namespace doris
