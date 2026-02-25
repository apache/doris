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

#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/rowset/segment_v2/variant/variant_statistics.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/common/variant_util.h"

namespace doris {

namespace vectorized {
class ColumnVariant;
class OlapBlockDataConvertor;
} // namespace vectorized
namespace segment_v2 {

#include "common/compile_check_begin.h"

class ColumnWriter;
class ScalarColumnWriter;

// Write already serialized binary data of variant columns into storage.
class VariantBinaryWriter {
public:
    virtual ~VariantBinaryWriter() = default;
    virtual Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                        const ColumnWriterOptions& opts, SegmentFooterPB* footer) = 0;
    virtual Status append_data(const TabletColumn* parent_column,
                               const vectorized::ColumnVariant& src, size_t num_rows,
                               vectorized::OlapBlockDataConvertor* converter) = 0;
    virtual Status finish() = 0;
    virtual Status write_data() = 0;
    virtual Status write_ordinal_index() = 0;
    virtual Status write_zone_map() = 0;
    virtual Status write_inverted_index() = 0;
    virtual Status write_bloom_filter_index() = 0;
    virtual uint64_t estimate_buffer_size() const = 0;
};

class VariantDocWriter : public VariantBinaryWriter {
public:
    ~VariantDocWriter() override = default;
    Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                const ColumnWriterOptions& opts, SegmentFooterPB* footer) override;
    Status append_data(const TabletColumn* parent_column, const vectorized::ColumnVariant& src,
                       size_t num_rows, vectorized::OlapBlockDataConvertor* converter) override;
    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    uint64_t estimate_buffer_size() const override;

private:
    Status _write_materialized_subcolumn(const TabletColumn& parent_column, std::string_view path,
                                         vectorized::ColumnVariant::Subcolumn& subcolumn,
                                         size_t num_rows,
                                         vectorized::OlapBlockDataConvertor* converter,
                                         int& column_id, const std::vector<uint32_t>* rowids);
    Status _write_doc_value_column(
            const TabletColumn& parent_column, const vectorized::ColumnVariant& src,
            size_t num_rows, vectorized::OlapBlockDataConvertor* converter,
            const phmap::flat_hash_map<StringRef, uint32_t, StringRefHash>& column_stats);

    const TabletColumn* _parent_column = nullptr;
    ColumnWriterOptions _opts;
    int _bucket_num = 0;
    int _first_column_id = -1;
    std::vector<std::unique_ptr<ColumnWriter>> _doc_value_column_writers;
    std::vector<ColumnWriterOptions> _doc_value_column_opts;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<TabletIndexes> _subcolumns_indexes;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
};

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
// - Single mode: convert src.get_sparse_column() and append to the single writer, populate
//   out_stats (merged) and write stats to meta
// - Bucket mode: materialize N ColumnMap temporaries, distribute entries by
//   variant_util::variant_sparse_shard_of(path), convert and append per bucket, and write
//   per-bucket stats to metas
class UnifiedSparseColumnWriter : public VariantBinaryWriter {
public:
    ~UnifiedSparseColumnWriter() override = default;
    Status init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                const ColumnWriterOptions& opts, SegmentFooterPB* footer) override;
    Status append_data(const TabletColumn* parent_column, const vectorized::ColumnVariant& src,
                       size_t num_rows, vectorized::OlapBlockDataConvertor* converter) override;
    uint64_t estimate_buffer_size() const override;
    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;

private:
    // Initialize single sparse column writer and consume one column_id.
    Status init_single(const TabletColumn& sparse_column, int& column_id,
                       const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    // Initialize N bucket writers and consume N column_ids.
    Status init_buckets(int bucket_num, const TabletColumn& parent_column, int& column_id,
                        const ColumnWriterOptions& base_opts, SegmentFooterPB* footer);

    Status append_single_sparse(const vectorized::ColumnVariant& src, size_t num_rows,
                                vectorized::OlapBlockDataConvertor* converter,
                                const TabletColumn& parent_column);

    Status append_bucket_sparse(const vectorized::ColumnVariant& src, size_t num_rows,
                                vectorized::OlapBlockDataConvertor* converter,
                                const TabletColumn& parent_column);

    // Single sparse writer and its options/meta
    std::unique_ptr<ColumnWriter> _single_writer;
    ColumnWriterOptions _single_opts;
    // Bucketized sparse writers and their options/metas (size == bucket_num)
    std::vector<std::unique_ptr<ColumnWriter>> _bucket_writers;
    std::vector<ColumnWriterOptions> _bucket_opts;
    // remember assigned column ids for conversion
    int _first_column_id = -1;
    int _bucket_num = 0;
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
    Status _process_doc_value_column(vectorized::ColumnVariant* ptr,
                                     vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                     int& column_id);

    Status _process_binary_column(vectorized::ColumnVariant* ptr,
                                  vectorized::OlapBlockDataConvertor* converter, size_t num_rows,
                                  int& column_id);
    // prepare a column for finalize
    doris::vectorized::ColumnVariant::MutablePtr _column;
    doris::vectorized::ColumnUInt8::MutablePtr _null_column;
    ColumnWriterOptions _opts;
    const TabletColumn* _tablet_column = nullptr;
    bool _is_finalized = false;
    // for root column
    std::unique_ptr<ColumnWriter> _root_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
    std::unique_ptr<VariantBinaryWriter> _binary_writer;
    // hold the references of subcolumns indexes
    std::vector<TabletIndexes> _subcolumns_indexes;

    // hold the references of subcolumns info
    std::unordered_map<std::string, TabletSchema::SubColumnInfo> _subcolumns_info;
};

class VariantDocCompactWriter : public ColumnWriter {
public:
    explicit VariantDocCompactWriter(const ColumnWriterOptions& opts, const TabletColumn* column,
                                     std::unique_ptr<Field> field);

    ~VariantDocCompactWriter() override = default;

    Status init() override;
    bool is_finalized() const { return _is_finalized; }

    Status append_data(const uint8_t** ptr, size_t num_rows) override;

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
    Status append_nullable(const uint8_t* null_map, const uint8_t** ptr, size_t num_rows) override;

    Status finish_current_page() override {
        return Status::NotSupported("variant writer has no data, can not finish_current_page");
    }

    Status finalize();

private:
    Status _write_materialized_subcolumn(const TabletColumn& parent_column, std::string_view path,
                                         vectorized::ColumnVariant::Subcolumn& subcolumn,
                                         size_t num_rows,
                                         vectorized::OlapBlockDataConvertor* converter,
                                         int& column_id, const std::vector<uint32_t>* rowids);
    Status _write_doc_value_column(const TabletColumn& parent_column,
                                   vectorized::ColumnVariant* variant_column,
                                   vectorized::OlapBlockDataConvertor* converter, int column_id,
                                   size_t num_rows);

    ordinal_t _next_rowid = 0;
    vectorized::MutableColumnPtr _column;
    const TabletColumn* _tablet_column = nullptr;
    ColumnWriterOptions _opts;
    bool _is_finalized = false;
    std::unique_ptr<ColumnWriter> _doc_value_column_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _subcolumn_writers;
    std::vector<TabletIndexes> _subcolumns_indexes;
    std::vector<ColumnWriterOptions> _subcolumn_opts;
};

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       CompressionTypePB compression_type);

#include "common/compile_check_end.h"

} // namespace segment_v2
} // namespace doris
