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
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"

#include <fmt/core.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <set>

#include "common/config.h"
#include "common/status.h"
#include "exec/decompressor.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "util/simd/bits.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/common/variant_util.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       CompressionTypePB compression_type) {
    meta->Clear();
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                          compression_type);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
    }
};

Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                             const TabletSchemaSPtr& tablet_schema,
                             IndexFileWriter* inverted_index_file_writer,
                             std::unique_ptr<ColumnWriter>* writer,
                             TabletIndexes& subcolumn_indexes, ColumnWriterOptions* opt,
                             int64_t none_null_value_size, bool need_record_none_null_value_size) {
    _init_column_meta(opt->meta, cid, column, opt->compression_type);
    // no need to record none null value size for typed column or nested column, since it's compaction stage
    // will directly pick it as sub column
    if (need_record_none_null_value_size) {
        // record none null value size for statistics
        opt->meta->set_none_null_size(none_null_value_size);
    }
    opt->need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opt->need_bloom_filter = column.is_bf_column();
    const auto& parent_index = tablet_schema->inverted_indexs(column.parent_unique_id());

    // init inverted index
    // parent_index denotes the index of the entire variant column
    // while subcolumn_index denotes the current subcolumn's index
    if (segment_v2::IndexColumnWriter::check_support_inverted_index(column)) {
        auto init_opt_inverted_index = [&]() {
            DCHECK(!subcolumn_indexes.empty());
            for (const auto& index : subcolumn_indexes) {
                opt->inverted_indexes.push_back(index.get());
            }
            opt->need_inverted_index = true;
            DCHECK(inverted_index_file_writer != nullptr);
            opt->index_file_writer = inverted_index_file_writer;
        };

        // the subcolumn index is already initialized
        if (!subcolumn_indexes.empty()) {
            init_opt_inverted_index();
        }
        // the subcolumn index is not initialized, but the parent index is present
        else if (!parent_index.empty() &&
                 vectorized::variant_util::inherit_index(parent_index, subcolumn_indexes, column)) {
            init_opt_inverted_index();
        }
        // no parent index and no subcolumn index
        else {
            opt->need_inverted_index = false;
        }
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE)                     \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opt->need_zone_map = false;                           \
        opt->need_bloom_filter = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY)
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB)
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT)

#undef DISABLE_INDEX_IF_FIELD_TYPE

    RETURN_IF_ERROR(ColumnWriter::create(*opt, &column, opt->file_writer, writer));
    RETURN_IF_ERROR((*writer)->init());

    return Status::OK();
}

Status convert_and_write_column(vectorized::OlapBlockDataConvertor* converter,
                                const TabletColumn& column, vectorized::DataTypePtr data_type,
                                ColumnWriter* writer,

                                const vectorized::ColumnPtr& src_column, size_t num_rows,
                                int column_id) {
    converter->add_column_data_convertor(column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({src_column, data_type, ""},
                                                                       0, num_rows, column_id));
    auto [status, converted_column] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);

    const uint8_t* nullmap = converted_column->get_nullmap();
    RETURN_IF_ERROR(writer->append(nullmap, converted_column->get_data(), num_rows));

    converter->clear_source_content(column_id);
    return Status::OK();
}

namespace {
// Per-path sparse materialization result from a VARIANT doc-value column.
// - subcolumn stores the decoded values for rows listed in rowids (dense, no gaps).
// - rowids records which input rows have a non-null value for this path.
// - non_null_count is used to build variant statistics quickly.
struct SubcolumnSparseData {
    vectorized::ColumnVariant::Subcolumn subcolumn {0, true, false};
    std::vector<uint32_t> rowids;
    uint32_t non_null_count = 0;
};

using DocSparseSubcolumns = phmap::flat_hash_map<StringRef, SubcolumnSparseData, StringRefHash>;
using DocValuePathStats = phmap::flat_hash_map<StringRef, uint32_t, StringRefHash>;

struct SubcolumnWriteEntry {
    std::string_view path;
    vectorized::ColumnVariant::Subcolumn* subcolumn = nullptr;
    // nullptr means dense materialization; otherwise sparse row ids for this path.
    const std::vector<uint32_t>* rowids = nullptr;
};

struct SubcolumnWritePlan {
    using DenseSubcolumns =
            phmap::flat_hash_map<std::string_view, vectorized::ColumnVariant::Subcolumn>;

    // Owns materialized subcolumns and a flattened iteration view for finalize().
    DocSparseSubcolumns sparse_subcolumns;
    DenseSubcolumns dense_subcolumns;
    std::vector<SubcolumnWriteEntry> entries;
    DocValuePathStats stats;
};

// Build per-path non-null counts from the serialized doc-value representation.
void build_doc_value_stats(const vectorized::ColumnVariant& variant, DocValuePathStats* stats) {
    auto [column_key, column_value] = variant.get_doc_value_data_paths_and_values();
    (void)column_value;
    const auto& column_offsets = variant.serialized_doc_value_column_offsets();
    const size_t num_rows = column_offsets.size();

    stats->clear();
    stats->reserve(column_key->size());
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = column_offsets[row - 1];
        const size_t end = column_offsets[row];
        for (size_t j = start; j < end; ++j) {
            const StringRef path = column_key->get_data_at(j);
            auto it = stats->try_emplace(path, 0).first;
            ++it->second;
        }
    }
}

// Materialize sparse subcolumns for each path and build per-path non-null counts.
// For each row, we decode only present (path, value) pairs and append them to the
// corresponding subcolumn, while recording the row id to allow gap filling later.
void build_sparse_subcolumns_and_stats(const vectorized::ColumnVariant& variant,
                                       DocSparseSubcolumns* subcolumns, DocValuePathStats* stats) {
    auto [column_key, column_value] = variant.get_doc_value_data_paths_and_values();
    const auto& column_offsets = variant.serialized_doc_value_column_offsets();
    const size_t num_rows = column_offsets.size();

    subcolumns->clear();
    stats->clear();
    subcolumns->reserve(column_key->size());

    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = column_offsets[row - 1];
        const size_t end = column_offsets[row];
        for (size_t i = start; i < end; ++i) {
            const StringRef path = column_key->get_data_at(i);
            auto& data = subcolumns->try_emplace(path).first->second;
            data.rowids.push_back(cast_set<uint32_t>(row));
            data.subcolumn.deserialize_from_binary_column(column_value, i);
            ++data.non_null_count;
        }
    }

    stats->reserve(subcolumns->size());
    for (const auto& [path, data] : *subcolumns) {
        stats->try_emplace(path, data.non_null_count);
    }
}

SubcolumnWritePlan build_subcolumn_write_plan(const vectorized::ColumnVariant& variant,
                                              size_t num_rows,
                                              int64_t variant_doc_materialization_min_rows) {
    SubcolumnWritePlan plan;
    // Below threshold: skip materialization and let finalize() compute stats on demand.
    if (num_rows < static_cast<size_t>(variant_doc_materialization_min_rows)) {
        return plan;
    }

    if (config::enable_variant_doc_sparse_write_subcolumns) {
        build_sparse_subcolumns_and_stats(variant, &plan.sparse_subcolumns, &plan.stats);
        plan.entries.reserve(plan.sparse_subcolumns.size());
        for (auto& [path, sparse] : plan.sparse_subcolumns) {
            SubcolumnWriteEntry entry;
            // StringRef points to variant storage; valid for the plan's lifetime.
            entry.path = std::string_view(path.data, path.size);
            entry.subcolumn = &sparse.subcolumn;
            entry.rowids = &sparse.rowids;
            plan.entries.push_back(entry);
        }
        return plan;
    }

    build_doc_value_stats(variant, &plan.stats);
    plan.dense_subcolumns = vectorized::variant_util::materialize_docs_to_subcolumns_map(variant);
    plan.entries.reserve(plan.dense_subcolumns.size());
    for (auto& [path, subcolumn] : plan.dense_subcolumns) {
        SubcolumnWriteEntry entry;
        entry.path = path;
        entry.subcolumn = &subcolumn;
        entry.rowids = nullptr;
        plan.entries.push_back(entry);
    }
    return plan;
}

template <typename WriteMaterializedFn, typename WriteDocValueFn>
Status execute_doc_write_pipeline(const vectorized::ColumnVariant& variant, size_t num_rows,
                                  int64_t variant_doc_materialization_min_rows, int& column_id,
                                  WriteMaterializedFn&& write_materialized_fn,
                                  WriteDocValueFn&& write_doc_value_fn,
                                  DocValuePathStats* out_column_stats) {
    SubcolumnWritePlan plan =
            build_subcolumn_write_plan(variant, num_rows, variant_doc_materialization_min_rows);
    *out_column_stats = std::move(plan.stats);
    if (out_column_stats->empty()) {
        build_doc_value_stats(variant, out_column_stats);
    }

    for (auto& entry : plan.entries) {
        RETURN_IF_ERROR(write_materialized_fn(entry, column_id));
    }
    RETURN_IF_ERROR(write_doc_value_fn(column_id));
    return Status::OK();
}

bool is_invalid_materialized_subcolumn_type(const vectorized::DataTypePtr& type) {
    return vectorized::variant_util::get_base_type_of_array(type)->get_primitive_type() ==
           PrimitiveType::INVALID_TYPE;
}

Status prepare_materialized_subcolumn_writer(
        const TabletColumn& parent_column, std::string_view path,
        const vectorized::ColumnVariant::Subcolumn& subcolumn,
        vectorized::ColumnPtr& current_column, vectorized::DataTypePtr& current_type,
        int64_t none_null_value_size, int current_column_id, size_t num_rows,
        const ColumnWriterOptions& base_opts, std::vector<TabletIndexes>* subcolumns_indexes,
        std::vector<ColumnWriterOptions>* subcolumn_opts,
        std::vector<std::unique_ptr<ColumnWriter>>* subcolumn_writers,
        TabletColumn* out_tablet_column) {
    TabletColumn tablet_column;
    TabletIndexes subcolumn_indexes;
    TabletSchema::SubColumnInfo sub_column_info;
    if (vectorized::variant_util::generate_sub_column_info(*base_opts.rowset_ctx->tablet_schema,
                                                           parent_column.unique_id(),
                                                           std::string(path), &sub_column_info)) {
        tablet_column = std::move(sub_column_info.column);
        subcolumn_indexes = std::move(sub_column_info.indexes);
        vectorized::DataTypePtr storage_type =
                vectorized::DataTypeFactory::instance().create_data_type(tablet_column);
        if (!storage_type->equals(*current_type)) {
            RETURN_IF_ERROR(vectorized::variant_util::cast_column(
                    {current_column, current_type, ""}, storage_type, &current_column));
        }
        current_type = std::move(storage_type);
    } else {
        const std::string column_name = parent_column.name_lower_case() + "." + std::string(path);
        const vectorized::DataTypePtr& final_data_type_from_object =
                subcolumn.get_least_common_type();
        vectorized::PathInData full_path = vectorized::PathInData(column_name);
        tablet_column = vectorized::variant_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::variant_util::ExtraInfo {.unique_id = -1,
                                                     .parent_unique_id = parent_column.unique_id(),
                                                     .path_info = full_path});
        const auto& indexes =
                base_opts.rowset_ctx->tablet_schema->inverted_indexs(parent_column.unique_id());
        vectorized::variant_util::inherit_index(indexes, subcolumn_indexes, tablet_column);
    }

    ColumnWriterOptions opts;
    opts.meta = base_opts.footer->add_columns();
    opts.index_file_writer = base_opts.index_file_writer;
    opts.compression_type = base_opts.compression_type;
    opts.rowset_ctx = base_opts.rowset_ctx;
    opts.file_writer = base_opts.file_writer;
    std::unique_ptr<ColumnWriter> writer;
    vectorized::variant_util::inherit_column_attributes(parent_column, tablet_column);

    bool need_record_none_null_value_size = true;

    RETURN_IF_ERROR(_create_column_writer(
            current_column_id, tablet_column, base_opts.rowset_ctx->tablet_schema,
            base_opts.index_file_writer, &writer, subcolumn_indexes, &opts, none_null_value_size,
            need_record_none_null_value_size));
    opts.meta->set_num_rows(num_rows);
    subcolumns_indexes->push_back(std::move(subcolumn_indexes));
    subcolumn_opts->push_back(opts);
    subcolumn_writers->push_back(std::move(writer));
    *out_tablet_column = std::move(tablet_column);
    return Status::OK();
}

Status append_sparse_converted_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                      vectorized::OlapBlockDataConvertor* converter, int cid,
                                      const vectorized::DataTypePtr& type,
                                      const vectorized::ColumnPtr& values_column,
                                      const std::vector<uint32_t>& rowids, size_t total_rows);

Status write_materialized_subcolumn(const TabletColumn& parent_column, std::string_view path,
                                    vectorized::ColumnVariant::Subcolumn& subcolumn,
                                    size_t num_rows, vectorized::OlapBlockDataConvertor* converter,
                                    int& column_id, const ColumnWriterOptions& base_opts,
                                    std::vector<TabletIndexes>* subcolumns_indexes,
                                    std::vector<ColumnWriterOptions>* subcolumn_opts,
                                    std::vector<std::unique_ptr<ColumnWriter>>* subcolumn_writers,
                                    const std::vector<uint32_t>* rowids) {
    const auto& least_common_type = subcolumn.get_least_common_type();
    if (is_invalid_materialized_subcolumn_type(least_common_type)) {
        return Status::OK();
    }

    subcolumn.finalize();
    vectorized::ColumnPtr current_column = subcolumn.get_finalized_column_ptr()->get_ptr();
    vectorized::DataTypePtr current_type = subcolumn.get_least_common_type();
    if (rowids != nullptr) {
        DCHECK_EQ(current_column->size(), rowids->size());
        if (rowids->size() != num_rows && !current_type->is_nullable()) {
            current_type = vectorized::make_nullable(current_type);
            current_column = vectorized::make_nullable(current_column);
        }
    }

    const int current_column_id = column_id++;
    const int64_t none_null_value_size = subcolumn.get_non_null_value_size();
    TabletColumn tablet_column;
    RETURN_IF_ERROR(prepare_materialized_subcolumn_writer(
            parent_column, path, subcolumn, current_column, current_type, none_null_value_size,
            current_column_id, num_rows, base_opts, subcolumns_indexes, subcolumn_opts,
            subcolumn_writers, &tablet_column));

    (void)converter;
    auto subcolumn_converter = std::make_unique<vectorized::OlapBlockDataConvertor>();
    ColumnWriter* writer = subcolumn_writers->back().get();
    if (rowids != nullptr) {
        return append_sparse_converted_column(tablet_column, writer, subcolumn_converter.get(), 0,
                                              current_type, current_column, *rowids, num_rows);
    }

    return convert_and_write_column(subcolumn_converter.get(), tablet_column, current_type, writer,
                                    current_column, num_rows, 0);
}

// Convert a sparse (values_column, rowids) pair into storage format and append to writer.
// Missing rows (gaps between rowids) are appended as nulls, so the output has total_rows.
Status append_sparse_converted_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                      vectorized::OlapBlockDataConvertor* converter, int cid,
                                      const vectorized::DataTypePtr& type,
                                      const vectorized::ColumnPtr& values_column,
                                      const std::vector<uint32_t>& rowids, size_t total_rows) {
    DCHECK_EQ(values_column->size(), rowids.size());
    const size_t cell_size = writer->get_field()->size();

    auto base_type = type;
    if (base_type->is_nullable()) {
        base_type = assert_cast<const vectorized::DataTypeNullable&>(*base_type).get_nested_type();
    }
    if (base_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
        // ARRAY convertor output is not a contiguous “cell_size-strided” buffer. It is a pointer-based
        // structure (size/offsets/item_ptr/...), so slicing it via `data_base + cell_size * idx`
        // can lead to out-of-bounds access.
        // Also, filling gaps via `writer->append_nulls()` relies on an implicit offsets contract
        // (offsets must provide `num_rows + 1` entries), which is easy to violate and can trigger
        // memory issues.
        // To make the behavior safe and consistent, we first materialize a full column by filling
        // gaps, then convert once and write via `append_nullable()`, so offsets are generated by
        // the convertor in a unified way.
        vectorized::MutableColumnPtr full_column = values_column->clone_empty();
        full_column->reserve(total_rows);

        size_t next_row = 0;
        size_t value_idx = 0;
        while (value_idx < rowids.size()) {
            const size_t row = rowids[value_idx];
            DCHECK_GE(row, next_row);
            const size_t gap = row - next_row;
            if (gap > 0) {
                DCHECK(tablet_column.is_nullable());
                full_column->insert_many_defaults(gap);
            }

            size_t run_len = 1;
            while (value_idx + run_len < rowids.size() &&
                   rowids[value_idx + run_len] == row + run_len) {
                ++run_len;
            }
            full_column->insert_range_from(*values_column, value_idx, run_len);
            value_idx += run_len;
            next_row = row + run_len;
        }
        if (next_row < total_rows) {
            DCHECK(tablet_column.is_nullable());
            full_column->insert_many_defaults(total_rows - next_row);
        }

        converter->add_column_data_convertor(tablet_column);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {full_column->get_ptr(), type, ""}, 0, total_rows, cid));
        auto [st, converted] = converter->convert_column_data(cid);
        RETURN_IF_ERROR(st);
        const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(converted->get_data());
        RETURN_IF_ERROR(writer->append_nullable(converted->get_nullmap(), &data_ptr, total_rows));
        converter->clear_source_content(cid);
        return Status::OK();
    }

    if (rowids.empty()) {
        DCHECK(tablet_column.is_nullable());
        return writer->append_nulls(total_rows);
    }

    converter->add_column_data_convertor(tablet_column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({values_column, type, ""}, 0,
                                                                       rowids.size(), cid));
    auto [st, converted] = converter->convert_column_data(cid);
    RETURN_IF_ERROR(st);

    const uint8_t* nullmap_base = converted->get_nullmap();
    const uint8_t* data_base = reinterpret_cast<const uint8_t*>(converted->get_data());
    auto append_gaps = [&](size_t gap) -> Status {
        if (gap == 0) {
            return Status::OK();
        }
        DCHECK(tablet_column.is_nullable());
        return writer->append_nulls(gap);
    };

    size_t next_row = 0;
    size_t value_idx = 0;
    while (value_idx < rowids.size()) {
        const size_t row = rowids[value_idx];
        DCHECK_GE(row, next_row);
        RETURN_IF_ERROR(append_gaps(row - next_row));

        size_t run_len = 1;
        while (value_idx + run_len < rowids.size() &&
               rowids[value_idx + run_len] == row + run_len) {
            ++run_len;
        }

        const uint8_t* run_nullmap = nullmap_base ? nullmap_base + value_idx : nullptr;
        const uint8_t* run_data = data_base + (cell_size * value_idx);
        RETURN_IF_ERROR(writer->append(run_nullmap, run_data, run_len));

        value_idx += run_len;
        next_row = row + run_len;
    }

    RETURN_IF_ERROR(append_gaps(total_rows - next_row));
    converter->clear_source_content(cid);
    return Status::OK();
}
} // namespace

Status UnifiedSparseColumnWriter::init(const TabletColumn* parent_column, int bucket_num,
                                       int& column_id, const ColumnWriterOptions& base_opts,
                                       SegmentFooterPB* footer) {
    _bucket_num = std::max(1, bucket_num);
    if (_bucket_num <= 1) {
        TabletColumn sparse_column = vectorized::variant_util::create_sparse_column(*parent_column);
        RETURN_IF_ERROR(init_single(sparse_column, column_id, base_opts, footer));
    } else {
        RETURN_IF_ERROR(init_buckets(_bucket_num, *parent_column, column_id, base_opts, footer));
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::append_data(const TabletColumn* parent_column,
                                              const vectorized::ColumnVariant& src, size_t num_rows,
                                              vectorized::OlapBlockDataConvertor* converter) {
    if (_single_writer) {
        RETURN_IF_ERROR(append_single_sparse(src, num_rows, converter, *parent_column));
    } else {
        RETURN_IF_ERROR(append_bucket_sparse(src, num_rows, converter, *parent_column));
    }
    return Status::OK();
}

// UnifiedSparseColumnWriter implementation
Status UnifiedSparseColumnWriter::init_single(const TabletColumn& sparse_column, int& column_id,
                                              const ColumnWriterOptions& base_opts,
                                              SegmentFooterPB* footer) {
    _single_opts = base_opts;
    _single_opts.meta = footer->add_columns();
    _init_column_meta(_single_opts.meta, column_id, sparse_column, base_opts.compression_type);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_single_opts, &sparse_column,
                                                    base_opts.file_writer, &_single_writer));
    RETURN_IF_ERROR(_single_writer->init());
    _first_column_id = column_id;
    ++column_id;
    return Status::OK();
}

Status UnifiedSparseColumnWriter::init_buckets(int bucket_num, const TabletColumn& parent_column,
                                               int& column_id, const ColumnWriterOptions& base_opts,
                                               SegmentFooterPB* footer) {
    _bucket_writers.clear();
    _bucket_opts.clear();
    _bucket_writers.resize(bucket_num);
    _bucket_opts.resize(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        TabletColumn bucket_col =
                vectorized::variant_util::create_sparse_shard_column(parent_column, b);
        _bucket_opts[b] = base_opts;
        _bucket_opts[b].meta = footer->add_columns();
        _init_column_meta(_bucket_opts[b].meta, column_id, bucket_col, base_opts.compression_type);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(
                _bucket_opts[b], &bucket_col, base_opts.file_writer, &_bucket_writers[b]));
        RETURN_IF_ERROR(_bucket_writers[b]->init());
        if (b == 0) {
            _first_column_id = column_id;
        }
        ++column_id;
    }
    return Status::OK();
}

uint64_t UnifiedSparseColumnWriter::estimate_buffer_size() const {
    uint64_t size = 0;
    if (_single_writer) {
        size += _single_writer->estimate_buffer_size();
    }
    for (const auto& w : _bucket_writers) {
        if (w) {
            size += w->estimate_buffer_size();
        }
    }
    return size;
}

Status UnifiedSparseColumnWriter::finish() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->finish());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->finish());
        }
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_data() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->write_data());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->write_data());
        }
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_ordinal_index() {
    if (_single_writer) {
        RETURN_IF_ERROR(_single_writer->write_ordinal_index());
    }
    for (auto& w : _bucket_writers) {
        if (w) {
            RETURN_IF_ERROR(w->write_ordinal_index());
        }
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_zone_map() {
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_inverted_index() {
    return Status::OK();
}

Status UnifiedSparseColumnWriter::write_bloom_filter_index() {
    return Status::OK();
}

// Single sparse mode path:
// - Convert the pre-serialized sparse ColumnMap from the engine format
//   (src.get_sparse_column()) to storage format using converter, binding
//   to the column id allocated during init_single (stored in _first_column_id).
// - Append to the single writer and populate sparse path statistics into
//   out_stats and the single column meta.
Status UnifiedSparseColumnWriter::append_single_sparse(
        const vectorized::ColumnVariant& src, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, const TabletColumn& parent_column) {
    TabletColumn sparse_column = vectorized::variant_util::create_sparse_column(parent_column);
    converter->add_column_data_convertor(sparse_column);
    DCHECK_EQ(src.get_sparse_column()->size(), num_rows);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {src.get_sparse_column(), nullptr, ""}, 0, num_rows, _first_column_id));
    auto [status, column] = converter->convert_column_data(_first_column_id);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(_single_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    converter->clear_source_content(_first_column_id);

    // Build path frequency statistics with upper bound limit to avoid
    // large memory and metadata size. Persist to meta for readers.
    phmap::flat_hash_map<StringRef, size_t, StringRefHash> path_counts;
    const auto [paths, _] = src.get_sparse_data_paths_and_values();
    size_t limit = parent_column.variant_max_sparse_column_statistics_size();
    for (size_t i = 0; i != paths->size(); ++i) {
        auto k = paths->get_data_at(i);
        if (auto it = path_counts.find(k); it != path_counts.end())
            ++it->second;
        else if (path_counts.size() < limit)
            path_counts.emplace(k, 1);
    }
    segment_v2::VariantStatistics sparse_stats;
    for (const auto& [k, cnt] : path_counts) {
        sparse_stats.sparse_column_non_null_size.emplace(k.to_string(), cnt);
    }
    sparse_stats.to_pb(_single_opts.meta->mutable_variant_statistics());
    _single_opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

// Bucketized sparse mode path:
// - Materialize N temporary ColumnMap (keys, values, offsets)
// - For each row, distribute (path,value) pairs to the bucket decided by
//   variant_util::variant_sparse_shard_of(path)
// - Convert and append each bucket map to its writer using the column id
//   sequence initialized by init_buckets (starting at _first_column_id)
// - Compute per-bucket path stats and persist into each bucket's meta
Status UnifiedSparseColumnWriter::append_bucket_sparse(
        const vectorized::ColumnVariant& src, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, const TabletColumn& parent_column) {
    const int bucket_num = static_cast<int>(_bucket_writers.size());
    const auto [paths_col, values_col] = src.get_sparse_data_paths_and_values();
    const auto& offsets = src.serialized_sparse_column_offsets();
    phmap::flat_hash_map<StringRef, uint32_t, StringRefHash> path_to_bucket;
    path_to_bucket.reserve(std::min<size_t>(paths_col->size(), 8192));
    const size_t limit = parent_column.variant_max_sparse_column_statistics_size();
    std::vector<phmap::flat_hash_map<StringRef, size_t, StringRefHash>> bucket_path_counts(
            bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        bucket_path_counts[b].reserve(std::min<size_t>(limit, 1024));
    }

    std::vector<vectorized::MutableColumnPtr> tmp_maps(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        tmp_maps[b] = vectorized::ColumnMap::create(
                vectorized::ColumnString::create(), vectorized::ColumnString::create(),
                vectorized::ColumnArray::ColumnOffsets::create());
    }
    for (int b = 0; b < bucket_num; ++b) {
        auto& m = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        m.get_offsets().reserve(num_rows);
    }
    std::vector<vectorized::ColumnString*> bucket_keys(bucket_num);
    std::vector<vectorized::ColumnString*> bucket_values(bucket_num);
    std::vector<vectorized::ColumnArray::Offsets64*> bucket_offsets(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        auto& m = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        bucket_keys[b] = &assert_cast<vectorized::ColumnString&>(m.get_keys());
        bucket_values[b] = &assert_cast<vectorized::ColumnString&>(m.get_values());
        bucket_offsets[b] = &m.get_offsets();
    }
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = offsets[row - 1];
        const size_t end = offsets[row];
        for (size_t i = start; i < end; ++i) {
            StringRef path = paths_col->get_data_at(i);
            uint32_t b = 0;
            if (auto it = path_to_bucket.find(path); it != path_to_bucket.end()) {
                b = it->second;
            } else {
                b = vectorized::variant_util::variant_binary_shard_of(path, bucket_num);
                path_to_bucket.emplace(path, b);
            }
            bucket_keys[b]->insert_data(path.data, path.size);
            bucket_values[b]->insert_from(*values_col, i);
            auto& path_counts = bucket_path_counts[b];
            if (auto it = path_counts.find(path); it != path_counts.end()) {
                ++it->second;
            } else if (path_counts.size() < limit) {
                path_counts.emplace(path, 1);
            }
        }
        for (int b = 0; b < bucket_num; ++b) {
            bucket_offsets[b]->push_back(bucket_keys[b]->size());
        }
    }
    for (int b = 0; b < bucket_num; ++b) {
        TabletColumn bucket_col =
                vectorized::variant_util::create_sparse_shard_column(parent_column, b);
        converter->add_column_data_convertor(bucket_col);
        int this_col_id = _first_column_id + b;
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {tmp_maps[b]->get_ptr(), nullptr, ""}, 0, num_rows, this_col_id));
        auto [st, converted] = converter->convert_column_data(this_col_id);
        RETURN_IF_ERROR(st);
        RETURN_IF_ERROR(_bucket_writers[b]->append(converted->get_nullmap(), converted->get_data(),
                                                   num_rows));
        converter->clear_source_content(this_col_id);
        _bucket_opts[b].meta->set_num_rows(num_rows);
    }
    for (int b = 0; b < bucket_num; ++b) {
        segment_v2::VariantStatistics bucket_stats;
        for (const auto& [k, cnt] : bucket_path_counts[b]) {
            bucket_stats.sparse_column_non_null_size.emplace(k.to_string(),
                                                             static_cast<int64_t>(cnt));
        }
        bucket_stats.to_pb(_bucket_opts[b].meta->mutable_variant_statistics());
    }
    return Status::OK();
}

Status VariantDocWriter::init(const TabletColumn* parent_column, int bucket_num, int& column_id,
                              const ColumnWriterOptions& opts, SegmentFooterPB* footer) {
    _parent_column = parent_column;
    _opts = opts;
    _bucket_num = bucket_num;
    _first_column_id = column_id;
    _doc_value_column_writers.resize(_bucket_num);
    _doc_value_column_opts.resize(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        const TabletColumn& bucket_column =
                vectorized::variant_util::create_doc_value_column(*parent_column, b);
        _doc_value_column_opts[b] = opts;
        _doc_value_column_opts[b].meta = footer->add_columns();
        _init_column_meta(_doc_value_column_opts[b].meta, column_id, bucket_column,
                          opts.compression_type);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(_doc_value_column_opts[b], &bucket_column,
                                                        opts.file_writer,
                                                        &_doc_value_column_writers[b]));
        RETURN_IF_ERROR(_doc_value_column_writers[b]->init());
        ++column_id;
    }
    return Status::OK();
}

Status VariantDocWriter::_write_materialized_subcolumn(
        const TabletColumn& parent_column, std::string_view path,
        vectorized::ColumnVariant::Subcolumn& subcolumn, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, int& column_id,
        const std::vector<uint32_t>* rowids) {
    return write_materialized_subcolumn(parent_column, path, subcolumn, num_rows, converter,
                                        column_id, _opts, &_subcolumns_indexes, &_subcolumn_opts,
                                        &_subcolumn_writers, rowids);
}

Status VariantDocWriter::_write_doc_value_column(const TabletColumn& parent_column,
                                                 const vectorized::ColumnVariant& src,
                                                 size_t num_rows,
                                                 vectorized::OlapBlockDataConvertor* converter,
                                                 const DocValuePathStats& column_stats) {
    const auto [paths_col, values_col] = src.get_doc_value_data_paths_and_values();
    const auto& offsets = src.serialized_doc_value_column_offsets();

    std::vector<vectorized::MutableColumnPtr> tmp_maps(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        tmp_maps[b] = vectorized::ColumnVariant::create_binary_column_fn();
        auto& map_col = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        map_col.get_offsets().reserve(num_rows);
    }
    std::vector<vectorized::ColumnString*> bucket_keys(_bucket_num);
    std::vector<vectorized::ColumnString*> bucket_values(_bucket_num);
    std::vector<vectorized::ColumnArray::Offsets64*> bucket_offsets(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        auto& m = assert_cast<vectorized::ColumnMap&>(*tmp_maps[b]);
        bucket_keys[b] = &assert_cast<vectorized::ColumnString&>(m.get_keys());
        bucket_values[b] = &assert_cast<vectorized::ColumnString&>(m.get_values());
        bucket_offsets[b] = &m.get_offsets();
    }

    std::vector<phmap::flat_hash_map<StringRef, uint32_t, StringRefHash>> bucket_path_counts(
            _bucket_num);
    const size_t limit = parent_column.variant_max_sparse_column_statistics_size();
    for (int b = 0; b < _bucket_num; ++b) {
        bucket_path_counts[b].reserve(std::min<size_t>(limit, 1024));
    }
    phmap::flat_hash_map<StringRef, uint32_t, StringRefHash> path_to_bucket;
    path_to_bucket.reserve(std::min<size_t>(paths_col->size(), 8192));

    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = offsets[row - 1];
        const size_t end = offsets[row];
        for (size_t i = start; i < end; ++i) {
            StringRef path = paths_col->get_data_at(i);
            uint32_t bucket = 0;
            auto it = path_to_bucket.find(path);
            if (it != path_to_bucket.end()) {
                bucket = it->second;
            } else {
                bucket = vectorized::variant_util::variant_binary_shard_of(path, _bucket_num);
                path_to_bucket.emplace_hint(it, path, bucket);
                bucket_path_counts[bucket][path] = 0;
            }
            bucket_keys[bucket]->insert_data(path.data, path.size);
            bucket_values[bucket]->insert_from(*values_col, i);
        }
        for (int b = 0; b < _bucket_num; ++b) {
            bucket_offsets[b]->push_back(bucket_keys[b]->size());
        }
    }

    for (const auto& [path, cnt] : column_stats) {
        uint32_t bucket = vectorized::variant_util::variant_binary_shard_of(path, _bucket_num);
        bucket_path_counts[bucket][path] = cnt;
    }

    for (int b = 0; b < _bucket_num; ++b) {
        TabletColumn bucket_column =
                vectorized::variant_util::create_doc_value_column(parent_column, b);
        converter->add_column_data_convertor(bucket_column);
        int this_col_id = _first_column_id + b;
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {tmp_maps[b]->get_ptr(), nullptr, ""}, 0, num_rows, this_col_id));
        auto [status, column] = converter->convert_column_data(this_col_id);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(_doc_value_column_writers[b]->append(column->get_nullmap(),
                                                             column->get_data(), num_rows));
        converter->clear_source_content(this_col_id);
        _doc_value_column_opts[b].meta->set_num_rows(num_rows);
        auto* stats = _doc_value_column_opts[b].meta->mutable_variant_statistics();
        auto* doc_value_column_non_null_size = stats->mutable_doc_value_column_non_null_size();
        for (const auto& [k, cnt] : bucket_path_counts[b]) {
            (*doc_value_column_non_null_size)[k.to_string()] = cnt;
        }
    }
    return Status::OK();
}

Status VariantDocWriter::append_data(const TabletColumn* parent_column,
                                     const vectorized::ColumnVariant& src, size_t num_rows,
                                     vectorized::OlapBlockDataConvertor* converter) {
    _subcolumn_writers.clear();
    _subcolumns_indexes.clear();
    _subcolumn_opts.clear();

    int subcolumn_column_id = _first_column_id + _bucket_num;
    DocValuePathStats column_stats;
    RETURN_IF_ERROR(execute_doc_write_pipeline(
            src, num_rows, parent_column->variant_doc_materialization_min_rows(),
            subcolumn_column_id,
            [this, parent_column, num_rows, converter](SubcolumnWriteEntry& entry,
                                                       int& materialized_column_id) {
                return _write_materialized_subcolumn(*parent_column, entry.path, *entry.subcolumn,
                                                     num_rows, converter, materialized_column_id,
                                                     entry.rowids);
            },
            [this, parent_column, &src, num_rows, converter, &column_stats](int) {
                return _write_doc_value_column(*parent_column, src, num_rows, converter,
                                               column_stats);
            },
            &column_stats));
    return Status::OK();
}

uint64_t VariantDocWriter::estimate_buffer_size() const {
    uint64_t size = 0;
    for (const auto& writer : _subcolumn_writers) {
        size += writer->estimate_buffer_size();
    }
    for (const auto& writer : _doc_value_column_writers) {
        size += writer->estimate_buffer_size();
    }
    return size;
}

Status VariantDocWriter::finish() {
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    return Status::OK();
}

Status VariantDocWriter::write_data() {
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(writer->write_data());
    }
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->write_data());
    }
    return Status::OK();
}

Status VariantDocWriter::write_ordinal_index() {
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(writer->write_ordinal_index());
    }
    for (auto& writer : _doc_value_column_writers) {
        RETURN_IF_ERROR(writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantDocWriter::write_zone_map() {
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_zone_map());
        }
    }
    return Status::OK();
}

Status VariantDocWriter::write_inverted_index() {
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_inverted_index());
        }
    }
    return Status::OK();
}

Status VariantDocWriter::write_bloom_filter_index() {
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    for (int i = 0; i < _doc_value_column_writers.size(); ++i) {
        if (_doc_value_column_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_doc_value_column_writers[i]->write_bloom_filter_index());
        }
    }
    return Status::OK();
}

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
    _null_column = vectorized::ColumnUInt8::create();
}

Status VariantColumnWriterImpl::init() {
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    int count = _tablet_column->variant_max_subcolumns_count();
    if (_opts.rowset_ctx->write_type == DataWriteType::TYPE_DIRECT) {
        count = 0;
    }
    _column = vectorized::ColumnVariant::create(count);
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_root_column(vectorized::ColumnVariant* ptr,
                                                     vectorized::OlapBlockDataConvertor* converter,
                                                     size_t num_rows, int& column_id) {
    // root column
    ColumnWriterOptions root_opts = _opts;
    _root_writer = std::unique_ptr<ColumnWriter>(new ScalarColumnWriter(
            _opts, std::unique_ptr<Field>(FieldFactory::create(*_tablet_column)),
            _opts.file_writer));
    RETURN_IF_ERROR(_root_writer->init());

    // make sure the root type
    auto expected_root_type = vectorized::make_nullable(
            std::make_shared<vectorized::ColumnVariant::MostCommonType>());
    ptr->ensure_root_node_type(expected_root_type);

    DCHECK_EQ(ptr->get_root()->get_ptr()->size(), num_rows);
    converter->add_column_data_convertor(*_tablet_column);
    const uint8_t* nullmap = nullptr;
    auto& nullable_column =
            assert_cast<vectorized::ColumnNullable&>(*ptr->get_root()->assume_mutable());
    auto root_column = nullable_column.get_nested_column_ptr();
    // If the root variant is nullable, then update the root column null column with the outer null column.
    if (_tablet_column->is_nullable()) {
        // use outer null column as final null column
        root_column = vectorized::ColumnNullable::create(
                root_column->get_ptr(), vectorized::ColumnUInt8::create(*_null_column));
        nullmap = _null_column->get_data().data();
    } else {
        // Otherwise setting to all not null.
        root_column = vectorized::ColumnNullable::create(
                root_column->get_ptr(), vectorized::ColumnUInt8::create(root_column->size(), 0));
    }
    // make sure the root_column is nullable
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {root_column->get_ptr(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    RETURN_IF_ERROR(_root_writer->append(nullmap, column->get_data(), num_rows));
    converter->clear_source_content(column_id);
    ++column_id;

    _opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_subcolumns(vectorized::ColumnVariant* ptr,
                                                    vectorized::OlapBlockDataConvertor* converter,
                                                    size_t num_rows, int& column_id) {
    // generate column info by entry info
    auto generate_column_info = [&](const auto& entry) {
        const std::string& column_name =
                _tablet_column->name_lower_case() + "." + entry->path.get_path();
        const vectorized::DataTypePtr& final_data_type_from_object =
                entry->data.get_least_common_type();
        vectorized::PathInData full_path;
        if (entry->path.has_nested_part()) {
            vectorized::PathInDataBuilder full_path_builder;
            full_path = full_path_builder.append(_tablet_column->name_lower_case(), false)
                                .append(entry->path.get_parts(), false)
                                .build();
        } else {
            full_path = vectorized::PathInData(column_name);
        }
        // set unique_id and parent_unique_id, will use unique_id to get iterator correct
        auto column = vectorized::variant_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::variant_util::ExtraInfo {
                        .unique_id = -1,
                        .parent_unique_id = _tablet_column->unique_id(),
                        .path_info = full_path});
        return column;
    };
    _subcolumns_indexes.resize(ptr->get_subcolumns().size());
    // convert sub column data from engine format to storage layer format
    // NOTE: We only keep up to variant_max_subcolumns_count as extracted columns; others are externalized.
    // uint32_t extracted = 0;
    // uint32_t extract_limit = _tablet_column->variant_max_subcolumns_count();
    for (const auto& entry :
         vectorized::variant_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        const auto& least_common_type = entry->data.get_least_common_type();
        if (vectorized::variant_util::get_base_type_of_array(least_common_type)
                    ->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
            continue;
        }
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        CHECK(entry->data.is_finalized());

        // create subcolumn writer if under limit; otherwise externalize ColumnMetaPB via IndexedColumn
        int current_column_id = column_id++;
        TabletColumn tablet_column;
        int64_t none_null_value_size = entry->data.get_non_null_value_size();
        vectorized::ColumnPtr current_column = entry->data.get_finalized_column_ptr()->get_ptr();
        vectorized::DataTypePtr current_type = entry->data.get_least_common_type();
        if (auto current_path = entry->path.get_path();
            _subcolumns_info.find(current_path) != _subcolumns_info.end()) {
            tablet_column = std::move(_subcolumns_info[current_path].column);
            _subcolumns_indexes[current_column_id] =
                    std::move(_subcolumns_info[current_path].indexes);
            if (auto storage_type =
                        vectorized::DataTypeFactory::instance().create_data_type(tablet_column);
                !storage_type->equals(*current_type)) {
                return Status::InvalidArgument("Storage type {} is not equal to current type {}",
                                               storage_type->get_name(), current_type->get_name());
            }
        } else {
            tablet_column = generate_column_info(entry);
        }
        ColumnWriterOptions opts;
        opts.meta = _opts.footer->add_columns();
        opts.index_file_writer = _opts.index_file_writer;
        opts.compression_type = _opts.compression_type;
        opts.rowset_ctx = _opts.rowset_ctx;
        opts.file_writer = _opts.file_writer;
        opts.encoding_preference = _opts.encoding_preference;
        std::unique_ptr<ColumnWriter> writer;
        vectorized::variant_util::inherit_column_attributes(*_tablet_column, tablet_column);

        bool need_record_none_null_value_size =
                (!tablet_column.path_info_ptr()->get_is_typed() ||
                 _tablet_column->variant_enable_typed_paths_to_sparse()) &&
                !tablet_column.path_info_ptr()->has_nested_part();

        RETURN_IF_ERROR(_create_column_writer(
                current_column_id, tablet_column, _opts.rowset_ctx->tablet_schema,
                _opts.index_file_writer, &writer, _subcolumns_indexes[current_column_id], &opts,
                none_null_value_size, need_record_none_null_value_size));
        _subcolumn_writers.push_back(std::move(writer));
        _subcolumn_opts.push_back(opts);
        _subcolumn_opts[current_column_id - 1].meta->set_num_rows(num_rows);

        RETURN_IF_ERROR(convert_and_write_column(converter, tablet_column, current_type,
                                                 _subcolumn_writers[current_column_id - 1].get(),
                                                 current_column, ptr->rows(), current_column_id));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_binary_column(
        vectorized::ColumnVariant* ptr, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id) {
    int bucket_num = 1;
    if (_tablet_column->variant_enable_doc_mode()) {
        _binary_writer = std::make_unique<VariantDocWriter>();
        bucket_num = std::max(1, _tablet_column->variant_doc_hash_shard_count());
    } else {
        _binary_writer = std::make_unique<UnifiedSparseColumnWriter>();
        bucket_num = std::max(1, _tablet_column->variant_sparse_hash_shard_count());
    }

    RETURN_IF_ERROR(
            _binary_writer->init(_tablet_column, bucket_num, column_id, _opts, _opts.footer));
    RETURN_IF_ERROR(_binary_writer->append_data(_tablet_column, *ptr, num_rows, converter));
    return Status::OK();
}

Status VariantColumnWriterImpl::finalize() {
    auto* ptr = _column.get();
    ptr->set_max_subcolumns_count(_tablet_column->variant_max_subcolumns_count());
    ptr->finalize(vectorized::ColumnVariant::FinalizeMode::WRITE_MODE);
    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());

    for (const auto& entry :
         vectorized::variant_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        // Not supported nested path to generate sub column info, currently
        if (entry->path.has_nested_part()) {
            continue;
        }
        TabletSchema::SubColumnInfo sub_column_info;
        if (vectorized::variant_util::generate_sub_column_info(
                    *_opts.rowset_ctx->tablet_schema, _tablet_column->unique_id(),
                    entry->path.get_path(), &sub_column_info)) {
            _subcolumns_info.emplace(entry->path.get_path(), std::move(sub_column_info));
        }
    }

    RETURN_IF_ERROR(ptr->convert_typed_path_to_storage_type(_subcolumns_info));

    RETURN_IF_ERROR(ptr->pick_subcolumns_to_sparse_column(
            _subcolumns_info, _tablet_column->variant_enable_typed_paths_to_sparse()));

#ifndef NDEBUG
    ptr->check_consistency();
#endif

    size_t num_rows = _column->size();
    int column_id = 0;

    // convert root column data from engine format to storage layer format
    RETURN_IF_ERROR(_process_root_column(ptr, olap_data_convertor.get(), num_rows, column_id));

    auto has_extracted_columns = [this]() {
        return std::ranges::any_of(
                _opts.rowset_ctx->tablet_schema->columns(),
                [](const auto& column) { return column->is_extracted_column(); });
    };
    if (!has_extracted_columns()) {
        if (!_tablet_column->variant_enable_doc_mode()) {
            // process and append each subcolumns to sub columns writers buffer
            RETURN_IF_ERROR(
                    _process_subcolumns(ptr, olap_data_convertor.get(), num_rows, column_id));
        }

        // process sparse column and append to sparse writer buffer
        RETURN_IF_ERROR(
                _process_binary_column(ptr, olap_data_convertor.get(), num_rows, column_id));
    }

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    return _column->is_finalized() && _is_finalized;
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    DCHECK(!is_finalized());
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    RETURN_IF_ERROR(src.sanitize());
    // TODO: if direct write we could avoid copy
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    if (!is_finalized()) {
        // not accurate
        return _column->byte_size();
    }
    uint64_t size = 0;
    size += _root_writer->estimate_buffer_size();
    for (auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    if (_binary_writer) {
        size += _binary_writer->estimate_buffer_size();
    }
    return size;
}

Status VariantColumnWriterImpl::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->finish());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->finish());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->write_data());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_data());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    // write ordinal index after data has been written which should be finalized
    assert(is_finalized());
    RETURN_IF_ERROR(_root_writer->write_ordinal_index());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_zone_map() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_zone_map());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_inverted_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_inverted_index());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    if (null_map != nullptr) {
        _null_column->insert_many_raw_data((const char*)null_map, num_rows);
    }
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               const TabletColumn* column,
                                               std::unique_ptr<Field> field)
        : ColumnWriter(std::move(field), opts.meta->is_nullable(), opts.meta) {
    //
    _tablet_column = column;
    _opts = opts;
    _column = vectorized::ColumnVariant::create(0);
}

Status VariantSubcolumnWriter::init() {
    return Status::OK();
}

Status VariantSubcolumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    // TODO: if direct write we could avoid copy
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    return _column->byte_size();
}

bool VariantSubcolumnWriter::is_finalized() const {
    return _column->is_finalized() && _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    auto* ptr = _column.get();
    ptr->finalize();

    DCHECK(ptr->is_finalized());
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(_tablet_column->parent_unique_id());

    TabletColumn flush_column;

    auto path = _tablet_column->path_info_ptr()->copy_pop_front().get_path();

    TabletSchema::SubColumnInfo sub_column_info;
    if (ptr->get_subcolumns().get_root()->data.get_least_common_base_type_id() ==
        PrimitiveType::INVALID_TYPE) {
        auto flush_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_TINYINT, true /* is_nullable */);
        ptr->ensure_root_node_type(flush_type);
    }
    flush_column = vectorized::variant_util::get_column_by_type(
            ptr->get_root_type(), _tablet_column->name(),
            vectorized::variant_util::ExtraInfo {
                    .unique_id = -1,
                    .parent_unique_id = _tablet_column->parent_unique_id(),
                    .path_info = *_tablet_column->path_info_ptr()});

    int64_t none_null_value_size = ptr->get_subcolumns().get_root()->data.get_non_null_value_size();
    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    // refresh opts and get writer with flush column
    vectorized::variant_util::inherit_column_attributes(parent_column, flush_column);
    RETURN_IF_ERROR(_create_column_writer(0, flush_column, _opts.rowset_ctx->tablet_schema,
                                          _opts.index_file_writer, &_writer, _indexes, &opts,
                                          none_null_value_size, need_record_none_null_value_size));

    _opts = opts;
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    int column_id = 0;
    RETURN_IF_ERROR(convert_and_write_column(olap_data_convertor.get(), flush_column,
                                             ptr->get_root_type(), _writer.get(),
                                             ptr->get_root()->get_ptr(), ptr->rows(), column_id));
    _is_finalized = true;
    return Status::OK();
}

Status VariantSubcolumnWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_writer->finish());
    return Status::OK();
}
Status VariantSubcolumnWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_writer->write_data());
    return Status::OK();
}
Status VariantSubcolumnWriter::write_ordinal_index() {
    assert(is_finalized());
    RETURN_IF_ERROR(_writer->write_ordinal_index());
    return Status::OK();
}

Status VariantSubcolumnWriter::write_zone_map() {
    assert(is_finalized());
    if (_opts.need_zone_map) {
        RETURN_IF_ERROR(_writer->write_zone_map());
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::write_inverted_index() {
    assert(is_finalized());
    if (_opts.need_inverted_index) {
        RETURN_IF_ERROR(_writer->write_inverted_index());
    }
    return Status::OK();
}
Status VariantSubcolumnWriter::write_bloom_filter_index() {
    assert(is_finalized());
    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                               size_t num_rows) {
    // the root contains the same nullable info
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantDocCompactWriter::VariantDocCompactWriter(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column,
                                                 std::unique_ptr<Field> field)
        : ColumnWriter(std::move(field), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _tablet_column = column;
    _column = vectorized::ColumnVariant::create(0);
}

Status VariantDocCompactWriter::init() {
    return Status::OK();
}

Status VariantDocCompactWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    auto* dst_ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

Status VariantDocCompactWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->finish());
    return Status::OK();
}
Status VariantDocCompactWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_data());
    return Status::OK();
}
Status VariantDocCompactWriter::write_ordinal_index() {
    assert(is_finalized());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_ordinal_index());
    return Status::OK();
}

Status VariantDocCompactWriter::write_zone_map() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_zone_map());

    return Status::OK();
}
Status VariantDocCompactWriter::write_inverted_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_inverted_index());
    return Status::OK();
}
Status VariantDocCompactWriter::write_bloom_filter_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_bloom_filter_index());
    return Status::OK();
}
Status VariantDocCompactWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

Status VariantDocCompactWriter::_write_materialized_subcolumn(
        const TabletColumn& parent_column, std::string_view path,
        vectorized::ColumnVariant::Subcolumn& subcolumn, size_t num_rows,
        vectorized::OlapBlockDataConvertor* converter, int& column_id,
        const std::vector<uint32_t>* rowids) {
    return write_materialized_subcolumn(parent_column, path, subcolumn, num_rows, converter,
                                        column_id, _opts, &_subcolumns_indexes, &_subcolumn_opts,
                                        &_subcolumn_writers, rowids);
}

Status VariantDocCompactWriter::_write_doc_value_column(
        const TabletColumn& parent_column, vectorized::ColumnVariant* variant_column,
        vectorized::OlapBlockDataConvertor* converter, int column_id, size_t num_rows) {
    std::string doc_value_column_path = _tablet_column->path_info_ptr()->get_path();
    size_t pos = doc_value_column_path.rfind("b");
    int bucket_value = std::stoi(doc_value_column_path.substr(pos + 1));
    TabletColumn doc_value_column =
            vectorized::variant_util::create_doc_value_column(parent_column, bucket_value);
    _init_column_meta(_opts.meta, column_id, doc_value_column, _opts.compression_type);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_opts, &doc_value_column, _opts.file_writer,
                                                    &_doc_value_column_writer));
    RETURN_IF_ERROR(_doc_value_column_writer->init());

    (void)converter;
    auto doc_value_converter = std::make_unique<vectorized::OlapBlockDataConvertor>();
    doc_value_converter->add_column_data_convertor(doc_value_column);
    RETURN_IF_ERROR(doc_value_converter->set_source_content_with_specifid_column(
            {variant_column->get_doc_value_column(), nullptr, ""}, 0, num_rows, 0));
    auto [status, column] = doc_value_converter->convert_column_data(0);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(
            _doc_value_column_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    doc_value_converter->clear_source_content(0);
    return Status::OK();
}
Status VariantDocCompactWriter::finalize() {
    auto* variant_column = assert_cast<vectorized::ColumnVariant*>(_column.get());

    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(_tablet_column->parent_unique_id());

    size_t num_rows = variant_column->size();
    auto converter = std::make_unique<vectorized::OlapBlockDataConvertor>();
    int column_id = 0;
    int64_t variant_doc_materialization_min_rows =
            parent_column.variant_doc_materialization_min_rows();

    _subcolumn_writers.clear();
    _subcolumns_indexes.clear();
    _subcolumn_opts.clear();

    DocValuePathStats column_stats;
    RETURN_IF_ERROR(execute_doc_write_pipeline(
            *variant_column, num_rows, variant_doc_materialization_min_rows, column_id,
            [this, &parent_column, num_rows, &converter](SubcolumnWriteEntry& entry,
                                                         int& materialized_column_id) {
                return _write_materialized_subcolumn(parent_column, entry.path, *entry.subcolumn,
                                                     num_rows, converter.get(),
                                                     materialized_column_id, entry.rowids);
            },
            [this, &parent_column, variant_column, &converter, num_rows](int doc_value_column_id) {
                return _write_doc_value_column(parent_column, variant_column, converter.get(),
                                               doc_value_column_id, num_rows);
            },
            &column_stats));

    _opts.meta->set_num_rows(num_rows);
    auto* stats = _opts.meta->mutable_variant_statistics();
    auto* doc_value_column_non_null_size = stats->mutable_doc_value_column_non_null_size();
    for (const auto& [k, cnt] : column_stats) {
        (*doc_value_column_non_null_size)[std::string(k)] = cnt;
    }
    _is_finalized = true;
    return Status::OK();
}

uint64_t VariantDocCompactWriter::estimate_buffer_size() {
    return _column->byte_size();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
