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
#include "storage/segment/variant/variant_column_writer_impl.h"

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_routing_plan.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       const ColumnWriterOptions& opts) {
    meta->Clear();
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(EncodingInfo::resolve_default_encoding(opts.storage_format, column));
    meta->set_compression(opts.compression_type);
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
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i), opts);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
        meta->set_variant_enable_doc_mode(column.variant_enable_doc_mode());
    }
}

Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                             const TabletSchemaSPtr& tablet_schema,
                             IndexFileWriter* inverted_index_file_writer,
                             std::unique_ptr<ColumnWriter>* writer,
                             TabletIndexes& subcolumn_indexes, ColumnWriterOptions* opt,
                             int64_t none_null_value_size, bool need_record_none_null_value_size) {
    _init_column_meta(opt->meta, cid, column, *opt);
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
                 variant_util::inherit_index(parent_index, subcolumn_indexes, column)) {
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

namespace variant_writer_helpers {

Status convert_and_write_column(OlapBlockDataConvertor* converter, const TabletColumn& column,
                                DataTypePtr data_type, ColumnWriter* writer,
                                const ColumnPtr& src_column, size_t num_rows, int column_id) {
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

} // namespace variant_writer_helpers

namespace {
// Per-path sparse materialization result from a VARIANT doc-value column.
// - subcolumn stores the decoded values for rows listed in rowids (dense, no gaps).
// - rowids records which input rows have a non-null value for this path.
// - non_null_count is used to build variant statistics quickly.
struct SubcolumnSparseData {
    ColumnVariant::Subcolumn subcolumn {0, true, false};
    std::vector<uint32_t> rowids;
    uint32_t non_null_count = 0;
};

using DocSparseSubcolumns = phmap::flat_hash_map<StringRef, SubcolumnSparseData, StringRefHash>;
using DocValuePathStats = phmap::flat_hash_map<StringRef, uint32_t, StringRefHash>;
using DocValuePathSet = phmap::flat_hash_set<StringRef, StringRefHash>;

struct SubcolumnWriteEntry {
    std::string_view path;
    ColumnVariant::Subcolumn* subcolumn = nullptr;
    // nullptr means dense materialization; otherwise sparse row ids for this path.
    std::vector<uint32_t>* rowids = nullptr;
};

struct SubcolumnWritePlan {
    using DenseSubcolumns = phmap::flat_hash_map<std::string_view, ColumnVariant::Subcolumn>;

    // Owns materialized subcolumns and a flattened iteration view for finalize().
    DocSparseSubcolumns sparse_subcolumns;
    DenseSubcolumns dense_subcolumns;
    std::vector<SubcolumnWriteEntry> entries;
    DocValuePathStats stats;
};

enum class DocValueMaterializationMode {
    // Materialize every doc-value path into a full column.
    DenseAllPaths,
    // Materialize every doc-value path as values plus row ids; gaps are filled during write.
    RowIdAllPaths,
    // Materialize only selected doc-value paths as values plus row ids. The caller keeps
    // unselected paths in doc-value or sparse columns.
    RowIdSelectedPaths,
};

struct DocValueMaterializationOptions {
    int64_t min_rows = 0;
    // Full doc-value path statistics already computed by the caller. It is an optimization only:
    // stats stay scoped to all doc-value paths, never just selected materialized paths.
    const DocValuePathStats* precomputed_stats = nullptr;
    // Optional materialized path filter. Used by plain non-doc staging after choosing which paths
    // become real subcolumns; the remaining doc-value items are emitted to sparse columns.
    const DocValuePathSet* selected_paths = nullptr;
};

constexpr size_t kInitialDocPathReserve = 8192;

void release_processed_subcolumn_write_entry(SubcolumnWriteEntry* entry);

// Build per-path non-null counts from the serialized doc-value representation.
void build_doc_value_stats(const ColumnVariant& variant, DocValuePathStats* stats) {
    auto [column_key, column_value] = variant.get_doc_value_data_paths_and_values();
    (void)column_value;
    const auto& column_offsets = variant.serialized_doc_value_column_offsets();
    const size_t num_rows = column_offsets.size();

    stats->clear();
    stats->reserve(std::min<size_t>(column_key->size(), kInitialDocPathReserve));
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

// Materialize sparse subcolumns for each path using precomputed per-path non-null counts.
// For each row, we decode only present (path, value) pairs and append them to the
// corresponding subcolumn, while recording the row id to allow gap filling later.
void build_sparse_subcolumns(const ColumnVariant& variant, const DocValuePathStats& stats,
                             const DocValuePathSet* selected_paths,
                             DocSparseSubcolumns* subcolumns) {
    auto [column_key, column_value] = variant.get_doc_value_data_paths_and_values();
    const auto& column_offsets = variant.serialized_doc_value_column_offsets();
    const size_t num_rows = column_offsets.size();

    subcolumns->clear();
    subcolumns->reserve(stats.size());

    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = column_offsets[row - 1];
        const size_t end = column_offsets[row];
        for (size_t i = start; i < end; ++i) {
            const StringRef path = column_key->get_data_at(i);
            if (selected_paths != nullptr && !selected_paths->contains(path)) {
                continue;
            }
            auto stat_it = stats.find(path);
            DCHECK(stat_it != stats.end());
            auto [data_it, inserted] = subcolumns->try_emplace(path);
            auto& data = data_it->second;
            if (inserted) {
                data.rowids.reserve(stat_it->second);
            }
            data.rowids.push_back(cast_set<uint32_t>(row));
            data.subcolumn.deserialize_from_binary_column(column_value, i);
            ++data.non_null_count;
        }
    }
}

void set_doc_value_stats(const ColumnVariant& variant, const DocValuePathStats* precomputed_stats,
                         DocValuePathStats* stats) {
    // Plain non-doc staging computes stats before choosing top-N materialized paths and reuses them
    // here. Persistent doc mode has no preselected paths, so stats are built on demand.
    if (precomputed_stats != nullptr) {
        *stats = *precomputed_stats;
    } else {
        build_doc_value_stats(variant, stats);
    }
}

DocValueMaterializationMode choose_doc_value_materialization_mode(
        const DocValueMaterializationOptions& options) {
    // "RowId" means the materialized subcolumn is built from only present values plus row ids. It is
    // different from the final sparse column that stores unmaterialized variant paths.
    if (options.selected_paths != nullptr) {
        return DocValueMaterializationMode::RowIdSelectedPaths;
    }
    if (config::enable_variant_doc_sparse_write_subcolumns) {
        return DocValueMaterializationMode::RowIdAllPaths;
    }
    return DocValueMaterializationMode::DenseAllPaths;
}

void append_sparse_write_entries(DocSparseSubcolumns* sparse_subcolumns,
                                 std::vector<SubcolumnWriteEntry>* entries) {
    entries->reserve(sparse_subcolumns->size());
    for (auto& [path, sparse] : *sparse_subcolumns) {
        SubcolumnWriteEntry entry;
        // StringRef points to variant storage; valid for the plan's lifetime.
        entry.path = std::string_view(path.data, path.size);
        entry.subcolumn = &sparse.subcolumn;
        entry.rowids = &sparse.rowids;
        entries->push_back(entry);
    }
}

void append_dense_write_entries(SubcolumnWritePlan::DenseSubcolumns* dense_subcolumns,
                                std::vector<SubcolumnWriteEntry>* entries) {
    entries->reserve(dense_subcolumns->size());
    for (auto& [path, subcolumn] : *dense_subcolumns) {
        SubcolumnWriteEntry entry;
        entry.path = path;
        entry.subcolumn = &subcolumn;
        entry.rowids = nullptr;
        entries->push_back(entry);
    }
}

SubcolumnWritePlan build_subcolumn_write_plan(const ColumnVariant& variant, size_t num_rows,
                                              const DocValueMaterializationOptions& options) {
    SubcolumnWritePlan plan;
    // Below threshold: skip materialization and let finalize() compute stats on demand.
    if (num_rows < static_cast<size_t>(options.min_rows)) {
        return plan;
    }

    set_doc_value_stats(variant, options.precomputed_stats, &plan.stats);
    switch (choose_doc_value_materialization_mode(options)) {
    case DocValueMaterializationMode::RowIdSelectedPaths:
        DCHECK(options.selected_paths != nullptr);
        build_sparse_subcolumns(variant, plan.stats, options.selected_paths,
                                &plan.sparse_subcolumns);
        append_sparse_write_entries(&plan.sparse_subcolumns, &plan.entries);
        break;
    case DocValueMaterializationMode::RowIdAllPaths:
        build_sparse_subcolumns(variant, plan.stats, nullptr, &plan.sparse_subcolumns);
        append_sparse_write_entries(&plan.sparse_subcolumns, &plan.entries);
        break;
    case DocValueMaterializationMode::DenseAllPaths:
        plan.dense_subcolumns =
                variant_util::materialize_docs_to_subcolumns_map(variant, plan.stats.size());
        append_dense_write_entries(&plan.dense_subcolumns, &plan.entries);
        break;
    }
    return plan;
}

template <typename WriteMaterializedFn, typename WriteDocValueFn>
Status execute_doc_write_pipeline(const ColumnVariant& variant, size_t num_rows,
                                  int64_t variant_doc_materialization_min_rows, int& column_id,
                                  WriteMaterializedFn&& write_materialized_fn,
                                  WriteDocValueFn&& write_doc_value_fn,
                                  DocValuePathStats* out_column_stats,
                                  const DocValuePathStats* precomputed_stats = nullptr,
                                  const DocValuePathSet* selected_paths = nullptr) {
    {
        DocValueMaterializationOptions options {
                .min_rows = variant_doc_materialization_min_rows,
                .precomputed_stats = precomputed_stats,
                .selected_paths = selected_paths,
        };
        SubcolumnWritePlan plan = build_subcolumn_write_plan(variant, num_rows, options);
        *out_column_stats = std::move(plan.stats);
        if (out_column_stats->empty()) {
            build_doc_value_stats(variant, out_column_stats);
        }

        for (auto& entry : plan.entries) {
            RETURN_IF_ERROR(write_materialized_fn(entry, column_id));
            release_processed_subcolumn_write_entry(&entry);
        }
    }
    RETURN_IF_ERROR(write_doc_value_fn(column_id));
    return Status::OK();
}

Status finish_and_write_column_writer(ColumnWriter* writer) {
    RETURN_IF_ERROR(writer->finish());
    RETURN_IF_ERROR(writer->write_data());
    return Status::OK();
}

void release_processed_subcolumn_write_entry(SubcolumnWriteEntry* entry) {
    DCHECK(entry != nullptr);
    DCHECK(entry->subcolumn != nullptr);
    ColumnVariant::Subcolumn released_subcolumn(0, true);
    std::swap(*entry->subcolumn, released_subcolumn);
    if (entry->rowids != nullptr) {
        std::vector<uint32_t> released_rowids;
        released_rowids.swap(*entry->rowids);
    }
}

bool is_invalid_materialized_subcolumn_type(const DataTypePtr& type) {
    return variant_util::get_base_type_of_array(type)->get_primitive_type() ==
           PrimitiveType::INVALID_TYPE;
}

Status prepare_materialized_subcolumn_writer(
        const TabletColumn& parent_column, std::string_view path,
        const ColumnVariant::Subcolumn& subcolumn, ColumnPtr& current_column,
        DataTypePtr& current_type, int64_t none_null_value_size, int current_column_id,
        size_t num_rows, const ColumnWriterOptions& base_opts,
        std::vector<TabletIndexes>* subcolumns_indexes,
        std::vector<ColumnWriterOptions>* subcolumn_opts,
        std::vector<std::unique_ptr<ColumnWriter>>* subcolumn_writers,
        TabletColumn* out_tablet_column) {
    TabletColumn tablet_column;
    TabletIndexes subcolumn_indexes;
    TabletSchema::SubColumnInfo sub_column_info;
    if (variant_util::generate_sub_column_info(*base_opts.rowset_ctx->tablet_schema,
                                               parent_column.unique_id(), std::string(path),
                                               &sub_column_info)) {
        tablet_column = std::move(sub_column_info.column);
        subcolumn_indexes = std::move(sub_column_info.indexes);
        DataTypePtr storage_type = DataTypeFactory::instance().create_data_type(tablet_column);
        if (!storage_type->equals(*current_type)) {
            RETURN_IF_ERROR(variant_util::cast_column({current_column, current_type, ""},
                                                      storage_type, &current_column));
        }
        current_type = std::move(storage_type);
    } else {
        const std::string column_name = parent_column.name_lower_case() + "." + std::string(path);
        const DataTypePtr& final_data_type_from_object = subcolumn.get_least_common_type();
        PathInData full_path = PathInData(column_name);
        tablet_column = variant_util::get_column_by_type(
                final_data_type_from_object, column_name,
                variant_util::ExtraInfo {.unique_id = -1,
                                         .parent_unique_id = parent_column.unique_id(),
                                         .path_info = full_path});
        const auto& indexes =
                base_opts.rowset_ctx->tablet_schema->inverted_indexs(parent_column.unique_id());
        variant_util::inherit_index(indexes, subcolumn_indexes, tablet_column);
    }

    ColumnWriterOptions opts;
    opts.meta = base_opts.footer->add_columns();
    opts.index_file_writer = base_opts.index_file_writer;
    opts.compression_type = base_opts.compression_type;
    opts.rowset_ctx = base_opts.rowset_ctx;
    opts.file_writer = base_opts.file_writer;
    opts.storage_format = base_opts.storage_format;
    std::unique_ptr<ColumnWriter> writer;
    variant_util::inherit_column_attributes(parent_column, tablet_column);

    const auto& path_info = tablet_column.path_info_ptr();
    DCHECK(path_info != nullptr);
    const bool need_record_none_null_value_size =
            (!path_info->get_is_typed() || parent_column.variant_enable_typed_paths_to_sparse()) &&
            !path_info->has_nested_part() &&
            variant_util::should_record_variant_path_stats(parent_column);

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
                                      OlapBlockDataConvertor* converter, int cid,
                                      const DataTypePtr& type, const ColumnPtr& values_column,
                                      const std::vector<uint32_t>& rowids, size_t total_rows);

Status write_materialized_subcolumn(const TabletColumn& parent_column, std::string_view path,
                                    ColumnVariant::Subcolumn& subcolumn, size_t num_rows,
                                    OlapBlockDataConvertor* converter, int& column_id,
                                    const ColumnWriterOptions& base_opts,
                                    std::vector<TabletIndexes>* subcolumns_indexes,
                                    std::vector<ColumnWriterOptions>* subcolumn_opts,
                                    std::vector<std::unique_ptr<ColumnWriter>>* subcolumn_writers,
                                    const std::vector<uint32_t>* rowids) {
    const auto& least_common_type = subcolumn.get_least_common_type();
    if (is_invalid_materialized_subcolumn_type(least_common_type)) {
        return Status::OK();
    }

    subcolumn.finalize();
    ColumnPtr current_column = subcolumn.get_finalized_column_ptr()->get_ptr();
    DataTypePtr current_type = subcolumn.get_least_common_type();
    if (rowids != nullptr) {
        DCHECK_EQ(current_column->size(), rowids->size());
        if (rowids->size() != num_rows && !current_type->is_nullable()) {
            current_type = make_nullable(current_type);
            current_column = make_nullable(current_column);
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
    auto subcolumn_converter = std::make_unique<OlapBlockDataConvertor>();
    ColumnWriter* writer = subcolumn_writers->back().get();
    if (rowids != nullptr) {
        return append_sparse_converted_column(tablet_column, writer, subcolumn_converter.get(), 0,
                                              current_type, current_column, *rowids, num_rows);
    }

    return variant_writer_helpers::convert_and_write_column(subcolumn_converter.get(),
                                                            tablet_column, current_type, writer,
                                                            current_column, num_rows, 0);
}

// Convert a sparse (values_column, rowids) pair into storage format and append to writer.
// Missing rows (gaps between rowids) are appended as nulls, so the output has total_rows.
Status append_sparse_converted_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                      OlapBlockDataConvertor* converter, int cid,
                                      const DataTypePtr& type, const ColumnPtr& values_column,
                                      const std::vector<uint32_t>& rowids, size_t total_rows) {
    DCHECK_EQ(values_column->size(), rowids.size());

    auto base_type = type;
    if (base_type->is_nullable()) {
        base_type = assert_cast<const DataTypeNullable&>(*base_type).get_nested_type();
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
        MutableColumnPtr full_column = values_column->clone_empty();
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

    // Non-ARRAY scalar path: writer cell is strided by sizeof(CppType).
    const size_t cell_size = field_type_size(writer->get_column()->type());

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

bool has_doc_value_data(const ColumnVariant& variant) {
    if (variant.size() == 0) {
        return false;
    }
    const auto& offsets = variant.serialized_doc_value_column_offsets();
    return !offsets.empty() && offsets[variant.size() - 1] > 0;
}

// The variant root column is always written by _process_root_column(). The plan below only decides
// how to write non-root variant data: extracted subcolumns, sparse columns, or doc-value buckets.
// Used when extracted columns already own all non-root variant data.
struct ExtractedColumnsOwnDataPlan {};

// Used when JSON parse already expanded paths into ColumnVariant subcolumns.
struct ParseTimeSubcolumnsWritePlan {
    // True when sparse columns are generated from the parse tree after top-N selection.
    bool write_sparse_columns = false;
};

// Used when plain non-doc VARIANT arrives as temporary doc-value KV staging.
struct DocValueStagingWritePlan {};

// Used by persistent doc mode; non-root data is written to doc-value bucket columns.
struct PersistentDocValueWritePlan {};

using VariantNonRootWritePlan =
        std::variant<ExtractedColumnsOwnDataPlan, ParseTimeSubcolumnsWritePlan,
                     DocValueStagingWritePlan, PersistentDocValueWritePlan>;

template <typename... Visitors>
struct Overloaded : Visitors... {
    using Visitors::operator()...;
};
template <typename... Visitors>
Overloaded(Visitors...) -> Overloaded<Visitors...>;

VariantNonRootWritePlan build_variant_non_root_write_plan(const TabletColumn& tablet_column,
                                                          const ColumnVariant& variant,
                                                          bool has_extracted_columns) {
    if (has_extracted_columns) {
        return ExtractedColumnsOwnDataPlan {};
    }

    // Plain non-doc VARIANT may arrive as doc-value KV staging from storage parse. The staging data
    // is internal to this root writer and is converted into materialized subcolumns plus sparse
    // columns.
    if (!tablet_column.variant_enable_doc_mode() && !tablet_column.variant_enable_nested_group() &&
        has_doc_value_data(variant)) {
        return DocValueStagingWritePlan {};
    }

    if (tablet_column.variant_enable_doc_mode()) {
        return PersistentDocValueWritePlan {};
    }

    return ParseTimeSubcolumnsWritePlan {
            .write_sparse_columns =
                    variant_util::should_write_variant_binary_columns(tablet_column)};
}

Status collect_typed_subcolumn_info_from_parse_tree(
        const ColumnVariant& variant, const TabletColumn& tablet_column,
        const TabletSchema& tablet_schema,
        std::unordered_map<std::string, TabletSchema::SubColumnInfo>* subcolumns_info) {
    for (const auto& entry : variant_util::get_sorted_subcolumns(variant.get_subcolumns())) {
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        // Not supported nested path to generate sub column info, currently
        if (entry->path.has_nested_part()) {
            continue;
        }
        TabletSchema::SubColumnInfo sub_column_info;
        if (variant_util::generate_sub_column_info(tablet_schema, tablet_column.unique_id(),
                                                   entry->path.get_path(), &sub_column_info)) {
            subcolumns_info->emplace(entry->path.get_path(), std::move(sub_column_info));
        }
    }
    return Status::OK();
}

Status prepare_parse_time_subcolumns_for_write(
        ColumnVariant* variant, const TabletColumn& tablet_column,
        const TabletSchema& tablet_schema,
        std::unordered_map<std::string, TabletSchema::SubColumnInfo>* subcolumns_info) {
    // Temporary doc-value staging has no parse-time subcolumns, so only ParseTimeSubcolumnsWritePlan
    // reaches this helper. Parse-time paths still need typed-path storage conversion before their
    // writers are created.
    RETURN_IF_ERROR(collect_typed_subcolumn_info_from_parse_tree(*variant, tablet_column,
                                                                 tablet_schema, subcolumns_info));
    RETURN_IF_ERROR(variant->convert_typed_path_to_storage_type(*subcolumns_info));
    return Status::OK();
}

Status prepare_non_root_write_plan_before_write(
        const VariantNonRootWritePlan& non_root_write_plan, ColumnVariant* variant,
        const TabletColumn& tablet_column, const TabletSchema& tablet_schema,
        std::unordered_map<std::string, TabletSchema::SubColumnInfo>* subcolumns_info) {
    if (std::holds_alternative<ParseTimeSubcolumnsWritePlan>(non_root_write_plan)) {
        RETURN_IF_ERROR(prepare_parse_time_subcolumns_for_write(variant, tablet_column,
                                                                tablet_schema, subcolumns_info));
    }
    return Status::OK();
}

Status prepare_sparse_columns_from_parse_tree(
        const VariantNonRootWritePlan& non_root_write_plan, ColumnVariant* variant,
        const TabletColumn& tablet_column,
        const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& subcolumns_info) {
    const auto* parse_time_plan = std::get_if<ParseTimeSubcolumnsWritePlan>(&non_root_write_plan);
    if (parse_time_plan == nullptr || !parse_time_plan->write_sparse_columns) {
        return Status::OK();
    }
    return variant->pick_subcolumns_to_sparse_column(
            subcolumns_info, tablet_column.variant_enable_typed_paths_to_sparse());
}

struct RegularVariantDocValuePlan {
    DocValuePathStats stats;
    DocValuePathSet materialized_paths;
};

size_t dotted_path_depth(StringRef path) {
    return static_cast<size_t>(std::count(path.data, path.data + path.size, '.'));
}

Status build_regular_variant_doc_value_plan(
        const ColumnVariant& variant, const TabletColumn& parent_column,
        const TabletSchema& tablet_schema,
        std::unordered_map<std::string, TabletSchema::SubColumnInfo>* subcolumns_info,
        RegularVariantDocValuePlan* plan) {
    build_doc_value_stats(variant, &plan->stats);
    if (plan->stats.empty()) {
        return Status::OK();
    }

    struct Candidate {
        StringRef path;
        uint32_t non_null_count = 0;
    };
    std::vector<Candidate> dynamic_candidates;
    dynamic_candidates.reserve(plan->stats.size());
    const bool materialize_all_dynamic_paths = parent_column.variant_max_subcolumns_count() == 0;

    for (const auto& [path_ref, non_null_count] : plan->stats) {
        if (path_ref.size == 0 || non_null_count == 0) {
            continue;
        }
        std::string path(path_ref.data, path_ref.size);
        TabletSchema::SubColumnInfo sub_column_info;
        const bool is_typed_path = variant_util::generate_sub_column_info(
                tablet_schema, parent_column.unique_id(), path, &sub_column_info);
        if (is_typed_path) {
            subcolumns_info->emplace(path, std::move(sub_column_info));
        }
        if (is_typed_path && !parent_column.variant_enable_typed_paths_to_sparse()) {
            plan->materialized_paths.emplace(path_ref);
            continue;
        }
        dynamic_candidates.push_back({path_ref, non_null_count});
    }

    std::sort(dynamic_candidates.begin(), dynamic_candidates.end(),
              [](const Candidate& lhs, const Candidate& rhs) {
                  if (lhs.non_null_count != rhs.non_null_count) {
                      return lhs.non_null_count > rhs.non_null_count;
                  }
                  const auto lhs_depth = dotted_path_depth(lhs.path);
                  const auto rhs_depth = dotted_path_depth(rhs.path);
                  if (lhs_depth != rhs_depth) {
                      return lhs_depth > rhs_depth;
                  }
                  return std::string_view(lhs.path.data, lhs.path.size) >
                         std::string_view(rhs.path.data, rhs.path.size);
              });

    const size_t dynamic_limit =
            materialize_all_dynamic_paths
                    ? dynamic_candidates.size()
                    : static_cast<size_t>(parent_column.variant_max_subcolumns_count());
    for (size_t i = 0; i < std::min(dynamic_limit, dynamic_candidates.size()); ++i) {
        plan->materialized_paths.emplace(dynamic_candidates[i].path);
    }
    return Status::OK();
}

Status append_typed_doc_value_to_sparse_column(const ColumnString* doc_values, size_t doc_value_pos,
                                               std::string_view path,
                                               const DataTypePtr& storage_type,
                                               ColumnString* sparse_keys,
                                               ColumnString* sparse_values) {
    ColumnVariant::Subcolumn subcolumn(0, true, false);
    subcolumn.deserialize_from_binary_column(doc_values, doc_value_pos);
    subcolumn.finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    ColumnPtr current_column = subcolumn.get_finalized_column_ptr()->get_ptr();
    DataTypePtr current_type = subcolumn.get_least_common_type();
    if (!storage_type->equals(*current_type)) {
        RETURN_IF_ERROR(variant_util::cast_column({current_column, current_type, ""}, storage_type,
                                                  &current_column));
    }

    DataTypePtr sparse_type = storage_type;
    if (!current_column->is_nullable()) {
        current_column = make_nullable(current_column);
        sparse_type = make_nullable(storage_type);
    }

    auto mutable_column = IColumn::mutate(std::move(current_column));
    ColumnVariant::Subcolumn typed_subcolumn(std::move(mutable_column), sparse_type, true, false);
    typed_subcolumn.serialize_to_binary_column(sparse_keys, path, sparse_values, 0);
    return Status::OK();
}

Status build_sparse_column_from_doc_values(
        const ColumnVariant& variant, const DocValuePathSet& materialized_paths,
        const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& typed_paths,
        size_t num_rows, MutableColumnPtr* result) {
    auto sparse_column = ColumnVariant::create_binary_column_fn();
    auto& sparse_map = assert_cast<ColumnMap&>(*sparse_column);
    auto& sparse_keys = assert_cast<ColumnString&>(sparse_map.get_keys());
    auto& sparse_values = assert_cast<ColumnString&>(sparse_map.get_values());
    auto& sparse_offsets = sparse_map.get_offsets();
    sparse_offsets.reserve(num_rows);

    std::unordered_map<std::string_view, DataTypePtr> typed_storage_types;
    typed_storage_types.reserve(typed_paths.size());
    for (const auto& [path, subcolumn_info] : typed_paths) {
        typed_storage_types.emplace(
                std::string_view(path.data(), path.size()),
                DataTypeFactory::instance().create_data_type(subcolumn_info.column));
    }

    const auto [doc_keys, doc_values] = variant.get_doc_value_data_paths_and_values();
    const auto& doc_offsets = variant.serialized_doc_value_column_offsets();
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t start = doc_offsets[row - 1];
        const size_t end = doc_offsets[row];
        for (size_t i = start; i < end; ++i) {
            const StringRef path = doc_keys->get_data_at(i);
            if (materialized_paths.contains(path)) {
                continue;
            }
            const std::string_view path_view(path.data, path.size);
            if (auto typed_it = typed_storage_types.find(path_view);
                typed_it != typed_storage_types.end()) {
                RETURN_IF_ERROR(append_typed_doc_value_to_sparse_column(
                        doc_values, i, path_view, typed_it->second, &sparse_keys, &sparse_values));
            } else {
                sparse_keys.insert_data(path.data, path.size);
                sparse_values.insert_from(*doc_values, i);
            }
        }
        sparse_offsets.push_back(sparse_keys.size());
    }
    *result = std::move(sparse_column);
    return Status::OK();
}
} // namespace

Status UnifiedSparseColumnWriter::init(const TabletColumn* parent_column, int bucket_num,
                                       int& column_id, const ColumnWriterOptions& base_opts,
                                       SegmentFooterPB* footer) {
    _bucket_num = std::max(1, bucket_num);
    if (_bucket_num <= 1) {
        TabletColumn sparse_column = variant_util::create_sparse_column(*parent_column);
        RETURN_IF_ERROR(init_single(sparse_column, column_id, base_opts, footer));
    } else {
        RETURN_IF_ERROR(init_buckets(_bucket_num, *parent_column, column_id, base_opts, footer));
    }
    return Status::OK();
}

Status UnifiedSparseColumnWriter::append_data(const TabletColumn* parent_column,
                                              const ColumnVariant& src, size_t num_rows,
                                              OlapBlockDataConvertor* converter) {
    if (_single_writer) {
        RETURN_IF_ERROR(append_single_sparse(src, num_rows, converter, *parent_column));
    } else {
        RETURN_IF_ERROR(append_bucket_sparse(src, num_rows, converter, *parent_column));
    }
    return Status::OK();
}

void UnifiedSparseColumnWriter::merge_stats_to(VariantStatistics* stats) const {
    if (stats == nullptr) {
        return;
    }
    for (const auto& [path, cnt] : _stats.sparse_column_non_null_size) {
        stats->sparse_column_non_null_size[path] += cnt;
    }
}

// UnifiedSparseColumnWriter implementation
Status UnifiedSparseColumnWriter::init_single(const TabletColumn& sparse_column, int& column_id,
                                              const ColumnWriterOptions& base_opts,
                                              SegmentFooterPB* footer) {
    _single_opts = base_opts;
    _single_opts.meta = footer->add_columns();
    _init_column_meta(_single_opts.meta, column_id, sparse_column, base_opts);
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
        TabletColumn bucket_col = variant_util::create_sparse_shard_column(parent_column, b);
        _bucket_opts[b] = base_opts;
        _bucket_opts[b].meta = footer->add_columns();
        _init_column_meta(_bucket_opts[b].meta, column_id, bucket_col, base_opts);
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
Status UnifiedSparseColumnWriter::append_single_sparse(const ColumnVariant& src, size_t num_rows,
                                                       OlapBlockDataConvertor* converter,
                                                       const TabletColumn& parent_column) {
    TabletColumn sparse_column = variant_util::create_sparse_column(parent_column);
    converter->resize(_first_column_id + 1);
    converter->add_column_data_convertor_at(sparse_column, _first_column_id);
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
        if (auto it = path_counts.find(k); it != path_counts.end()) {
            ++it->second;
        } else if (path_counts.size() < limit) {
            path_counts.emplace(k, 1);
        }
    }

    // Build path frequency statistics with upper bound limit to avoid
    // large memory and metadata size. Persist to meta for readers.
    segment_v2::VariantStatistics sparse_stats;
    for (const auto& [k, cnt] : path_counts) {
        sparse_stats.sparse_column_non_null_size.emplace(k.to_string(), static_cast<uint32_t>(cnt));
    }
    sparse_stats.to_pb(_single_opts.meta->mutable_variant_statistics());
    _stats.sparse_column_non_null_size = sparse_stats.sparse_column_non_null_size;
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
Status UnifiedSparseColumnWriter::append_bucket_sparse(const ColumnVariant& src, size_t num_rows,
                                                       OlapBlockDataConvertor* converter,
                                                       const TabletColumn& parent_column) {
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

    std::vector<MutableColumnPtr> tmp_maps(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        tmp_maps[b] = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                        ColumnArray::ColumnOffsets::create());
    }
    for (int b = 0; b < bucket_num; ++b) {
        auto& m = assert_cast<ColumnMap&>(*tmp_maps[b]);
        m.get_offsets().reserve(num_rows);
    }
    std::vector<ColumnString*> bucket_keys(bucket_num);
    std::vector<ColumnString*> bucket_values(bucket_num);
    std::vector<ColumnArray::Offsets64*> bucket_offsets(bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        auto& m = assert_cast<ColumnMap&>(*tmp_maps[b]);
        bucket_keys[b] = &assert_cast<ColumnString&>(m.get_keys());
        bucket_values[b] = &assert_cast<ColumnString&>(m.get_values());
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
                b = variant_util::variant_binary_shard_of(path, bucket_num);
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
    converter->resize(_first_column_id + bucket_num);
    for (int b = 0; b < bucket_num; ++b) {
        TabletColumn bucket_col = variant_util::create_sparse_shard_column(parent_column, b);
        int this_col_id = _first_column_id + b;
        converter->add_column_data_convertor_at(bucket_col, this_col_id);
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
            const std::string k_str = k.to_string();
            const uint32_t cnt_u32 = static_cast<uint32_t>(cnt);
            bucket_stats.sparse_column_non_null_size.emplace(k_str, cnt_u32);
            _stats.sparse_column_non_null_size[k_str] += cnt_u32;
        }
        bucket_stats.to_pb(_bucket_opts[b].meta->mutable_variant_statistics());
        _bucket_opts[b].meta->set_num_rows(num_rows);
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
                variant_util::create_doc_value_column(*parent_column, b);
        _doc_value_column_opts[b] = opts;
        _doc_value_column_opts[b].meta = footer->add_columns();
        _init_column_meta(_doc_value_column_opts[b].meta, column_id, bucket_column, opts);
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
        ColumnVariant::Subcolumn& subcolumn, size_t num_rows, OlapBlockDataConvertor* converter,
        int& column_id, const std::vector<uint32_t>* rowids) {
    return write_materialized_subcolumn(parent_column, path, subcolumn, num_rows, converter,
                                        column_id, _opts, &_subcolumns_indexes, &_subcolumn_opts,
                                        &_subcolumn_writers, rowids);
}

Status VariantDocWriter::_write_doc_value_column(const TabletColumn& parent_column,
                                                 const ColumnVariant& src, size_t num_rows,
                                                 OlapBlockDataConvertor* converter,
                                                 const DocValuePathStats& column_stats) {
    _stats.doc_value_column_non_null_size.clear();
    const auto [paths_col, values_col] = src.get_doc_value_data_paths_and_values();
    const auto& offsets = src.serialized_doc_value_column_offsets();

    std::vector<MutableColumnPtr> tmp_maps(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        tmp_maps[b] = ColumnVariant::create_binary_column_fn();
        auto& map_col = assert_cast<ColumnMap&>(*tmp_maps[b]);
        map_col.get_offsets().reserve(num_rows);
    }
    std::vector<ColumnString*> bucket_keys(_bucket_num);
    std::vector<ColumnString*> bucket_values(_bucket_num);
    std::vector<ColumnArray::Offsets64*> bucket_offsets(_bucket_num);
    for (int b = 0; b < _bucket_num; ++b) {
        auto& m = assert_cast<ColumnMap&>(*tmp_maps[b]);
        bucket_keys[b] = &assert_cast<ColumnString&>(m.get_keys());
        bucket_values[b] = &assert_cast<ColumnString&>(m.get_values());
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
                bucket = variant_util::variant_binary_shard_of(path, _bucket_num);
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
        uint32_t bucket = variant_util::variant_binary_shard_of(path, _bucket_num);
        bucket_path_counts[bucket][path] = cnt;
    }

    for (int b = 0; b < _bucket_num; ++b) {
        TabletColumn bucket_column = variant_util::create_doc_value_column(parent_column, b);
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
            const std::string k_str = k.to_string();
            (*doc_value_column_non_null_size)[k_str] = cnt;
            _stats.doc_value_column_non_null_size[k_str] += cnt;
        }
        _doc_value_column_opts[b].meta->set_num_rows(num_rows);
    }
    return Status::OK();
}

Status VariantDocWriter::append_data(const TabletColumn* parent_column, const ColumnVariant& src,
                                     size_t num_rows, OlapBlockDataConvertor* converter) {
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

void VariantDocWriter::merge_stats_to(VariantStatistics* stats) const {
    if (stats == nullptr) {
        return;
    }
    for (const auto& [path, cnt] : _stats.doc_value_column_non_null_size) {
        stats->doc_value_column_non_null_size[path] += cnt;
    }
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

namespace variant_writer_helpers {

void maybe_remove_root_jsonb_with_empty_defaults(MutableColumnPtr* root_column, size_t num_rows,
                                                 bool remove_root_jsonb) {
    if (!remove_root_jsonb) {
        return;
    }
    auto bare_jsonb_type = std::make_shared<ColumnVariant::MostCommonType>();
    auto bare_jsonb_col = bare_jsonb_type->create_column();
    bare_jsonb_col->insert_many_defaults(num_rows);
    *root_column = std::move(bare_jsonb_col);
}

Status prepare_subcolumn_writer_target(
        const ColumnWriterOptions& base_opts, const TabletColumn& parent_column,
        int current_column_id, const PathInData& relative_path, const DataTypePtr& current_type,
        int64_t none_null_value_size, size_t num_rows,
        const TabletSchema::SubColumnInfo* existing_subcolumn_info, bool check_storage_type,
        TabletIndexes* out_subcolumn_indexes, ColumnWriterOptions* out_subcolumn_opts,
        std::unique_ptr<ColumnWriter>* out_writer, TabletColumn* out_tablet_column) {
    if (out_subcolumn_indexes == nullptr || out_subcolumn_opts == nullptr ||
        out_writer == nullptr || out_tablet_column == nullptr) {
        return Status::InvalidArgument("subcolumn writer target output is null");
    }

    TabletColumn tablet_column;
    TabletIndexes subcolumn_indexes;
    bool resolved_from_schema = false;
    if (existing_subcolumn_info != nullptr) {
        tablet_column = existing_subcolumn_info->column;
        subcolumn_indexes = existing_subcolumn_info->indexes;
        resolved_from_schema = true;
    } else {
        TabletSchema::SubColumnInfo sub_column_info;
        if (variant_util::generate_sub_column_info(*base_opts.rowset_ctx->tablet_schema,
                                                   parent_column.unique_id(),
                                                   relative_path.get_path(), &sub_column_info)) {
            tablet_column = std::move(sub_column_info.column);
            subcolumn_indexes = std::move(sub_column_info.indexes);
            resolved_from_schema = true;
        } else {
            const std::string column_name =
                    parent_column.name_lower_case() + "." + relative_path.get_path();
            PathInData full_path;
            if (relative_path.has_nested_part()) {
                PathInDataBuilder full_path_builder;
                full_path = full_path_builder.append(parent_column.name_lower_case(), false)
                                    .append(relative_path.get_parts(), false)
                                    .build();
            } else {
                full_path = PathInData(column_name);
            }
            tablet_column = variant_util::get_column_by_type(
                    current_type, column_name,
                    variant_util::ExtraInfo {.unique_id = -1,
                                             .parent_unique_id = parent_column.unique_id(),
                                             .path_info = full_path});
            const auto& indexes =
                    base_opts.rowset_ctx->tablet_schema->inverted_indexs(parent_column.unique_id());
            variant_util::inherit_index(indexes, subcolumn_indexes, tablet_column);
        }
    }

    if (resolved_from_schema && check_storage_type) {
        auto storage_type = DataTypeFactory::instance().create_data_type(tablet_column);
        if (!storage_type->equals(*current_type)) {
            return Status::InvalidArgument(
                    "Storage type {} is not equal to current type {} for path {}",
                    storage_type->get_name(), current_type->get_name(), relative_path.get_path());
        }
    }

    ColumnWriterOptions opts;
    opts.meta = base_opts.footer->add_columns();
    opts.index_file_writer = base_opts.index_file_writer;
    opts.compression_type = base_opts.compression_type;
    opts.rowset_ctx = base_opts.rowset_ctx;
    opts.file_writer = base_opts.file_writer;
    opts.storage_format = base_opts.storage_format;
    variant_util::inherit_column_attributes(parent_column, tablet_column);

    bool need_record_none_null_value_size =
            (!tablet_column.path_info_ptr()->get_is_typed() ||
             parent_column.variant_enable_typed_paths_to_sparse()) &&
            !tablet_column.path_info_ptr()->has_nested_part() &&
            variant_util::should_record_variant_path_stats(parent_column);

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(_create_column_writer(
            current_column_id, tablet_column, base_opts.rowset_ctx->tablet_schema,
            base_opts.index_file_writer, &writer, subcolumn_indexes, &opts, none_null_value_size,
            need_record_none_null_value_size));
    opts.meta->set_num_rows(num_rows);
    *out_subcolumn_indexes = std::move(subcolumn_indexes);
    *out_subcolumn_opts = opts;
    *out_writer = std::move(writer);
    *out_tablet_column = std::move(tablet_column);
    return Status::OK();
}

} // namespace variant_writer_helpers

VariantColumnWriterImpl::~VariantColumnWriterImpl() = default;

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
    _null_column = ColumnUInt8::create();
    _nested_group_provider = create_nested_group_write_provider();
}

bool VariantColumnWriterImpl::_can_use_nested_group_streaming_compaction() const {
    return _opts.rowset_ctx != nullptr &&
           _opts.rowset_ctx->write_type == DataWriteType::TYPE_COMPACTION &&
           _tablet_column->variant_enable_nested_group() &&
           !_tablet_column->variant_enable_doc_mode() && !_opts.input_rs_readers.empty();
}

Status VariantColumnWriterImpl::init() {
    const bool can_use_streaming = _can_use_nested_group_streaming_compaction();

    if (can_use_streaming) {
        _streaming_compaction_writer = std::make_unique<VariantStreamingCompactionWriter>(
                _opts, _tablet_column, _nested_group_provider.get(), &_statistics);
        return _streaming_compaction_writer->init();
    }
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    int count = _tablet_column->variant_max_subcolumns_count();
    if (_opts.rowset_ctx->write_type == DataWriteType::TYPE_DIRECT) {
        count = 0;
    }
    _column = ColumnVariant::create(count, _tablet_column->variant_enable_doc_mode());
    return Status::OK();
}

bool VariantColumnWriterImpl::_has_extracted_variant_columns() const {
    const int current_variant_uid = _tablet_column->unique_id();
    return std::ranges::any_of(_opts.rowset_ctx->tablet_schema->columns(),
                               [current_variant_uid](const auto& column) {
                                   return column->is_extracted_column() &&
                                          column->parent_unique_id() == current_variant_uid;
                               });
}

Status VariantColumnWriterImpl::_process_root_column(ColumnVariant* ptr,
                                                     OlapBlockDataConvertor* converter,
                                                     size_t num_rows, int& column_id) {
    // root column
    _root_writer = std::make_unique<ScalarColumnWriter>(
            _opts, std::make_shared<TabletColumn>(*_tablet_column), _opts.file_writer);
    RETURN_IF_ERROR(_root_writer->init());

    // make sure the root type
    auto expected_root_type = make_nullable(std::make_shared<ColumnVariant::MostCommonType>());
    ptr->ensure_root_node_type(expected_root_type);

    DCHECK_EQ(ptr->get_root()->get_ptr()->size(), num_rows);
    converter->add_column_data_convertor(*_tablet_column);
    const uint8_t* nullmap = nullptr;
    // get_root() already returns a MutableColumnPtr; store it to avoid dangling ref and
    // to avoid calling assert_mutable() again (which would see use_count>1 and throw).
    auto root_mut = ptr->get_root();
    auto& nullable_column = assert_cast<ColumnNullable&>(*root_mut);
    // Use const access to get the nested column ptr without bumping use_count in the
    // non-const chameleon_ptr path, then mutate() to get exclusive ownership.
    auto root_column = IColumn::mutate(
            static_cast<const ColumnNullable&>(nullable_column).get_nested_column_ptr());

    const bool has_root_ng =
            std::ranges::any_of(_nested_group_routing_plan.ng_only_prefixes,
                                [](const std::string& p) { return is_root_nested_group_path(p); });
    variant_writer_helpers::maybe_remove_root_jsonb_with_empty_defaults(
            &root_column, num_rows,
            _nested_group_routing_plan.can_remove_root_jsonb() && has_root_ng);

    // If the root variant is nullable, then update the root column null column with the outer null column.
    if (_tablet_column->is_nullable()) {
        // use outer null column as final null column
        // Move root_column (exclusive) directly into create() to avoid sharing ownership.
        root_column =
                ColumnNullable::create(std::move(root_column), ColumnUInt8::create(*_null_column));
        nullmap = _null_column->get_data().data();
    } else {
        // Otherwise setting to all not null.
        size_t col_size = root_column->size();
        root_column =
                ColumnNullable::create(std::move(root_column), ColumnUInt8::create(col_size, 0));
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

Status VariantColumnWriterImpl::_process_subcolumns(ColumnVariant* ptr,
                                                    OlapBlockDataConvertor* converter,
                                                    size_t num_rows, int& column_id) {
    _subcolumns_indexes.resize(ptr->get_subcolumns().size());

    auto write_one_subcolumn = [&](const std::string& current_path, const PathInData& relative_path,
                                   const DataTypePtr& current_type, const ColumnPtr& current_column,
                                   size_t non_null_count, bool check_storage_type,
                                   bool use_existing_subcolumn_info) -> Status {
        int current_column_id = column_id++;
        if (_subcolumns_indexes.size() <= cast_set<size_t>(current_column_id)) {
            _subcolumns_indexes.resize(cast_set<size_t>(current_column_id) + 1);
        }

        const TabletSchema::SubColumnInfo* existing_subcolumn_info = nullptr;
        if (use_existing_subcolumn_info) {
            if (auto it = _subcolumns_info.find(current_path); it != _subcolumns_info.end()) {
                existing_subcolumn_info = &it->second;
            }
        }

        TabletColumn tablet_column;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, *_tablet_column, current_column_id, relative_path, current_type,
                non_null_count, num_rows, existing_subcolumn_info, check_storage_type,
                &_subcolumns_indexes[current_column_id], &opts, &writer, &tablet_column));
        _subcolumn_writers.push_back(std::move(writer));
        _subcolumn_opts.push_back(opts);

        RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
                converter, tablet_column, current_type, _subcolumn_writers.back().get(),
                current_column, ptr->rows(), current_column_id));
        return Status::OK();
    };

    // convert sub column data from engine format to storage layer format
    // NOTE: We only keep up to variant_max_subcolumns_count as extracted columns; others are externalized.
    for (const auto& entry : variant_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        if (entry->path.empty()) {
            continue;
        }
        const auto& least_common_type = entry->data.get_least_common_type();
        if (least_common_type == nullptr) {
            continue;
        }
        auto base_type = variant_util::get_base_type_of_array(least_common_type);
        if (base_type != nullptr &&
            base_type->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
            continue;
        }
        // Skip Array(Variant) subcolumns — these represent NG (nested group) data
        // that should be handled by the NG writer, not as regular subcolumns.
        if (base_type != nullptr &&
            typeid_cast<const DataTypeVariant*>(base_type.get()) != nullptr) {
            continue;
        }
        const std::string current_path = entry->path.get_path();
        if (_nested_group_routing_plan.is_excluded_subcolumn(current_path)) {
            continue;
        }
        CHECK(entry->data.is_finalized());
        RETURN_IF_ERROR(write_one_subcolumn(current_path, entry->path, least_common_type,
                                            entry->data.get_finalized_column_ptr()->get_ptr(),
                                            entry->data.get_non_null_value_size(),
                                            true /* check_storage_type */,
                                            true /* use_existing_subcolumn_info */));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_regular_doc_value_staging(
        ColumnVariant* ptr, OlapBlockDataConvertor* converter, size_t num_rows, int& column_id) {
    DCHECK(!_tablet_column->variant_enable_doc_mode());
    DCHECK(!_tablet_column->variant_enable_nested_group());
    RegularVariantDocValuePlan plan;
    RETURN_IF_ERROR(build_regular_variant_doc_value_plan(
            *ptr, *_tablet_column, *_opts.rowset_ctx->tablet_schema, &_subcolumns_info, &plan));

    DocValuePathStats column_stats;
    RETURN_IF_ERROR(execute_doc_write_pipeline(
            *ptr, num_rows, 0 /* materialize selected paths regardless of row count */, column_id,
            [this, num_rows, converter](SubcolumnWriteEntry& entry, int& materialized_column_id) {
                return write_materialized_subcolumn(*_tablet_column, entry.path, *entry.subcolumn,
                                                    num_rows, converter, materialized_column_id,
                                                    _opts, &_subcolumns_indexes, &_subcolumn_opts,
                                                    &_subcolumn_writers, entry.rowids);
            },
            [this, ptr, num_rows, converter, &plan](int& binary_column_id) {
                if (!variant_util::should_write_variant_binary_columns(*_tablet_column)) {
                    return Status::OK();
                }

                MutableColumnPtr sparse_column;
                RETURN_IF_ERROR(build_sparse_column_from_doc_values(
                        *ptr, plan.materialized_paths, _subcolumns_info, num_rows, &sparse_column));
                auto sparse_variant = ColumnVariant::create(0, false, num_rows);
                sparse_variant->set_sparse_column(sparse_column->get_ptr());

                _binary_writer = std::make_unique<UnifiedSparseColumnWriter>();
                const int bucket_num =
                        std::max(1, _tablet_column->variant_sparse_hash_shard_count());
                RETURN_IF_ERROR(_binary_writer->init(_tablet_column, bucket_num, binary_column_id,
                                                     _opts, _opts.footer));
                return _binary_writer->append_data(_tablet_column, *sparse_variant, num_rows,
                                                   converter);
            },
            &column_stats, &plan.stats, &plan.materialized_paths));
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_binary_column(ColumnVariant* ptr,
                                                       OlapBlockDataConvertor* converter,
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
    if (_streaming_compaction_writer != nullptr) {
        return Status::OK();
    }
    auto* ptr = _column.get();
    ptr->set_max_subcolumns_count(_tablet_column->variant_max_subcolumns_count());

    ptr->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);

    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());
    const bool has_extracted_columns = _has_extracted_variant_columns();
    const VariantNonRootWritePlan non_root_write_plan =
            build_variant_non_root_write_plan(*_tablet_column, *ptr, has_extracted_columns);
    RETURN_IF_ERROR(prepare_non_root_write_plan_before_write(
            non_root_write_plan, ptr, *_tablet_column, *_opts.rowset_ctx->tablet_schema,
            &_subcolumns_info));

    // Root NG dedup is handled in _process_root_column() — see the
    // has_root_ng check there. We intentionally do NOT modify the in-memory
    // root data here because the legacy NestedGroup prepare path still needs it.
    NestedGroupsMap prebuilt_nested_groups;
    bool has_prebuilt_nested_groups = false;
    _nested_group_routing_plan = NestedGroupRoutingPlan {};
    if (!has_extracted_columns && _tablet_column->variant_enable_nested_group()) {
        std::vector<std::string> ng_candidate_paths;
        std::vector<std::string> conflict_candidate_paths;
        RETURN_IF_ERROR(build_nested_groups_from_variant_jsonb(
                *ptr, &prebuilt_nested_groups, &ng_candidate_paths, &conflict_candidate_paths));
        RETURN_IF_ERROR(build_nested_group_routing_plan_from_candidates(
                *ptr, ng_candidate_paths, conflict_candidate_paths, &_nested_group_routing_plan));
        has_prebuilt_nested_groups = true;
    }

    RETURN_IF_ERROR(prepare_sparse_columns_from_parse_tree(non_root_write_plan, ptr,
                                                           *_tablet_column, _subcolumns_info));

#ifndef NDEBUG
    ptr->check_consistency();
#endif

    size_t num_rows = _column->size();
    int column_id = 0;

    // convert root column data from engine format to storage layer format
    RETURN_IF_ERROR(_process_root_column(ptr, olap_data_convertor.get(), num_rows, column_id));

    RETURN_IF_ERROR(std::visit(
            Overloaded {[](const ExtractedColumnsOwnDataPlan&) { return Status::OK(); },
                        [this, ptr, &olap_data_convertor, num_rows,
                         &column_id](const DocValueStagingWritePlan&) {
                            return _process_regular_doc_value_staging(
                                    ptr, olap_data_convertor.get(), num_rows, column_id);
                        },
                        [this, ptr, &olap_data_convertor, num_rows,
                         &column_id](const PersistentDocValueWritePlan&) {
                            return _process_binary_column(ptr, olap_data_convertor.get(), num_rows,
                                                          column_id);
                        },
                        [this, ptr, &olap_data_convertor, num_rows,
                         &column_id](const ParseTimeSubcolumnsWritePlan& plan) {
                            RETURN_IF_ERROR(_process_subcolumns(ptr, olap_data_convertor.get(),
                                                                num_rows, column_id));
                            if (plan.write_sparse_columns) {
                                return _process_binary_column(ptr, olap_data_convertor.get(),
                                                              num_rows, column_id);
                            }
                            return Status::OK();
                        }},
            non_root_write_plan));

    // Legacy non-streaming NestedGroup write behavior stays behind provider->prepare().
    if (_tablet_column->variant_enable_nested_group()) {
        if (has_prebuilt_nested_groups) {
            RETURN_IF_ERROR(_nested_group_provider->prepare_with_built_groups(
                    prebuilt_nested_groups, _tablet_column, _opts, olap_data_convertor.get(),
                    &column_id, &_statistics));
        } else {
            RETURN_IF_ERROR(_nested_group_provider->prepare(*ptr, _tablet_column, _opts,
                                                            olap_data_convertor.get(), &column_id,
                                                            &_statistics));
        }
    }
    if (_binary_writer) {
        _binary_writer->merge_stats_to(&_statistics);
    }
    _statistics.to_pb(_opts.meta->mutable_variant_statistics());

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->is_finalized();
    }
    return _column->is_finalized() && _is_finalized;
}

Status VariantColumnWriterImpl::_for_each_column_writer(
        const std::function<Status(ColumnWriter*)>& func) {
    RETURN_IF_ERROR(func(_root_writer.get()));
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(func(writer.get()));
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_ensure_materialized_variant_finalized() {
    if (_streaming_compaction_writer != nullptr || is_finalized()) {
        return Status::OK();
    }
    return finalize();
}

void VariantColumnWriterImpl::_assert_ready_for_index_writes() const {
    if (_streaming_compaction_writer == nullptr) {
        assert(is_finalized());
    }
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->append_data(ptr, num_rows, nullptr);
    }
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const ColumnVariant*>(column->column_data);
    RETURN_IF_ERROR(src.sanitize());
    DCHECK(!is_finalized());
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->estimate_buffer_size();
    }
    if (!is_finalized()) {
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
    size += _nested_group_provider->estimate_buffer_size();
    return size;
}

Status VariantColumnWriterImpl::finish() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->finish();
    }
    RETURN_IF_ERROR(_ensure_materialized_variant_finalized());
    RETURN_IF_ERROR(_for_each_column_writer([](ColumnWriter* writer) { return writer->finish(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->finish());
    }
    RETURN_IF_ERROR(_nested_group_provider->finish());
    return Status::OK();
}
Status VariantColumnWriterImpl::write_data() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->write_data();
    }
    RETURN_IF_ERROR(_ensure_materialized_variant_finalized());
    RETURN_IF_ERROR(
            _for_each_column_writer([](ColumnWriter* writer) { return writer->write_data(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_data());
    }
    RETURN_IF_ERROR(_nested_group_provider->write_data());
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->write_ordinal_index();
    }
    _assert_ready_for_index_writes();
    RETURN_IF_ERROR(_for_each_column_writer(
            [](ColumnWriter* writer) { return writer->write_ordinal_index(); }));
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_nested_group_provider->write_ordinal_index());
    return Status::OK();
}

Status VariantColumnWriterImpl::write_zone_map() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->write_zone_map();
    }
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_zone_map());
    }
    RETURN_IF_ERROR(_nested_group_provider->write_zone_map());
    return Status::OK();
}

Status VariantColumnWriterImpl::write_inverted_index() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->write_inverted_index();
    }
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_inverted_index());
    }
    RETURN_IF_ERROR(_nested_group_provider->write_inverted_index());
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->write_bloom_filter_index();
    }
    _assert_ready_for_index_writes();
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    if (_binary_writer) {
        RETURN_IF_ERROR(_binary_writer->write_bloom_filter_index());
    }
    RETURN_IF_ERROR(_nested_group_provider->write_bloom_filter_index());
    return Status::OK();
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    if (_streaming_compaction_writer != nullptr) {
        return _streaming_compaction_writer->append_data(ptr, num_rows, null_map);
    }
    if (null_map != nullptr) {
        _null_column->insert_many_raw_data((const char*)null_map, num_rows);
    }
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _column = ColumnVariant::create(0, false);
}

Status VariantSubcolumnWriter::init() {
    return Status::OK();
}

Status VariantSubcolumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const ColumnVariant*>(column->column_data);
    // TODO: if direct write we could avoid copy
    _column->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    if (!is_finalized()) {
        return _column->byte_size();
    }
    return _writer ? _writer->estimate_buffer_size() : 0;
}

bool VariantSubcolumnWriter::is_finalized() const {
    return _column->is_finalized() && _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    auto* ptr = _column.get();
    ptr->finalize();

    DCHECK(ptr->is_finalized());
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());

    TabletColumn flush_column;
    if (ptr->get_subcolumns().get_root()->data.get_least_common_base_type_id() ==
        PrimitiveType::INVALID_TYPE) {
        auto flush_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT,
                                                                       true /* is_nullable */);
        ptr->ensure_root_node_type(flush_type);
    }
    flush_column = variant_util::get_column_by_type(
            ptr->get_root_type(), get_column()->name(),
            variant_util::ExtraInfo {.unique_id = -1,
                                     .parent_unique_id = get_column()->parent_unique_id(),
                                     .path_info = *get_column()->path_info_ptr()});

    int64_t none_null_value_size = ptr->get_subcolumns().get_root()->data.get_non_null_value_size();
    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    // refresh opts and get writer with flush column
    variant_util::inherit_column_attributes(parent_column, flush_column);
    RETURN_IF_ERROR(_create_column_writer(0, flush_column, _opts.rowset_ctx->tablet_schema,
                                          _opts.index_file_writer, &_writer, _indexes, &opts,
                                          none_null_value_size, need_record_none_null_value_size));

    _opts = opts;
    auto olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    int column_id = 0;
    RETURN_IF_ERROR(variant_writer_helpers::convert_and_write_column(
            olap_data_convertor.get(), flush_column, ptr->get_root_type(), _writer.get(),
            ptr->get_root()->get_ptr(), ptr->rows(), column_id));
    _opts.meta->set_num_rows(ptr->rows());
    ++column_id;

    DORIS_CHECK(!parent_column.variant_enable_nested_group());

    _is_finalized = true;
    return Status::OK();
}

Status VariantSubcolumnWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    return _writer->finish();
}
Status VariantSubcolumnWriter::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    return _writer->write_data();
}
Status VariantSubcolumnWriter::write_ordinal_index() {
    assert(is_finalized());
    return _writer->write_ordinal_index();
}

Status VariantSubcolumnWriter::write_zone_map() {
    assert(is_finalized());
    if (_opts.need_zone_map) {
        return _writer->write_zone_map();
    }
    return Status::OK();
}

Status VariantSubcolumnWriter::write_inverted_index() {
    assert(is_finalized());
    if (_opts.need_inverted_index) {
        return _writer->write_inverted_index();
    }
    return Status::OK();
}
Status VariantSubcolumnWriter::write_bloom_filter_index() {
    assert(is_finalized());
    if (_opts.need_bloom_filter) {
        return _writer->write_bloom_filter_index();
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
                                                 TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
    _column = ColumnVariant::create(0, false);
}

Status VariantDocCompactWriter::init() {
    return Status::OK();
}

Status VariantDocCompactWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const ColumnVariant*>(column->column_data);
    auto* dst_ptr = assert_cast<ColumnVariant*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

Status VariantDocCompactWriter::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    if (_data_written) {
        return Status::OK();
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
    if (_data_written) {
        return Status::OK();
    }
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    RETURN_IF_ERROR(_doc_value_column_writer->write_data());
    _data_written = true;
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
        ColumnVariant::Subcolumn& subcolumn, size_t num_rows, OlapBlockDataConvertor* converter,
        int& column_id, const std::vector<uint32_t>* rowids) {
    return write_materialized_subcolumn(parent_column, path, subcolumn, num_rows, converter,
                                        column_id, _opts, &_subcolumns_indexes, &_subcolumn_opts,
                                        &_subcolumn_writers, rowids);
}

Status VariantDocCompactWriter::_write_doc_value_column(const TabletColumn& parent_column,
                                                        ColumnVariant* variant_column,
                                                        OlapBlockDataConvertor* converter,
                                                        int column_id, size_t num_rows) {
    std::string doc_value_column_path = get_column()->path_info_ptr()->get_path();
    size_t pos = doc_value_column_path.rfind("b");
    int bucket_value = std::stoi(doc_value_column_path.substr(pos + 1));
    TabletColumn doc_value_column =
            variant_util::create_doc_value_column(parent_column, bucket_value);
    _init_column_meta(_opts.meta, column_id, doc_value_column, _opts);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_opts, &doc_value_column, _opts.file_writer,
                                                    &_doc_value_column_writer));
    RETURN_IF_ERROR(_doc_value_column_writer->init());

    (void)converter;
    auto doc_value_converter = std::make_unique<OlapBlockDataConvertor>();
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
    auto* variant_column = assert_cast<ColumnVariant*>(_column.get());

    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());

    size_t num_rows = variant_column->size();
    auto converter = std::make_unique<OlapBlockDataConvertor>();
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
                const size_t prev_writer_count = _subcolumn_writers.size();
                RETURN_IF_ERROR(_write_materialized_subcolumn(
                        parent_column, entry.path, *entry.subcolumn, num_rows, converter.get(),
                        materialized_column_id, entry.rowids));
                DCHECK_EQ(_subcolumn_writers.size(), prev_writer_count + 1);
                RETURN_IF_ERROR(finish_and_write_column_writer(_subcolumn_writers.back().get()));
                return Status::OK();
            },
            [this, &parent_column, variant_column, &converter, num_rows](int doc_value_column_id) {
                RETURN_IF_ERROR(_write_doc_value_column(parent_column, variant_column,
                                                        converter.get(), doc_value_column_id,
                                                        num_rows));
                RETURN_IF_ERROR(finish_and_write_column_writer(_doc_value_column_writer.get()));
                return Status::OK();
            },
            &column_stats));

    _opts.meta->set_num_rows(num_rows);
    auto* stats = _opts.meta->mutable_variant_statistics();
    auto* doc_value_column_non_null_size = stats->mutable_doc_value_column_non_null_size();
    for (const auto& [k, cnt] : column_stats) {
        (*doc_value_column_non_null_size)[std::string(k)] = cnt;
    }
    _column = ColumnVariant::create(0, false);
    _data_written = true;
    _is_finalized = true;
    return Status::OK();
}

uint64_t VariantDocCompactWriter::estimate_buffer_size() {
    return _column->byte_size();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
