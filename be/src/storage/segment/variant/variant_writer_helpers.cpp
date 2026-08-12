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

#include "storage/segment/variant/variant_writer_helpers.h"

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/metrics/doris_metrics.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "exec/common/variant_util.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/types.h"

namespace doris::segment_v2::variant_writer_helpers {

bool has_extracted_variant_columns(const TabletSchema& tablet_schema, int parent_column_unique_id) {
    return std::ranges::any_of(tablet_schema.columns(),
                               [parent_column_unique_id](const auto& column) {
                                   return column->is_extracted_column() &&
                                          column->parent_unique_id() == parent_column_unique_id;
                               });
}

Status validate_variant_v2_writer_layout(const TabletSchema& tablet_schema,
                                         const TabletColumn& parent_column) {
    if (parent_column.variant_enable_nested_group()) {
        return Status::NotSupported("ColumnVariantV2 writer does not support nested-group layout");
    }
    if (tablet_schema.deprecated_variant_flatten_nested()) {
        return Status::NotSupported(
                "ColumnVariantV2 writer does not support deprecated flatten-nested layout");
    }
    DORIS_CHECK_GE(parent_column.variant_max_subcolumns_count(), 0);
    return Status::OK();
}

Status make_variant_shredder_options(const TabletSchema& tablet_schema,
                                     const TabletColumn& parent_column,
                                     VariantShredderPhysicalLayout physical_layout,
                                     VariantShredderOptions* options) {
    DORIS_CHECK(options != nullptr);
    RETURN_IF_ERROR(validate_variant_v2_writer_layout(tablet_schema, parent_column));

    VariantShredderOptions result {
            .tablet_schema = &tablet_schema,
            .parent_column_unique_id = parent_column.unique_id(),
            .physical_layout = physical_layout,
            .max_subcolumns_count = cast_set<size_t>(parent_column.variant_max_subcolumns_count()),
            .typed_paths_to_sparse = parent_column.variant_enable_typed_paths_to_sparse(),
            .sparse_bucket_count = cast_set<uint32_t>(
                    std::max(1, parent_column.variant_sparse_hash_shard_count())),
            .max_sparse_column_statistics_size =
                    cast_set<size_t>(parent_column.variant_max_sparse_column_statistics_size()),
            .doc_bucket_count =
                    cast_set<uint32_t>(std::max(1, parent_column.variant_doc_hash_shard_count())),
            .doc_materialization_min_rows = cast_set<size_t>(
                    std::max<int64_t>(0, parent_column.variant_doc_materialization_min_rows())),
            .check_duplicate_json_path = config::variant_enable_duplicate_json_path_check,
    };
    *options = std::move(result);
    return Status::OK();
}

Status classify_variant_writer_input(const VariantColumnData& column,
                                     VariantWriterInputFormat current_format,
                                     std::string_view writer_description,
                                     VariantWriterInputFormat* input_format) {
    DORIS_CHECK(input_format != nullptr);
    if (column.column_data == nullptr) {
        return Status::InvalidArgument("{} received null column data", writer_description);
    }
    const bool is_v1 = check_and_get_column<ColumnVariant>(*column.column_data) != nullptr;
    const bool is_v2 = check_and_get_column<ColumnVariantV2>(*column.column_data) != nullptr;
    if (!is_v1 && !is_v2) {
        return Status::InvalidArgument("{} requires ColumnVariant or ColumnVariantV2, got {}",
                                       writer_description, column.column_data->get_name());
    }

    const VariantWriterInputFormat detected_format =
            is_v1 ? VariantWriterInputFormat::V1 : VariantWriterInputFormat::V2;
    if (current_format != VariantWriterInputFormat::UNSET && current_format != detected_format) {
        return Status::InvalidArgument("{} input representation changed within one segment",
                                       writer_description);
    }
    *input_format = detected_format;
    return Status::OK();
}

Status append_variant_v2_to_shredder(VariantShredder* shredder, const VariantColumnData& column,
                                     size_t num_rows, std::span<const uint8_t> outer_nulls,
                                     VariantShredderAppendStats* append_stats) {
    DORIS_CHECK(shredder != nullptr);
    DORIS_CHECK(column.column_data != nullptr);
    const auto* source = check_and_get_column<ColumnVariantV2>(*column.column_data);
    DORIS_CHECK(source != nullptr);
    if (column.row_pos > source->size() || num_rows > source->size() - column.row_pos) {
        return Status::InvalidArgument("ColumnVariantV2 writer range [{}, {}) exceeds {} rows",
                                       column.row_pos, column.row_pos + num_rows, source->size());
    }
    if (!outer_nulls.empty() && outer_nulls.size() != num_rows) {
        return Status::InvalidArgument("ColumnVariantV2 outer-null span has {} rows, expected {}",
                                       outer_nulls.size(), num_rows);
    }

    if (source->is_encoded()) {
        const Status status =
                shredder->append(source->read_view(), column.row_pos, num_rows, outer_nulls);
        if (status.ok() && append_stats != nullptr) {
            *append_stats = {};
        }
        return status;
    }
    if (source->is_shredded()) {
        return shredder->append_shredded(*source, column.row_pos, num_rows, outer_nulls,
                                         append_stats);
    }

    ColumnVariantV2::MutablePtr encoded_batch;
    RETURN_IF_CATCH_EXCEPTION(
            { encoded_batch = source->materialize_encoded_range(column.row_pos, num_rows); });
    DORIS_CHECK(encoded_batch->is_encoded());
    const Status status = shredder->append(encoded_batch->read_view(), 0, num_rows, outer_nulls);
    if (status.ok() && append_stats != nullptr) {
        *append_stats = {};
    }
    return status;
}

void record_variant_v2_shredded_writer_stats(const VariantShredderAppendStats& append_stats) {
    if (append_stats.native_shredded_rows != 0) {
        DorisMetrics::instance()->variant_v2_shredded_writer_native_rows->increment(
                append_stats.native_shredded_rows);
    }
    if (append_stats.encoded_fallback_rows != 0) {
        DorisMetrics::instance()->variant_v2_shredded_writer_fallback_rows->increment(
                append_stats.encoded_fallback_rows);
    }
}

void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
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
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i), opts);
    }
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
        meta->set_variant_enable_doc_mode(column.variant_enable_doc_mode());
    }
}

Status create_column_writer(uint32_t cid, const TabletColumn& column,
                            const TabletSchemaSPtr& tablet_schema,
                            IndexFileWriter* inverted_index_file_writer,
                            std::unique_ptr<ColumnWriter>* writer, TabletIndexes& subcolumn_indexes,
                            ColumnWriterOptions* opt, int64_t none_null_value_size,
                            bool need_record_none_null_value_size) {
    init_column_meta(opt->meta, cid, column, *opt);
    if (need_record_none_null_value_size) {
        opt->meta->set_none_null_size(none_null_value_size);
    }
    opt->need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opt->need_bloom_filter = column.is_bf_column();
    const auto& parent_index = tablet_schema->inverted_indexs(column.parent_unique_id());

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

        if (!subcolumn_indexes.empty()) {
            init_opt_inverted_index();
        } else if (!parent_index.empty() &&
                   variant_util::inherit_index(parent_index, subcolumn_indexes, column)) {
            init_opt_inverted_index();
        } else {
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

Status convert_and_write_column(OlapBlockDataConvertor* converter, const TabletColumn& column,
                                DataTypePtr data_type, ColumnWriter* writer,
                                const ColumnPtr& src_column, size_t num_rows, int column_id) {
    converter->add_column_data_convertor(column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({src_column, data_type, ""},
                                                                       0, num_rows, column_id));
    auto [status, converted_column] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);

    RETURN_IF_ERROR(writer->append(converted_column->get_nullmap(), converted_column->get_data(),
                                   num_rows));
    converter->clear_source_content(column_id);
    return Status::OK();
}

namespace {

Status append_sparse_array_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                  OlapBlockDataConvertor* converter, int column_id,
                                  const DataTypePtr& type, const ColumnPtr& values_column,
                                  std::span<const uint32_t> rowids, size_t total_rows) {
    // Example: values=[a,b], rowids=[1,4], total_rows=6 becomes
    // [NULL,a,NULL,NULL,b,NULL]. ARRAY convertor output contains offsets/pointers rather than a
    // fixed cell stride, so materialize only this path while it is being written. Scalar paths
    // never allocate the full N-row representation.
    MutableColumnPtr full_column = values_column->clone_empty();
    full_column->reserve(total_rows);
    size_t next_row = 0;
    size_t value_index = 0;
    while (value_index < rowids.size()) {
        const size_t row = rowids[value_index];
        DORIS_CHECK_GE(row, next_row);
        full_column->insert_many_defaults(row - next_row);

        size_t run_length = 1;
        while (value_index + run_length < rowids.size() &&
               rowids[value_index + run_length] == row + run_length) {
            ++run_length;
        }
        full_column->insert_range_from(*values_column, value_index, run_length);
        value_index += run_length;
        next_row = row + run_length;
    }
    DORIS_CHECK_LE(next_row, total_rows);
    full_column->insert_many_defaults(total_rows - next_row);

    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {full_column->get_ptr(), type, ""}, 0, total_rows, column_id));
    auto [status, converted] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);
    const auto* data = reinterpret_cast<const uint8_t*>(converted->get_data());
    RETURN_IF_ERROR(writer->append_nullable(converted->get_nullmap(), &data, total_rows));
    converter->clear_source_content(column_id);
    return Status::OK();
}

Status append_sparse_scalar_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                   OlapBlockDataConvertor* converter, int column_id,
                                   const DataTypePtr& type, const ColumnPtr& values_column,
                                   std::span<const uint32_t> rowids, size_t total_rows) {
    const size_t cell_size = field_type_size(writer->get_column()->type());
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column({values_column, type, ""}, 0,
                                                                       rowids.size(), column_id));
    auto [status, converted] = converter->convert_column_data(column_id);
    RETURN_IF_ERROR(status);

    const uint8_t* nullmap = converted->get_nullmap();
    const auto* data = reinterpret_cast<const uint8_t*>(converted->get_data());
    auto append_gap = [&](size_t gap) -> Status {
        if (gap == 0) {
            return Status::OK();
        }
        DORIS_CHECK(tablet_column.is_nullable());
        return writer->append_nulls(gap);
    };

    size_t next_row = 0;
    size_t value_index = 0;
    while (value_index < rowids.size()) {
        const size_t row = rowids[value_index];
        DORIS_CHECK_GE(row, next_row);
        RETURN_IF_ERROR(append_gap(row - next_row));

        size_t run_length = 1;
        while (value_index + run_length < rowids.size() &&
               rowids[value_index + run_length] == row + run_length) {
            ++run_length;
        }
        const uint8_t* run_nullmap = nullmap == nullptr ? nullptr : nullmap + value_index;
        RETURN_IF_ERROR(writer->append(run_nullmap, data + cell_size * value_index, run_length));
        value_index += run_length;
        next_row = row + run_length;
    }
    RETURN_IF_ERROR(append_gap(total_rows - next_row));
    converter->clear_source_content(column_id);
    return Status::OK();
}

} // namespace

Status append_sparse_converted_column(const TabletColumn& tablet_column, ColumnWriter* writer,
                                      OlapBlockDataConvertor* converter, int column_id,
                                      const DataTypePtr& type, const ColumnPtr& values_column,
                                      std::span<const uint32_t> rowids, size_t total_rows) {
    DORIS_CHECK_EQ(values_column->size(), rowids.size());
    if (!rowids.empty()) {
        DORIS_CHECK_LT(rowids.back(), total_rows);
    }

    // Column ids and convertor slots advance together across physical paths. Reserve exactly one
    // slot at the common entry point, including empty segments and paths whose forced cast removed
    // every value; otherwise the next path is registered at this index but queried by the next id.
    converter->add_column_data_convertor(tablet_column);
    if (total_rows == 0) {
        return Status::OK();
    }
    if (rowids.empty()) {
        DORIS_CHECK(tablet_column.is_nullable());
        return writer->append_nulls(total_rows);
    }

    const DataTypePtr base_type = remove_nullable(type);
    if (base_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
        return append_sparse_array_column(tablet_column, writer, converter, column_id, type,
                                          values_column, rowids, total_rows);
    }
    return append_sparse_scalar_column(tablet_column, writer, converter, column_id, type,
                                       values_column, rowids, total_rows);
}

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

    const bool need_record_none_null_value_size =
            (!tablet_column.path_info_ptr()->get_is_typed() ||
             parent_column.variant_enable_typed_paths_to_sparse()) &&
            !tablet_column.path_info_ptr()->has_nested_part() &&
            variant_util::should_record_variant_path_stats(parent_column);

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(create_column_writer(
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

} // namespace doris::segment_v2::variant_writer_helpers

namespace doris::segment_v2 {

void _init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                       const ColumnWriterOptions& opts) {
    variant_writer_helpers::init_column_meta(meta, column_id, column, opts);
}

} // namespace doris::segment_v2
