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
#include <charconv>
#include <memory>
#include <ranges>
#include <span>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/typeid_cast.h"
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
#include "storage/segment/variant/v2/variant_column_writer.h"
#include "storage/segment/variant/v2/variant_path_builder.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

namespace {
Status parse_doc_compact_bucket(std::string_view path, int* bucket) {
    DORIS_CHECK(bucket != nullptr);
    const std::string marker = "." + DOC_VALUE_COLUMN_PATH + ".b";
    const size_t marker_pos = path.rfind(marker);
    if (marker_pos == std::string_view::npos) {
        return Status::Corruption("Invalid Variant doc compact path {}", path);
    }
    const std::string_view suffix = path.substr(marker_pos + marker.size());
    const auto [end, error] =
            std::from_chars(suffix.data(), suffix.data() + suffix.size(), *bucket);
    if (error != std::errc() || end != suffix.data() + suffix.size() || *bucket < 0) {
        return Status::Corruption("Invalid Variant doc compact bucket in path {}", path);
    }
    return Status::OK();
}

Status finish_and_write_column_writer(ColumnWriter* writer) {
    RETURN_IF_ERROR(writer->finish());
    RETURN_IF_ERROR(writer->write_data());
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

Status UnifiedSparseColumnWriter::append_shredded(const TabletColumn* parent_column,
                                                  const VariantShreddedColumns& shredded,
                                                  size_t num_rows,
                                                  OlapBlockDataConvertor* converter) {
    if (shredded.binary_buckets.size() != cast_set<size_t>(_bucket_num)) {
        return Status::InvalidArgument("Variant shredder produced {} sparse buckets, expected {}",
                                       shredded.binary_buckets.size(), _bucket_num);
    }
    converter->resize(_first_column_id + _bucket_num);
    for (int bucket = 0; bucket < _bucket_num; ++bucket) {
        const auto& source = shredded.binary_buckets[bucket];
        if (source.column->size() != num_rows) {
            return Status::InvalidArgument("Variant sparse bucket {} has {} rows, expected {}",
                                           bucket, source.column->size(), num_rows);
        }
        TabletColumn bucket_column =
                _bucket_num == 1 ? variant_util::create_sparse_column(*parent_column)
                                 : variant_util::create_sparse_shard_column(*parent_column, bucket);
        const int column_id = _first_column_id + bucket;
        if (num_rows > 0) {
            converter->add_column_data_convertor_at(bucket_column, column_id);
            RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                    {source.column, variant_util::get_variant_binary_column_type(), ""}, 0,
                    num_rows, column_id));
            auto [status, converted] = converter->convert_column_data(column_id);
            RETURN_IF_ERROR(status);
            ColumnWriter* writer =
                    _bucket_num == 1 ? _single_writer.get() : _bucket_writers[bucket].get();
            RETURN_IF_ERROR(
                    writer->append(converted->get_nullmap(), converted->get_data(), num_rows));
            converter->clear_source_content(column_id);
        }

        ColumnWriterOptions& opts = _bucket_num == 1 ? _single_opts : _bucket_opts[bucket];
        source.statistics.to_pb(opts.meta->mutable_variant_statistics());
        opts.meta->set_num_rows(num_rows);
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
    variant_writer_helpers::init_column_meta(_single_opts.meta, column_id, sparse_column,
                                             base_opts);
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
        variant_writer_helpers::init_column_meta(_bucket_opts[b].meta, column_id, bucket_col,
                                                 base_opts);
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
// Bucketized sparse mode path:
// - Materialize N temporary ColumnMap (keys, values, offsets)
// - For each row, distribute (path,value) pairs to the bucket decided by
//   variant_util::variant_sparse_shard_of(path)
// - Convert and append each bucket map to its writer using the column id
//   sequence initialized by init_buckets (starting at _first_column_id)
// - Compute per-bucket path stats and persist into each bucket's meta
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
        variant_writer_helpers::init_column_meta(_doc_value_column_opts[b].meta, column_id,
                                                 bucket_column, opts);
        RETURN_IF_ERROR(ColumnWriter::create_map_writer(_doc_value_column_opts[b], &bucket_column,
                                                        opts.file_writer,
                                                        &_doc_value_column_writers[b]));
        RETURN_IF_ERROR(_doc_value_column_writers[b]->init());
        ++column_id;
    }
    return Status::OK();
}

Status VariantDocWriter::append_shredded(const TabletColumn* parent_column,
                                         const VariantShreddedColumns& shredded, size_t num_rows,
                                         OlapBlockDataConvertor* converter) {
    if (shredded.binary_buckets.size() != cast_set<size_t>(_bucket_num)) {
        return Status::InvalidArgument("Variant shredder produced {} doc buckets, expected {}",
                                       shredded.binary_buckets.size(), _bucket_num);
    }
    converter->resize(_first_column_id + _bucket_num);
    for (int bucket = 0; bucket < _bucket_num; ++bucket) {
        const auto& source = shredded.binary_buckets[bucket];
        if (source.column->size() != num_rows) {
            return Status::InvalidArgument("Variant doc bucket {} has {} rows, expected {}", bucket,
                                           source.column->size(), num_rows);
        }
        TabletColumn bucket_column = variant_util::create_doc_value_column(*parent_column, bucket);
        const int column_id = _first_column_id + bucket;
        if (num_rows > 0) {
            converter->add_column_data_convertor_at(bucket_column, column_id);
            RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                    {source.column, variant_util::get_variant_binary_column_type(), ""}, 0,
                    num_rows, column_id));
            auto [status, converted] = converter->convert_column_data(column_id);
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(_doc_value_column_writers[bucket]->append(
                    converted->get_nullmap(), converted->get_data(), num_rows));
            converter->clear_source_content(column_id);
        }

        source.statistics.to_pb(_doc_value_column_opts[bucket].meta->mutable_variant_statistics());
        _doc_value_column_opts[bucket].meta->set_num_rows(num_rows);
    }
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

VariantColumnWriterImpl::VariantColumnWriterImpl(ColumnWriterOptions opts,
                                                 const TabletColumn* column)
        : _opts(std::move(opts)), _tablet_column(column) {}

VariantColumnWriterImpl::~VariantColumnWriterImpl() = default;

Status VariantColumnWriterImpl::init() {
    DORIS_CHECK(!_initialized);
    _initialized = true;
    return _ensure_writer();
}

Status VariantColumnWriterImpl::_ensure_writer() {
    DORIS_CHECK(_initialized);
    if (_v2_writer) {
        return Status::OK();
    }
    auto writer = std::make_unique<VariantV2ColumnWriter>(_opts, _tablet_column);
    RETURN_IF_ERROR(writer->init());
    _v2_writer = std::move(writer);
    return Status::OK();
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    if (ptr == nullptr || *ptr == nullptr) {
        return Status::InvalidArgument("Variant writer received null column data");
    }
    const auto& column = *reinterpret_cast<const VariantColumnData*>(*ptr);
    VariantWriterInputFormat input_format;
    RETURN_IF_ERROR(variant_writer_helpers::classify_variant_writer_input(
            column, VariantWriterInputFormat::UNSET, "Variant writer", &input_format));
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->append(column, num_rows, {});
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    if (ptr == nullptr || *ptr == nullptr) {
        return Status::InvalidArgument("Variant writer received null column data");
    }
    const auto& column = *reinterpret_cast<const VariantColumnData*>(*ptr);
    VariantWriterInputFormat input_format;
    RETURN_IF_ERROR(variant_writer_helpers::classify_variant_writer_input(
            column, VariantWriterInputFormat::UNSET, "Variant writer", &input_format));
    RETURN_IF_ERROR(_ensure_writer());
    const std::span<const uint8_t> outer_nulls =
            null_map == nullptr ? std::span<const uint8_t> {}
                                : std::span<const uint8_t> {null_map, num_rows};
    return _v2_writer->append(column, num_rows, outer_nulls);
}

Status VariantColumnWriterImpl::finalize() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->finalize();
}

bool VariantColumnWriterImpl::is_finalized() const {
    return _v2_writer && _v2_writer->is_finalized();
}

bool VariantColumnWriterImpl::has_streaming_compaction_writer_for_test() const {
    return false;
}

Status VariantColumnWriterImpl::finish() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->finish();
}

Status VariantColumnWriterImpl::write_data() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->write_data();
}

Status VariantColumnWriterImpl::write_ordinal_index() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->write_ordinal_index();
}

Status VariantColumnWriterImpl::write_zone_map() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->write_zone_map();
}

Status VariantColumnWriterImpl::write_inverted_index() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->write_inverted_index();
}

Status VariantColumnWriterImpl::write_bloom_filter_index() {
    RETURN_IF_ERROR(_ensure_writer());
    return _v2_writer->write_bloom_filter_index();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    return _v2_writer ? _v2_writer->estimate_buffer_size() : 0;
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
}

VariantSubcolumnWriter::~VariantSubcolumnWriter() = default;

Status VariantSubcolumnWriter::init() {
    return Status::OK();
}

Status VariantSubcolumnWriter::_initialize_v2_builder() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    RETURN_IF_ERROR(variant_writer_helpers::validate_variant_v2_writer_layout(
            *_opts.rowset_ctx->tablet_schema, parent_column));
    DORIS_CHECK(get_column()->path_info_ptr() != nullptr);
    _v2_builder =
            std::make_unique<VariantPathBuilder>(get_column()->path_info_ptr()->copy_pop_front());
    return Status::OK();
}

Status VariantSubcolumnWriter::_ensure_input_format(const VariantColumnData& column) {
    VariantWriterInputFormat input_format;
    RETURN_IF_ERROR(variant_writer_helpers::classify_variant_writer_input(
            column, _input_format, "Variant subcolumn writer", &input_format));
    if (_input_format != VariantWriterInputFormat::UNSET) {
        return Status::OK();
    }
    if (input_format == VariantWriterInputFormat::V2) {
        RETURN_IF_ERROR(_initialize_v2_builder());
    }
    _input_format = input_format;
    return Status::OK();
}

Status VariantSubcolumnWriter::_append_v2(const VariantColumnData& column, size_t num_rows,
                                          std::span<const uint8_t> outer_nulls) {
    DORIS_CHECK(_v2_builder != nullptr);
    DORIS_CHECK(column.column_data != nullptr);
    const auto* source = check_and_get_column<ColumnVariantV2>(*column.column_data);
    DORIS_CHECK(source != nullptr);
    if (column.row_pos > source->size() || num_rows > source->size() - column.row_pos) {
        return Status::InvalidArgument("ColumnVariantV2 writer range [{}, {}) exceeds {} rows",
                                       column.row_pos, column.row_pos + num_rows, source->size());
    }
    DORIS_CHECK(outer_nulls.empty() || outer_nulls.size() == num_rows);

    const auto append_encoded = [&](const ColumnVariantV2::ReadView& view, size_t begin) -> Status {
        DORIS_CHECK(!view.is_typed());
        for (size_t offset = 0; offset < num_rows; ++offset) {
            if (!outer_nulls.empty() && outer_nulls[offset] != 0) {
                continue;
            }
            const size_t input_row = begin + offset;
            const VariantRef value = view.value_at(input_row);
            if (!value.is_null()) {
                RETURN_IF_ERROR(_v2_builder->append(value, _num_rows + offset));
            }
        }
        return Status::OK();
    };

    const auto view = source->read_view();
    if (!view.is_typed()) {
        return append_encoded(view, column.row_pos);
    }

    auto encoded_batch = ColumnVariantV2::create();
    RETURN_IF_CATCH_EXCEPTION(
            { encoded_batch->insert_range_from(*source, column.row_pos, num_rows); });
    return append_encoded(encoded_batch->read_view(), 0);
}

Status VariantSubcolumnWriter::_append(const uint8_t* null_map, const uint8_t** ptr,
                                       size_t num_rows) {
    if (ptr == nullptr || *ptr == nullptr) {
        return Status::InvalidArgument("Variant subcolumn writer received null column data");
    }
    if (_is_finalized) {
        return Status::InternalError("Cannot append Variant subcolumn after writer finalization");
    }
    const auto& column = *reinterpret_cast<const VariantColumnData*>(*ptr);
    RETURN_IF_ERROR(_ensure_input_format(column));
    const std::span<const uint8_t> outer_nulls =
            null_map == nullptr ? std::span<const uint8_t> {}
                                : std::span<const uint8_t> {null_map, num_rows};
    RETURN_IF_ERROR(_append_v2(column, num_rows, outer_nulls));
    _num_rows += num_rows;
    _next_rowid += num_rows;
    return Status::OK();
}

Status VariantSubcolumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    return _append(nullptr, ptr, num_rows);
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    if (is_finalized()) {
        return _writer ? _writer->estimate_buffer_size() : 0;
    }
    return _v2_builder ? _v2_builder->byte_size() : 0;
}

bool VariantSubcolumnWriter::is_finalized() const {
    return _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    if (_is_finalized) {
        return Status::OK();
    }
    if (_input_format == VariantWriterInputFormat::UNSET) {
        RETURN_IF_ERROR(_initialize_v2_builder());
        _input_format = VariantWriterInputFormat::V2;
    }
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    DORIS_CHECK(!parent_column.variant_enable_nested_group());

    DataTypePtr flush_type;
    ColumnPtr flush_values;
    DorisVector<uint32_t> flush_rowids;
    int64_t non_null_value_size = 0;
    DORIS_CHECK(_v2_builder != nullptr);
    RETURN_IF_ERROR(_v2_builder->complete_rows(_num_rows));
    DataTypePtr storage_type;
    TabletSchema::SubColumnInfo subcolumn_info;
    if (variant_util::generate_sub_column_info(*_opts.rowset_ctx->tablet_schema,
                                               parent_column.unique_id(),
                                               _v2_builder->path().get_path(), &subcolumn_info)) {
        storage_type = DataTypeFactory::instance().create_data_type(subcolumn_info.column);
    }

    bool publish_builder = _v2_builder->non_null_rows() != 0;
    if (publish_builder) {
        RETURN_IF_ERROR(_v2_builder->convert_to(
                normalize_variant_path_integer_widths(_v2_builder->type())));
        publish_builder =
                !variant_path_type_contains_nothing(_v2_builder->type()) ||
                (storage_type != nullptr && !variant_path_type_contains_nothing(storage_type));
    }
    if (publish_builder && storage_type != nullptr) {
        RETURN_IF_ERROR(_v2_builder->convert_to(storage_type));
    }

    if (!publish_builder) {
        flush_type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT,
                                                                  true /* is_nullable */);
        MutableColumnPtr defaults = flush_type->create_column();
        flush_values = std::move(defaults);
    } else {
        flush_type = _v2_builder->type();
        flush_values = _v2_builder->column();
        const std::span<const uint32_t> rowids = _v2_builder->rowids();
        flush_rowids.assign(rowids.begin(), rowids.end());
        non_null_value_size = cast_set<int64_t>(flush_rowids.size());
    }
    _v2_builder.reset();

    TabletColumn flush_column = variant_util::get_column_by_type(
            flush_type, get_column()->name(),
            variant_util::ExtraInfo {.unique_id = -1,
                                     .parent_unique_id = get_column()->parent_unique_id(),
                                     .path_info = *get_column()->path_info_ptr()});

    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    variant_util::inherit_column_attributes(parent_column, flush_column);
    RETURN_IF_ERROR(variant_writer_helpers::create_column_writer(
            0, flush_column, _opts.rowset_ctx->tablet_schema, _opts.index_file_writer, &_writer,
            _indexes, &opts, non_null_value_size, need_record_none_null_value_size));

    _opts = opts;
    OlapBlockDataConvertor converter;
    RETURN_IF_ERROR(variant_writer_helpers::append_sparse_converted_column(
            flush_column, _writer.get(), &converter, 0, flush_type, flush_values, flush_rowids,
            _num_rows));
    _opts.meta->set_num_rows(_num_rows);
    none_null_size = cast_set<size_t>(non_null_value_size);

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
    return _append(null_map, ptr, num_rows);
}

VariantDocCompactWriter::VariantDocCompactWriter(const ColumnWriterOptions& opts,
                                                 TabletColumnPtr column)
        : ColumnWriter(std::move(column), opts.meta->is_nullable(), opts.meta) {
    _opts = opts;
}

VariantDocCompactWriter::~VariantDocCompactWriter() = default;

Status VariantDocCompactWriter::init() {
    return Status::OK();
}

Status VariantDocCompactWriter::_initialize_v2_shredder() {
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    VariantShredderOptions options;
    RETURN_IF_ERROR(variant_writer_helpers::make_variant_shredder_options(
            *_opts.rowset_ctx->tablet_schema, parent_column, VariantShredderPhysicalLayout::DOC,
            &options));
    _v2_shredder = std::make_unique<VariantShredder>(std::move(options));
    return Status::OK();
}

Status VariantDocCompactWriter::_ensure_input_format(const VariantColumnData& column) {
    VariantWriterInputFormat input_format;
    RETURN_IF_ERROR(variant_writer_helpers::classify_variant_writer_input(
            column, _input_format, "Variant doc compact writer", &input_format));
    if (_input_format != VariantWriterInputFormat::UNSET) {
        return Status::OK();
    }
    if (input_format == VariantWriterInputFormat::V2) {
        RETURN_IF_ERROR(_initialize_v2_shredder());
    }
    _input_format = input_format;
    return Status::OK();
}

Status VariantDocCompactWriter::_append(const uint8_t* null_map, const uint8_t** ptr,
                                        size_t num_rows) {
    if (ptr == nullptr || *ptr == nullptr) {
        return Status::InvalidArgument("Variant doc compact writer received null column data");
    }
    if (_is_finalized) {
        return Status::InternalError("Cannot append Variant doc compact after writer finalization");
    }
    const auto& column = *reinterpret_cast<const VariantColumnData*>(*ptr);
    RETURN_IF_ERROR(_ensure_input_format(column));
    const std::span<const uint8_t> outer_nulls =
            null_map == nullptr ? std::span<const uint8_t> {}
                                : std::span<const uint8_t> {null_map, num_rows};
    RETURN_IF_ERROR(variant_writer_helpers::append_variant_v2_to_shredder(
            _v2_shredder.get(), column, num_rows, outer_nulls));
    _num_rows += num_rows;
    _next_rowid += num_rows;
    return Status::OK();
}

Status VariantDocCompactWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    return _append(nullptr, ptr, num_rows);
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
    return _append(null_map, ptr, num_rows);
}

Status VariantDocCompactWriter::_write_materialized_subcolumns(
        const TabletColumn& parent_column, const VariantShreddedColumns& shredded,
        OlapBlockDataConvertor* converter, size_t num_rows, int& column_id) {
    for (const VariantPathColumn& path_column : shredded.materialized) {
        if (!path_column.column || path_column.column->size() != path_column.rowids.size()) {
            return Status::InvalidArgument(
                    "Variant doc compact materialized path {} has {} compact values for {} row ids",
                    path_column.path.get_path(),
                    path_column.column ? path_column.column->size() : 0, path_column.rowids.size());
        }
        TabletIndexes indexes;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        TabletColumn tablet_column;
        const int current_column_id = column_id++;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, parent_column, current_column_id, path_column.path, path_column.type,
                cast_set<int64_t>(path_column.rowids.size()), num_rows, nullptr, true, &indexes,
                &opts, &writer, &tablet_column));
        RETURN_IF_ERROR(variant_writer_helpers::append_sparse_converted_column(
                tablet_column, writer.get(), converter, current_column_id, path_column.type,
                path_column.column, path_column.rowids, num_rows));
        RETURN_IF_ERROR(finish_and_write_column_writer(writer.get()));
        _subcolumns_indexes.push_back(std::move(indexes));
        _subcolumn_opts.push_back(opts);
        _subcolumn_writers.push_back(std::move(writer));
    }
    return Status::OK();
}

Status VariantDocCompactWriter::_write_doc_value_column(const TabletColumn& parent_column,
                                                        int bucket_value,
                                                        const ColumnPtr& source_column,
                                                        const DataTypePtr& source_type,
                                                        OlapBlockDataConvertor* converter,
                                                        int column_id, size_t num_rows) {
    TabletColumn doc_value_column =
            variant_util::create_doc_value_column(parent_column, bucket_value);
    variant_writer_helpers::init_column_meta(_opts.meta, column_id, doc_value_column, _opts);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(_opts, &doc_value_column, _opts.file_writer,
                                                    &_doc_value_column_writer));
    RETURN_IF_ERROR(_doc_value_column_writer->init());

    if (num_rows > 0) {
        converter->resize(column_id + 1);
        converter->add_column_data_convertor_at(doc_value_column, column_id);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {source_column, source_type, ""}, 0, num_rows, column_id));
        auto [status, column] = converter->convert_column_data(column_id);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(_doc_value_column_writer->append(column->get_nullmap(), column->get_data(),
                                                         num_rows));
        converter->clear_source_content(column_id);
    }
    _opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

Status VariantDocCompactWriter::_finalize_v2(const TabletColumn& parent_column, size_t num_rows,
                                             OlapBlockDataConvertor* converter, int& column_id) {
    DORIS_CHECK(_v2_shredder != nullptr);
    VariantShreddedColumns shredded;
    RETURN_IF_ERROR(_v2_shredder->finish(&shredded));
    DORIS_CHECK_EQ(shredded.num_rows, num_rows);
    _v2_shredder.reset();

    int bucket_value = -1;
    RETURN_IF_ERROR(
            parse_doc_compact_bucket(get_column()->path_info_ptr()->get_path(), &bucket_value));
    if (cast_set<size_t>(bucket_value) >= shredded.binary_buckets.size()) {
        return Status::Corruption("Invalid Variant doc compact bucket {} in path {}", bucket_value,
                                  get_column()->path_info_ptr()->get_path());
    }
    for (size_t bucket = 0; bucket < shredded.binary_buckets.size(); ++bucket) {
        const auto& source = shredded.binary_buckets[bucket];
        const auto* map = check_and_get_column<ColumnMap>(source.column.get());
        if (map == nullptr || map->size() != num_rows) {
            return Status::Corruption("Variant doc compact bucket {} is not a {}-row map", bucket,
                                      num_rows);
        }
        const bool has_values =
                !map->get_offsets().empty() && map->get_offsets().back() != map->get_offsets()[-1];
        if (bucket != cast_set<size_t>(bucket_value) &&
            (has_values || !source.statistics.doc_value_column_non_null_size.empty())) {
            return Status::InvalidArgument(
                    "Variant doc compact input for bucket {} contains data for bucket {}",
                    bucket_value, bucket);
        }
    }

    RETURN_IF_ERROR(_write_materialized_subcolumns(parent_column, shredded, converter, num_rows,
                                                   column_id));
    const auto& source = shredded.binary_buckets[bucket_value];
    RETURN_IF_ERROR(_write_doc_value_column(parent_column, bucket_value, source.column,
                                            variant_util::get_variant_binary_column_type(),
                                            converter, column_id, num_rows));
    RETURN_IF_ERROR(finish_and_write_column_writer(_doc_value_column_writer.get()));
    source.statistics.to_pb(_opts.meta->mutable_variant_statistics());
    return Status::OK();
}

Status VariantDocCompactWriter::finalize() {
    if (_is_finalized) {
        return Status::OK();
    }
    if (_input_format == VariantWriterInputFormat::UNSET) {
        RETURN_IF_ERROR(_initialize_v2_shredder());
        _input_format = VariantWriterInputFormat::V2;
    }
    const auto& parent_column =
            _opts.rowset_ctx->tablet_schema->column_by_uid(get_column()->parent_unique_id());
    auto converter = std::make_unique<OlapBlockDataConvertor>();
    int column_id = 0;

    _subcolumn_writers.clear();
    _subcolumns_indexes.clear();
    _subcolumn_opts.clear();
    RETURN_IF_ERROR(_finalize_v2(parent_column, _num_rows, converter.get(), column_id));
    _opts.meta->set_num_rows(_num_rows);
    _data_written = true;
    _is_finalized = true;
    return Status::OK();
}

uint64_t VariantDocCompactWriter::estimate_buffer_size() {
    return _v2_shredder ? _v2_shredder->byte_size() : 0;
}

} // namespace doris::segment_v2
