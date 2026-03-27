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

#include "storage/segment/variant/variant_streaming_compaction_writer.h"

#include <memory>

#include "common/cast_set.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "exec/common/variant_util.h"
#include "storage/index/indexed_column_writer.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "storage/types.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

VariantStreamingCompactionWriter::VariantStreamingCompactionWriter(
        const ColumnWriterOptions& opts, const TabletColumn* column,
        NestedGroupWriteProvider* nested_group_provider, VariantStatistics* statistics)
        : _opts(opts),
          _tablet_column(column),
          _nested_group_provider(nested_group_provider),
          _statistics(statistics) {}

Status VariantStreamingCompactionWriter::init() {
    RETURN_IF_ERROR(build_nested_group_streaming_write_plan(_opts.input_rs_readers, *_tablet_column,
                                                            &_streaming_plan));
    RETURN_IF_ERROR(_init_root_writer());
    int column_id = 1;
    RETURN_IF_ERROR(_init_regular_subcolumn_writers(column_id));
    RETURN_IF_ERROR(_nested_group_provider->init_with_plan(_streaming_plan, _tablet_column, _opts,
                                                           &column_id, _statistics));
    _statistics->to_pb(_opts.meta->mutable_variant_statistics());
    _phase = Phase::INITIALIZED;
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_init_root_writer() {
    _root_writer = std::make_unique<ScalarColumnWriter>(
            _opts, std::unique_ptr<StorageField>(StorageFieldFactory::create(*_tablet_column)),
            _opts.file_writer);
    RETURN_IF_ERROR(_root_writer->init());
    _opts.meta->set_num_rows(0);
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_init_regular_subcolumn_writers(int& column_id) {
    _streaming_regular_subcolumn_writers.clear();
    for (const auto& plan_entry : _streaming_plan.regular_subcolumns) {
        TabletColumn tablet_column;
        TabletIndexes subcolumn_indexes;
        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(variant_writer_helpers::prepare_subcolumn_writer_target(
                _opts, *_tablet_column, column_id, plan_entry.path_in_data, plan_entry.data_type, 0,
                0, nullptr /* existing_subcolumn_info */, false /* check_storage_type */,
                &subcolumn_indexes, &opts, &writer, &tablet_column));
        auto converter = std::make_unique<OlapBlockDataConvertor>();
        converter->add_column_data_convertor(tablet_column);
        _subcolumns_indexes.push_back(std::move(subcolumn_indexes));
        _subcolumn_opts.push_back(opts);
        _subcolumn_writers.push_back(std::move(writer));
        _streaming_regular_subcolumn_writers.push_back(
                StreamingRegularSubcolumnWriter {.plan = plan_entry,
                                                 .tablet_column = std::move(tablet_column),
                                                 .converter = std::move(converter)});
        ++column_id;
    }
    return Status::OK();
}

Status VariantStreamingCompactionWriter::append_data(const uint8_t** ptr, size_t num_rows,
                                                     const uint8_t* outer_null_map) {
    RETURN_IF_ERROR(_check_initialized("append_data"));
    RETURN_IF_ERROR(_append_input_from_raw(ptr, num_rows, outer_null_map));
    if (num_rows > 0 && _phase == Phase::INITIALIZED) {
        _phase = Phase::APPENDING;
    }
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_append_input_from_raw(const uint8_t** ptr,
                                                                size_t num_rows,
                                                                const uint8_t* outer_null_map) {
    const auto* column = reinterpret_cast<const VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const ColumnVariant*>(column->column_data);
    RETURN_IF_ERROR(src.sanitize());
    return _append_input(src, column->row_pos, num_rows, outer_null_map);
}

Status VariantStreamingCompactionWriter::_append_input(const ColumnVariant& src, size_t row_pos,
                                                       size_t num_rows,
                                                       const uint8_t* outer_null_map) {
    auto chunk_variant = ColumnVariant::create(0);
    chunk_variant->insert_range_from(src, row_pos, num_rows);
    RETURN_IF_ERROR(chunk_variant->sanitize());
    chunk_variant->finalize();
    return _append_chunk(*chunk_variant, outer_null_map);
}

Status VariantStreamingCompactionWriter::_append_root_column(const ColumnVariant& chunk_variant,
                                                             const uint8_t* outer_null_map) {
    auto* variant = const_cast<ColumnVariant*>(&chunk_variant);
    auto expected_root_type = make_nullable(std::make_shared<ColumnVariant::MostCommonType>());
    variant->ensure_root_node_type(expected_root_type);

    auto& nullable_column = assert_cast<ColumnNullable&>(*variant->get_root()->assume_mutable());
    auto root_column = nullable_column.get_nested_column_ptr();
    const size_t num_rows = chunk_variant.rows();
    variant_writer_helpers::maybe_remove_root_jsonb_with_empty_defaults(
            &root_column, num_rows, _streaming_plan.can_remove_root_jsonb());

    const uint8_t* nullmap = nullptr;
    if (_tablet_column->is_nullable()) {
        auto null_column = ColumnUInt8::create();
        if (outer_null_map != nullptr) {
            null_column->insert_many_raw_data(reinterpret_cast<const char*>(outer_null_map),
                                              num_rows);
            nullmap = outer_null_map;
        } else {
            null_column->insert_many_defaults(num_rows);
        }
        root_column = ColumnNullable::create(root_column->get_ptr(), std::move(null_column));
    } else {
        root_column = ColumnNullable::create(root_column->get_ptr(),
                                             ColumnUInt8::create(root_column->size(), 0));
    }

    auto converter = std::make_unique<OlapBlockDataConvertor>();
    converter->add_column_data_convertor(*_tablet_column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {root_column->get_ptr(), nullptr, ""}, 0, num_rows, 0));
    auto [status, column] = converter->convert_column_data(0);
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(_root_writer->append(nullmap, column->get_data(), num_rows));
    converter->clear_source_content(0);
    _opts.meta->set_num_rows(_root_writer->get_next_rowid());
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_append_regular_subcolumns(
        const ColumnVariant& chunk_variant) {
    const size_t num_rows = chunk_variant.rows();
    for (size_t i = 0; i < _streaming_regular_subcolumn_writers.size(); ++i) {
        auto& state = _streaming_regular_subcolumn_writers[i];
        auto* subcolumn = chunk_variant.get_subcolumn(state.plan.path_in_data);
        ColumnWriter* writer = _subcolumn_writers[i].get();
        if (subcolumn == nullptr || subcolumn->get_least_common_type() == nullptr) {
            DCHECK(state.tablet_column.is_nullable());
            RETURN_IF_ERROR(writer->append_nulls(num_rows));
            _subcolumn_opts[i].meta->set_num_rows(writer->get_next_rowid());
            continue;
        }
        auto base_type = variant_util::get_base_type_of_array(subcolumn->get_least_common_type());
        if (base_type != nullptr &&
            base_type->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
            DCHECK(state.tablet_column.is_nullable());
            RETURN_IF_ERROR(writer->append_nulls(num_rows));
            _subcolumn_opts[i].meta->set_num_rows(writer->get_next_rowid());
            continue;
        }
        if (!subcolumn->is_finalized()) {
            const_cast<ColumnVariant::Subcolumn*>(subcolumn)->finalize();
        }
        ColumnPtr current_column = subcolumn->get_finalized_column_ptr()->get_ptr();
        DataTypePtr current_type = subcolumn->get_least_common_type();
        if (!state.plan.data_type->equals(*current_type)) {
            RETURN_IF_ERROR(variant_util::cast_column({current_column, current_type, ""},
                                                      state.plan.data_type, &current_column));
            current_type = state.plan.data_type;
        }
        DCHECK_EQ(current_column->size(), num_rows);
        // Keep one converter per writer so array/map offsets stay rebased across streaming chunks.
        RETURN_IF_ERROR(state.converter->set_source_content_with_specifid_column(
                {current_column, current_type, ""}, 0, num_rows, 0));
        auto [status, converted] = state.converter->convert_column_data(0);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(writer->append(converted->get_nullmap(), converted->get_data(), num_rows));
        state.converter->clear_source_content(0);
        _subcolumn_opts[i].meta->set_num_rows(writer->get_next_rowid());
    }
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_append_chunk(const ColumnVariant& chunk_variant,
                                                       const uint8_t* outer_null_map) {
    RETURN_IF_ERROR(_append_root_column(chunk_variant, outer_null_map));
    RETURN_IF_ERROR(_append_regular_subcolumns(chunk_variant));
    RETURN_IF_ERROR(_nested_group_provider->append_chunk(_streaming_plan, chunk_variant));
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_for_each_column_writer(
        const std::function<Status(ColumnWriter*)>& func) {
    RETURN_IF_ERROR(func(_root_writer.get()));
    for (auto& writer : _subcolumn_writers) {
        RETURN_IF_ERROR(func(writer.get()));
    }
    return Status::OK();
}

uint64_t VariantStreamingCompactionWriter::estimate_buffer_size() const {
    uint64_t size = 0;
    if (_root_writer) {
        size += _root_writer->estimate_buffer_size();
    }
    for (const auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    size += _nested_group_provider->estimate_buffer_size();
    return size;
}

Status VariantStreamingCompactionWriter::_check_initialized(std::string_view action) const {
    if (!is_initialized()) {
        return Status::InternalError(
                "VariantStreamingCompactionWriter must be initialized before "
                "{}",
                action);
    }
    if (_phase == Phase::CLOSED) {
        return Status::InternalError("VariantStreamingCompactionWriter is already closed: {}",
                                     action);
    }
    return Status::OK();
}

Status VariantStreamingCompactionWriter::_check_closed(std::string_view action) const {
    if (!is_finalized()) {
        return Status::InternalError("VariantStreamingCompactionWriter must be closed before {}",
                                     action);
    }
    return Status::OK();
}

Status VariantStreamingCompactionWriter::finish() {
    RETURN_IF_ERROR(_check_initialized("finish"));
    RETURN_IF_ERROR(_for_each_column_writer([](ColumnWriter* writer) { return writer->finish(); }));
    RETURN_IF_ERROR(_nested_group_provider->finish());
    _phase = Phase::CLOSED;
    return Status::OK();
}

Status VariantStreamingCompactionWriter::write_data() {
    RETURN_IF_ERROR(_check_closed("write_data"));
    RETURN_IF_ERROR(
            _for_each_column_writer([](ColumnWriter* writer) { return writer->write_data(); }));
    RETURN_IF_ERROR(_nested_group_provider->write_data());
    return Status::OK();
}

Status VariantStreamingCompactionWriter::write_ordinal_index() {
    RETURN_IF_ERROR(_check_closed("write_ordinal_index"));
    RETURN_IF_ERROR(_for_each_column_writer(
            [](ColumnWriter* writer) { return writer->write_ordinal_index(); }));
    RETURN_IF_ERROR(_nested_group_provider->write_ordinal_index());
    return Status::OK();
}

Status VariantStreamingCompactionWriter::write_zone_map() {
    RETURN_IF_ERROR(_check_closed("write_zone_map"));
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    RETURN_IF_ERROR(_nested_group_provider->write_zone_map());
    return Status::OK();
}

Status VariantStreamingCompactionWriter::write_inverted_index() {
    RETURN_IF_ERROR(_check_closed("write_inverted_index"));
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    RETURN_IF_ERROR(_nested_group_provider->write_inverted_index());
    return Status::OK();
}

Status VariantStreamingCompactionWriter::write_bloom_filter_index() {
    RETURN_IF_ERROR(_check_closed("write_bloom_filter_index"));
    for (size_t i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    RETURN_IF_ERROR(_nested_group_provider->write_bloom_filter_index());
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
