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
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
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
    opt->need_bitmap_index = column.has_bitmap_index();
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
                 vectorized::schema_util::inherit_index(parent_index, subcolumn_indexes, column)) {
            init_opt_inverted_index();
        }
        // no parent index and no subcolumn index
        else {
            opt->need_inverted_index = false;
        }
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE, type_name)          \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opt->need_zone_map = false;                           \
        opt->need_bloom_filter = false;                       \
        opt->need_bitmap_index = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY, "array")
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB, "jsonb")
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT, "variant")

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

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
}

Status VariantColumnWriterImpl::init() {
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    int count = _tablet_column->variant_max_subcolumns_count();
    if (_opts.rowset_ctx->write_type == DataWriteType::TYPE_DIRECT) {
        count = 0;
    }
    auto col = vectorized::ColumnVariant::create(count);
    _column = std::move(col);
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
                root_column->get_ptr(), vectorized::ColumnUInt8::create(_null_column));
        nullmap = _null_column.get_data().data();
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
        auto column = vectorized::schema_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::schema_util::ExtraInfo {.unique_id = -1,
                                                    .parent_unique_id = _tablet_column->unique_id(),
                                                    .path_info = full_path});
        return column;
    };
    _subcolumns_indexes.resize(ptr->get_subcolumns().size());
    // convert sub column data from engine format to storage layer format
    for (const auto& entry :
         vectorized::schema_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        const auto& least_common_type = entry->data.get_least_common_type();
        if (vectorized::schema_util::get_base_type_of_array(least_common_type)
                    ->get_primitive_type() == PrimitiveType::INVALID_TYPE) {
            continue;
        }
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        CHECK(entry->data.is_finalized());

        // create subcolumn writer
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
        std::unique_ptr<ColumnWriter> writer;
        vectorized::schema_util::inherit_column_attributes(*_tablet_column, tablet_column);

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

Status VariantColumnWriterImpl::_process_sparse_column(
        vectorized::ColumnVariant* ptr, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id) {
    // create sparse column writer
    TabletColumn sparse_column = vectorized::schema_util::create_sparse_column(*_tablet_column);
    ColumnWriterOptions sparse_writer_opts;
    sparse_writer_opts.meta = _opts.footer->add_columns();

    _init_column_meta(sparse_writer_opts.meta, column_id, sparse_column, _opts.compression_type);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(sparse_writer_opts, &sparse_column,
                                                    _opts.file_writer, &_sparse_column_writer));
    RETURN_IF_ERROR(_sparse_column_writer->init());

    // convert root column data from engine format to storage layer format
    converter->add_column_data_convertor(sparse_column);
    DCHECK_EQ(ptr->get_sparse_column()->size(), num_rows);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {ptr->get_sparse_column(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    RETURN_IF_ERROR(
            _sparse_column_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    converter->clear_source_content(column_id);
    ++column_id;

    // get stastics
    // todo: reuse the statics from collected stastics from compaction stage
    std::unordered_map<StringRef, size_t> sparse_data_paths_statistics;
    const auto [sparse_data_paths, _] = ptr->get_sparse_data_paths_and_values();
    for (size_t i = 0; i != sparse_data_paths->size(); ++i) {
        auto path = sparse_data_paths->get_data_at(i);
        if (auto it = sparse_data_paths_statistics.find(path);
            it != sparse_data_paths_statistics.end()) {
            ++it->second;
        } else if (sparse_data_paths_statistics.size() <
                   config::variant_max_sparse_column_statistics_size) {
            sparse_data_paths_statistics.emplace(path, 1);
        }
    }

    // assign to _statistics.sparse_column_non_null_size
    for (const auto& [path, size] : sparse_data_paths_statistics) {
        _statistics.sparse_column_non_null_size.emplace(path.to_string(), size);
    }
    // set statistics info
    _statistics.to_pb(sparse_writer_opts.meta->mutable_variant_statistics());
    sparse_writer_opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

Status VariantColumnWriterImpl::finalize() {
    auto* ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    ptr->set_max_subcolumns_count(_tablet_column->variant_max_subcolumns_count());
    ptr->finalize(vectorized::ColumnVariant::FinalizeMode::WRITE_MODE);
    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());

    for (const auto& entry :
         vectorized::schema_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        // Not supported nested path to generate sub column info, currently
        if (entry->path.has_nested_part()) {
            continue;
        }
        TabletSchema::SubColumnInfo sub_column_info;
        if (vectorized::schema_util::generate_sub_column_info(
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
        // process and append each subcolumns to sub columns writers buffer
        RETURN_IF_ERROR(_process_subcolumns(ptr, olap_data_convertor.get(), num_rows, column_id));

        // process sparse column and append to sparse writer buffer
        RETURN_IF_ERROR(
                _process_sparse_column(ptr, olap_data_convertor.get(), num_rows, column_id));
    }

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    const auto* ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    return ptr->is_finalized() && _is_finalized;
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    DCHECK(!is_finalized());
    const auto* column = reinterpret_cast<const vectorized::VariantColumnData*>(*ptr);
    const auto& src = *reinterpret_cast<const vectorized::ColumnVariant*>(column->column_data);
    RETURN_IF_ERROR(src.sanitize());
    auto* dst_ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, column->row_pos, num_rows);
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
    size += _sparse_column_writer ? _sparse_column_writer->estimate_buffer_size() : 0;
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
    if (_sparse_column_writer) {
        RETURN_IF_ERROR(_sparse_column_writer->finish());
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
    if (_sparse_column_writer) {
        RETURN_IF_ERROR(_sparse_column_writer->write_data());
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
    if (_sparse_column_writer) {
        RETURN_IF_ERROR(_sparse_column_writer->write_ordinal_index());
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
    return Status::OK();
}

Status VariantColumnWriterImpl::write_bitmap_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bitmap_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bitmap_index());
        }
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
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    assert(is_finalized());
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bloom_filter) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bloom_filter_index());
        }
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                                size_t num_rows) {
    if (null_map != nullptr) {
        _null_column.insert_many_raw_data((const char*)null_map, num_rows);
    }
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

VariantSubcolumnWriter::VariantSubcolumnWriter(const ColumnWriterOptions& opts,
                                               const TabletColumn* column,
                                               std::unique_ptr<Field> field)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()) {
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
    auto* dst_ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, column->row_pos, num_rows);
    return Status::OK();
}

uint64_t VariantSubcolumnWriter::estimate_buffer_size() {
    return _column->byte_size();
}

bool VariantSubcolumnWriter::is_finalized() const {
    const auto* ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
    return ptr->is_finalized() && _is_finalized;
}

Status VariantSubcolumnWriter::finalize() {
    auto* ptr = assert_cast<vectorized::ColumnVariant*>(_column.get());
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
    flush_column = vectorized::schema_util::get_column_by_type(
            ptr->get_root_type(), _tablet_column->name(),
            vectorized::schema_util::ExtraInfo {
                    .unique_id = -1,
                    .parent_unique_id = _tablet_column->parent_unique_id(),
                    .path_info = *_tablet_column->path_info_ptr()});

    int64_t none_null_value_size = ptr->get_subcolumns().get_root()->data.get_non_null_value_size();
    bool need_record_none_null_value_size = (!flush_column.path_info_ptr()->get_is_typed()) &&
                                            !flush_column.path_info_ptr()->has_nested_part();
    ColumnWriterOptions opts = _opts;

    // refresh opts and get writer with flush column
    vectorized::schema_util::inherit_column_attributes(parent_column, flush_column);
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

Status VariantSubcolumnWriter::write_bitmap_index() {
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

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
