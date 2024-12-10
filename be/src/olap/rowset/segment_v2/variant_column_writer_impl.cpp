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
#include "olap/rowset/segment_v2/variant_column_writer_impl.h"

#include "common/status.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/segment_loader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/columns_number.h"
#include "vec/common/schema_util.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::segment_v2 {

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
}

Status VariantColumnWriterImpl::init() {
    // caculate stats info
    std::set<std::string> dynamic_paths;
    RETURN_IF_ERROR(_get_subcolumn_paths_from_stats(dynamic_paths));
    if (dynamic_paths.empty()) {
        _column = vectorized::ColumnObject::create(true, false);
    } else {
        vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
        for (const auto& path : dynamic_paths) {
            dynamic_subcolumns.add(vectorized::PathInData(path),
                                   vectorized::ColumnObject::Subcolumn {0, true});
        }
        _column = vectorized::ColumnObject::create(dynamic_subcolumns, true);
    }
    if (_tablet_column->is_nullable()) {
        _null_column = vectorized::ColumnUInt8::create(0);
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_get_subcolumn_paths_from_stats(std::set<std::string>& paths) {
    std::unordered_map<StringRef, size_t> path_to_total_number_of_non_null_values;

    // Merge and collect all stats info from all input rowsets
    for (RowsetReaderSharedPtr reader : _opts.input_rs_readers) {
        SegmentCacheHandle segment_cache;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(reader->rowset()), &segment_cache));
        for (const auto& segment : segment_cache.get_segments()) {
            const auto* source_statistics = segment->get_variant_stats(_tablet_column->unique_id());
            if (source_statistics) {
                for (const auto& [path, size] : source_statistics->_subcolumns_non_null_size) {
                    auto it = path_to_total_number_of_non_null_values.find(path);
                    if (it == path_to_total_number_of_non_null_values.end()) {
                        it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
                    }
                    it->second += size;
                }
                for (const auto& [path, size] : source_statistics->_sparse_column_non_null_size) {
                    auto it = path_to_total_number_of_non_null_values.find(path);
                    if (it == path_to_total_number_of_non_null_values.end()) {
                        it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
                    }
                    it->second += size;
                }
            }
        }
    }
    // Check if the number of all dynamic paths exceeds the limit.
    if (path_to_total_number_of_non_null_values.size() > vectorized::ColumnObject::MAX_SUBCOLUMNS) {
        // Sort paths by total number of non null values.
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(path_to_total_number_of_non_null_values.size());
        for (const auto& [path, size] : path_to_total_number_of_non_null_values) {
            paths_with_sizes.emplace_back(size, path);
        }
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());

        // Fill dynamic_paths with first max_dynamic_paths paths in sorted list.
        for (const auto& [size, path] : paths_with_sizes) {
            if (paths.size() < vectorized::ColumnObject::MAX_SUBCOLUMNS) {
                paths.emplace(path);
            }
            // // todo : Add all remaining paths into shared data statistics until we reach its max size;
            // else if (new_statistics.shared_data_paths_statistics.size() < Statistics::MAX_SHARED_DATA_STATISTICS_SIZE) {
            //     new_statistics.shared_data_paths_statistics.emplace(path, size);
            // }
        }
    } else {
        // Use all dynamic paths from all source columns.
        for (const auto& [path, _] : path_to_total_number_of_non_null_values) {
            paths.emplace(path);
        }
    }

    return Status::OK();
}

Status VariantColumnWriterImpl::_process_root_column(vectorized::ColumnObject* ptr,
                                                     vectorized::OlapBlockDataConvertor* converter,
                                                     size_t num_rows, int& column_id) {
    // root column
    ColumnWriterOptions root_opts = _opts;
    _root_writer = std::unique_ptr<ColumnWriter>(new ScalarColumnWriter(
            _opts, std::unique_ptr<Field>(FieldFactory::create(*_tablet_column)),
            _opts.file_writer));
    RETURN_IF_ERROR(_root_writer->init());

    // make sure the root type
    auto expected_root_type =
            vectorized::make_nullable(std::make_shared<vectorized::ColumnObject::MostCommonType>());
    ptr->ensure_root_node_type(expected_root_type);

    converter->add_column_data_convertor(*_tablet_column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {ptr->get_root()->get_ptr(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    const uint8_t* nullmap =
            vectorized::check_and_get_column<vectorized::ColumnUInt8>(_null_column.get())
                    ->get_data()
                    .data();
    RETURN_IF_ERROR(_root_writer->append(nullmap, column->get_data(), num_rows));
    ++column_id;
    converter->clear_source_content();

    _opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_subcolumns(vectorized::ColumnObject* ptr,
                                                    vectorized::OlapBlockDataConvertor* converter,
                                                    size_t num_rows, int& column_id) {
    // generate column info by entry info
    auto generate_column_info = [&](const auto& entry) {
        const std::string& column_name =
                _tablet_column->name_lower_case() + "." + entry->path.get_path();
        const vectorized::DataTypePtr& final_data_type_from_object =
                entry->data.get_least_common_type();
        vectorized::PathInDataBuilder full_path_builder;
        auto full_path = full_path_builder.append(_tablet_column->name_lower_case(), false)
                                 .append(entry->path.get_parts(), false)
                                 .build();
        // set unique_id and parent_unique_id, will use unique_id to get iterator correct
        return vectorized::schema_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::schema_util::ExtraInfo {.unique_id = _tablet_column->unique_id(),
                                                    .parent_unique_id = _tablet_column->unique_id(),
                                                    .path_info = full_path});
    };
    // convert sub column data from engine format to storage layer format
    for (const auto& entry :
         vectorized::schema_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        CHECK(entry->data.is_finalized());
        int current_column_id = column_id++;
        TabletColumn tablet_column = generate_column_info(entry);
        RETURN_IF_ERROR(_create_column_writer(current_column_id, tablet_column, *_tablet_column,
                                              _opts.rowset_ctx->tablet_schema));
        converter->add_column_data_convertor(tablet_column);
        RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
                {entry->data.get_finalized_column_ptr()->get_ptr(),
                 entry->data.get_least_common_type(), tablet_column.name()},
                0, num_rows, current_column_id));
        auto [status, column] = converter->convert_column_data(current_column_id);
        if (!status.ok()) {
            return status;
        }
        const uint8_t* nullmap = column->get_nullmap();
        RETURN_IF_ERROR(_subcolumn_writers[current_column_id - 1]->append(
                nullmap, column->get_data(), num_rows));
        converter->clear_source_content();
        _subcolumn_opts[current_column_id - 1].meta->set_num_rows(num_rows);

        // get stastics
        _statistics._subcolumns_non_null_size.emplace(entry->path.get_path(),
                                                      entry->data.get_non_null_value_size());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_sparse_column(
        vectorized::ColumnObject* ptr, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id) {
    // create sparse column writer
    TabletColumn sparse_column =
            vectorized::schema_util::create_sparse_column(_tablet_column->unique_id());
    ColumnWriterOptions sparse_writer_opts;
    sparse_writer_opts.meta = _opts.footer->add_columns();

    _init_column_meta(sparse_writer_opts.meta, column_id, sparse_column);
    RETURN_IF_ERROR(ColumnWriter::create_map_writer(sparse_writer_opts, &sparse_column,
                                                    _opts.file_writer, &_sparse_column_writer));
    RETURN_IF_ERROR(_sparse_column_writer->init());

    // convert root column data from engine format to storage layer format
    converter->add_column_data_convertor(sparse_column);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {ptr->get_sparse_column()->get_ptr(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    RETURN_IF_ERROR(
            _sparse_column_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    ++column_id;
    converter->clear_source_content();

    // get stastics
    // todo: reuse the statics from collected stastics from compaction stage
    std::unordered_map<std::string, size_t> sparse_data_paths_statistics;
    const auto [sparse_data_paths, _] = ptr->get_sparse_data_paths_and_values();
    for (size_t i = 0; i != sparse_data_paths->size(); ++i) {
        auto path = sparse_data_paths->get_data_at(i);
        if (auto it = _statistics._sparse_column_non_null_size.find(path);
            it != _statistics._sparse_column_non_null_size.end()) {
            ++it->second;
        } else if (_statistics._sparse_column_non_null_size.size() <
                   VariantStatistics::MAX_SHARED_DATA_STATISTICS_SIZE) {
            _statistics._sparse_column_non_null_size.emplace(path, 1);
        }
    }

    sparse_writer_opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

void VariantStatistics::to_pb(VariantStatisticsPB* stats) const {
    for (const auto& [path, value] : _sparse_column_non_null_size) {
        stats->mutable_subcolumn_non_null_size()->emplace(path.to_string(), value);
    }
    for (const auto& [path, value] : _sparse_column_non_null_size) {
        stats->mutable_sparse_column_non_null_size()->emplace(path.to_string(), value);
    }
}

void VariantStatistics::from_pb(const VariantStatisticsPB& stats) {
    // make sure the ref of path, todo not use ref
    for (const auto& [path, value] : stats.subcolumn_non_null_size()) {
        _subcolumns_non_null_size[StringRef(path.data(), path.size())] = value;
    }
    for (const auto& [path, value] : stats.sparse_column_non_null_size()) {
        _sparse_column_non_null_size[StringRef(path.data(), path.size())] = value;
    }
}

Status VariantColumnWriterImpl::finalize() {
    auto* ptr = assert_cast<vectorized::ColumnObject*>(_column.get());
    RETURN_IF_ERROR(ptr->finalize(vectorized::ColumnObject::FinalizeMode::WRITE_MODE));

    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());

    if (ptr->is_null_root()) {
        auto root_type = vectorized::make_nullable(
                std::make_shared<vectorized::ColumnObject::MostCommonType>());
        auto root_col = root_type->create_column();
        root_col->insert_many_defaults(ptr->rows());
        ptr->create_root(root_type, std::move(root_col));
    }

    size_t num_rows = _column->size();
    int column_id = 0;

    // convert root column data from engine format to storage layer format
    RETURN_IF_ERROR(_process_root_column(ptr, olap_data_convertor.get(), num_rows, column_id));

    // process and append each subcolumns to sub columns writers buffer
    RETURN_IF_ERROR(_process_subcolumns(ptr, olap_data_convertor.get(), num_rows, column_id));

    // process sparse column and append to sparse writer buffer
    RETURN_IF_ERROR(_process_sparse_column(ptr, olap_data_convertor.get(), num_rows, column_id));

    // set statistics info
    _statistics.to_pb(_opts.meta->mutable_variant_statistics());

    _is_finalized = true;
    return Status::OK();
}

bool VariantColumnWriterImpl::is_finalized() const {
    const auto* ptr = assert_cast<vectorized::ColumnObject*>(_column.get());
    return ptr->is_finalized() && _is_finalized;
}

Status VariantColumnWriterImpl::append_data(const uint8_t** ptr, size_t num_rows) {
    DCHECK(!is_finalized());
    const auto& src = *reinterpret_cast<const vectorized::ColumnObject*>(*ptr);
    auto* dst_ptr = assert_cast<vectorized::ColumnObject*>(_column.get());
    // TODO: if direct write we could avoid copy
    dst_ptr->insert_range_from(src, 0, num_rows);
    return Status::OK();
}

uint64_t VariantColumnWriterImpl::estimate_buffer_size() {
    if (!is_finalized()) {
        // not accurate
        return _column->byte_size();
    }
    uint64_t size = 0;
    for (auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    size += _root_writer->estimate_buffer_size();
    size += _sparse_column_writer->estimate_buffer_size();
    return size;
}

Status VariantColumnWriterImpl::finish() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->finish());
    RETURN_IF_ERROR(_sparse_column_writer->finish());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_data() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->write_data());
    RETURN_IF_ERROR(_sparse_column_writer->write_data());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->write_ordinal_index());
    RETURN_IF_ERROR(_sparse_column_writer->write_ordinal_index());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_zone_map() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_zone_map) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_zone_map());
        }
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::write_bitmap_index() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_bitmap_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_bitmap_index());
        }
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_inverted_index() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    for (int i = 0; i < _subcolumn_writers.size(); ++i) {
        if (_subcolumn_opts[i].need_inverted_index) {
            RETURN_IF_ERROR(_subcolumn_writers[i]->write_inverted_index());
        }
    }
    return Status::OK();
}
Status VariantColumnWriterImpl::write_bloom_filter_index() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
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
        _null_column->insert_many_raw_data((const char*)null_map, num_rows);
    }
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    return Status::OK();
}

void VariantColumnWriterImpl::_init_column_meta(ColumnMetaPB* meta, uint32_t column_id,
                                                const TabletColumn& column) {
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(_opts.compression_type);
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
        _init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i));
    }
};

Status VariantColumnWriterImpl::_create_column_writer(uint32_t cid, const TabletColumn& column,
                                                      const TabletColumn& parent_column,
                                                      const TabletSchemaSPtr& tablet_schema) {
    ColumnWriterOptions opts;
    opts.meta = _opts.footer->add_columns();

    _init_column_meta(opts.meta, cid, column);

    opts.need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opts.need_bloom_filter = parent_column.is_bf_column();
    // const auto* tablet_index = tablet_schema->get_ngram_bf_index(parent_column.unique_id());
    // if (tablet_index) {
    //     opts.need_bloom_filter = true;
    //     opts.is_ngram_bf_index = true;
    //     //narrow convert from int32_t to uint8_t and uint16_t which is dangerous
    //     auto gram_size = tablet_index->get_gram_size();
    //     auto gram_bf_size = tablet_index->get_gram_bf_size();
    //     if (gram_size > 256 || gram_size < 1) {
    //         return Status::NotSupported("Do not support ngram bloom filter for ngram_size: ",
    //                                     gram_size);
    //     }
    //     if (gram_bf_size > 65535 || gram_bf_size < 64) {
    //         return Status::NotSupported("Do not support ngram bloom filter for bf_size: ",
    //                                     gram_bf_size);
    //     }
    //     opts.gram_size = gram_size;
    //     opts.gram_bf_size = gram_bf_size;
    // }

    opts.need_bitmap_index = parent_column.has_bitmap_index();
    bool skip_inverted_index = false;
    if (_opts.rowset_ctx != nullptr) {
        // skip write inverted index for index compaction column
        skip_inverted_index = _opts.rowset_ctx->columns_to_do_index_compaction.contains(
                parent_column.unique_id());
    }
    if (const auto& index = tablet_schema->inverted_index(parent_column);
        index != nullptr && !skip_inverted_index) {
        opts.inverted_index = index;
        opts.need_inverted_index = true;
        DCHECK(_opts.inverted_index_file_writer != nullptr);
        opts.inverted_index_file_writer = _opts.inverted_index_file_writer;
        // TODO support multiple inverted index
    }

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE, type_name)          \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opts.need_zone_map = false;                           \
        opts.need_bloom_filter = false;                       \
        opts.need_bitmap_index = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY, "array")
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB, "jsonb")
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT, "variant")

#undef DISABLE_INDEX_IF_FIELD_TYPE

#undef CHECK_FIELD_TYPE

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _opts.file_writer, &writer));
    RETURN_IF_ERROR(writer->init());
    _subcolumn_writers.push_back(std::move(writer));
    _subcolumn_opts.push_back(opts);

    return Status::OK();
};

} // namespace doris::segment_v2