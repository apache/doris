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

#include <fmt/core.h>
#include <gen_cpp/segment_v2.pb.h>

#include "common/config.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/segment_loader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/columns_number.h"
#include "vec/common/schema_util.h"
#include "vec/json/path_in_data.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris::segment_v2 {

VariantColumnWriterImpl::VariantColumnWriterImpl(const ColumnWriterOptions& opts,
                                                 const TabletColumn* column) {
    _opts = opts;
    _tablet_column = column;
}

Status VariantColumnWriterImpl::init() {
    // caculate stats info
    std::set<std::string> subcolumn_paths;
    RETURN_IF_ERROR(_get_subcolumn_paths_from_stats(subcolumn_paths));
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    auto col = vectorized::ColumnObject::create(_tablet_column->variant_max_subcolumns_count());
    for (const auto& str_path : subcolumn_paths) {
        DCHECK(col->add_sub_column(vectorized::PathInData(str_path), 0));
    }
    _column = std::move(col);
    if (_tablet_column->is_nullable()) {
        _null_column = vectorized::ColumnUInt8::create(0);
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_get_subcolumn_paths_from_stats(std::set<std::string>& paths) {
    std::unordered_map<std::string, size_t> path_to_total_number_of_non_null_values;

    // Merge and collect all stats info from all input rowsets/segments
    for (RowsetReaderSharedPtr reader : _opts.input_rs_readers) {
        SegmentCacheHandle segment_cache;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(reader->rowset()), &segment_cache));
        for (const auto& segment : segment_cache.get_segments()) {
            ColumnReader* column_reader =
                    DORIS_TRY(segment->get_column_reader(_tablet_column->unique_id()));
            if (!column_reader) {
                continue;
            }
            CHECK(column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
            const VariantStatistics* source_statistics =
                    static_cast<const VariantColumnReader*>(column_reader)->get_stats();
            if (!source_statistics) {
                continue;
            }
            for (const auto& [path, size] : source_statistics->subcolumns_non_null_size) {
                auto it = path_to_total_number_of_non_null_values.find(path);
                if (it == path_to_total_number_of_non_null_values.end()) {
                    it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
                }
                it->second += size;
            }
            for (const auto& [path, size] : source_statistics->sparse_column_non_null_size) {
                if (path.empty()) {
                    CHECK(false);
                }
                auto it = path_to_total_number_of_non_null_values.find(path);
                if (it == path_to_total_number_of_non_null_values.end()) {
                    it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
                }
                it->second += size;
            }
        }
    }

    // Check if the number of all subcolumn paths exceeds the limit.
    DCHECK(_tablet_column->variant_max_subcolumns_count() >= 0)
            << "max subcolumns count is: " << _tablet_column->variant_max_subcolumns_count();
    if (_tablet_column->variant_max_subcolumns_count() &&
        path_to_total_number_of_non_null_values.size() >
                _tablet_column->variant_max_subcolumns_count()) {
        // Sort paths by total number of non null values.
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(path_to_total_number_of_non_null_values.size());
        for (const auto& [path, size] : path_to_total_number_of_non_null_values) {
            paths_with_sizes.emplace_back(size, path);
        }
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());
        // Fill subcolumn_paths with first subcolumn paths in sorted list.
        // reserve 1 for root column
        for (const auto& [size, path] : paths_with_sizes) {
            if (paths.size() < _tablet_column->variant_max_subcolumns_count()) {
                VLOG_DEBUG << "pick " << path << " as subcolumn";
                paths.emplace(path);
            }
            // // todo : Add all remaining paths into shared data statistics until we reach its max size;
            // else if (new_statistics.sparse_data_paths_statistics.size() < Statistics::MAX_SPARSE_DATA_STATISTICS_SIZE) {
            //     new_statistics.sparse_data_paths_statistics.emplace(path, size);
            // }
        }
        DBUG_EXECUTE_IF("variant_column_writer_impl._get_subcolumn_paths_from_stats", {
            auto stats = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "variant_column_writer_impl._get_subcolumn_paths_from_stats", "stats", "");
            auto subcolumns = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "variant_column_writer_impl._get_subcolumn_paths_from_stats", "subcolumns", "");
            LOG(INFO) << "stats: " << stats;
            LOG(INFO) << "subcolumns: " << subcolumns;
            if (stats.empty()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("debug point stats is empty");
            }
            std::vector<std::string> sizes;
            boost::split(sizes, stats, boost::algorithm::is_any_of(","));
            CHECK_EQ(sizes.size(), paths_with_sizes.size()) << "stats not match " << stats;
            for (int i = 0; i < sizes.size(); ++i) {
                CHECK_EQ(fmt::format("{}", paths_with_sizes[i].first), sizes[i]);
            }
            std::set<std::string> subcolumns_set;
            boost::split(subcolumns_set, subcolumns, boost::algorithm::is_any_of(","));
            if (!std::equal(paths.begin(), paths.end(), subcolumns_set.begin(),
                            subcolumns_set.end())) {
                CHECK(false) << "subcolumns not match " << subcolumns;
            }
        })
    } else {
        // Use all subcolumn paths from all source columns.
        for (const auto& [path, _] : path_to_total_number_of_non_null_values) {
            VLOG_DEBUG << "pick " << path << " as subcolumn";
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
    DCHECK_EQ(ptr->get_root()->get_ptr()->size(), num_rows);
    RETURN_IF_ERROR(converter->set_source_content_with_specifid_column(
            {ptr->get_root()->get_ptr(), nullptr, ""}, 0, num_rows, column_id));
    auto [status, column] = converter->convert_column_data(column_id);
    if (!status.ok()) {
        return status;
    }
    const uint8_t* nullmap =
            _null_column
                    ? vectorized::check_and_get_column<vectorized::ColumnUInt8>(_null_column.get())
                              ->get_data()
                              .data()
                    : nullptr;
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
        // set unique_id and parent_unique_id, will use parent_unique_id to get iterator correct
        return vectorized::schema_util::get_column_by_type(
                final_data_type_from_object, column_name,
                vectorized::schema_util::ExtraInfo {.unique_id = _tablet_column->unique_id(),
                                                    .parent_unique_id = _tablet_column->unique_id(),
                                                    .path_info = full_path});
    };
    // convert sub column data from engine format to storage layer format
    for (const auto& entry :
         vectorized::schema_util::get_sorted_subcolumns(ptr->get_subcolumns())) {
        const auto& least_common_type = entry->data.get_least_common_type();
        if (is_nothing(remove_nullable(
                    vectorized::schema_util::get_base_type_of_array(least_common_type)))) {
            continue;
        }
        if (entry->path.empty()) {
            // already handled
            continue;
        }
        CHECK(entry->data.is_finalized());
        int current_column_id = column_id++;
        TabletColumn tablet_column = generate_column_info(entry);
        vectorized::schema_util::inherit_column_attributes(*_tablet_column, tablet_column);
        RETURN_IF_ERROR(_create_column_writer(current_column_id, tablet_column,
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
        _statistics.subcolumns_non_null_size.emplace(entry->path.get_path(),
                                                     entry->data.get_non_null_value_size());
    }
    return Status::OK();
}

Status VariantColumnWriterImpl::_process_sparse_column(
        vectorized::ColumnObject* ptr, vectorized::OlapBlockDataConvertor* converter,
        size_t num_rows, int& column_id) {
    // create sparse column writer
    TabletColumn sparse_column = vectorized::schema_util::create_sparse_column(*_tablet_column);
    ColumnWriterOptions sparse_writer_opts;
    sparse_writer_opts.meta = _opts.footer->add_columns();

    _init_column_meta(sparse_writer_opts.meta, column_id, sparse_column);
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
    VLOG_DEBUG << "dump sparse "
               << vectorized::schema_util::dump_column(
                          vectorized::ColumnObject::get_sparse_column_type(),
                          ptr->get_sparse_column());
    RETURN_IF_ERROR(
            _sparse_column_writer->append(column->get_nullmap(), column->get_data(), num_rows));
    ++column_id;
    converter->clear_source_content();

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
                   VariantStatistics::MAX_SPARSE_DATA_STATISTICS_SIZE) {
            sparse_data_paths_statistics.emplace(path, 1);
        }
    }

    // assign to _statistics.sparse_column_non_null_size
    for (const auto& [path, size] : sparse_data_paths_statistics) {
        _statistics.sparse_column_non_null_size.emplace(path.to_string(), size);
    }
    sparse_writer_opts.meta->set_num_rows(num_rows);
    return Status::OK();
}

void VariantStatistics::to_pb(VariantStatisticsPB* stats) const {
    for (const auto& [path, value] : subcolumns_non_null_size) {
        stats->mutable_subcolumn_non_null_size()->emplace(path, value);
    }
    for (const auto& [path, value] : sparse_column_non_null_size) {
        stats->mutable_sparse_column_non_null_size()->emplace(path, value);
    }
    LOG(INFO) << "num subcolumns " << subcolumns_non_null_size.size() << ", num sparse columns "
              << sparse_column_non_null_size.size();
}

void VariantStatistics::from_pb(const VariantStatisticsPB& stats) {
    // make sure the ref of path, todo not use ref
    for (const auto& [path, value] : stats.subcolumn_non_null_size()) {
        subcolumns_non_null_size[path] = value;
    }
    for (const auto& [path, value] : stats.sparse_column_non_null_size()) {
        sparse_column_non_null_size[path] = value;
    }
}

Status VariantColumnWriterImpl::finalize() {
    auto* ptr = assert_cast<vectorized::ColumnObject*>(_column.get());
    RETURN_IF_ERROR(ptr->finalize(vectorized::ColumnObject::FinalizeMode::WRITE_MODE));

    // convert each subcolumns to storage format and add data to sub columns writers buffer
    auto olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();

    DCHECK(ptr->is_finalized());

    // if (ptr->is_null_root()) {
    //     CHECK(false);
    //     // auto root_type = vectorized::make_nullable(
    //     //         std::make_shared<vectorized::ColumnObject::MostCommonType>());
    //     // auto root_col = root_type->create_column();
    //     // root_col->insert_many_defaults(ptr->rows());
    //     // ptr->create_root(root_type, std::move(root_col));
    // }

#ifndef NDEBUG
    ptr->check_consistency();
#endif

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
    size += _root_writer->estimate_buffer_size();
    for (auto& column_writer : _subcolumn_writers) {
        size += column_writer->estimate_buffer_size();
    }
    size += _sparse_column_writer->estimate_buffer_size();
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
    RETURN_IF_ERROR(_sparse_column_writer->finish());
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
    RETURN_IF_ERROR(_sparse_column_writer->write_data());
    return Status::OK();
}
Status VariantColumnWriterImpl::write_ordinal_index() {
    if (!is_finalized()) {
        RETURN_IF_ERROR(finalize());
    }
    RETURN_IF_ERROR(_root_writer->write_ordinal_index());
    for (auto& column_writer : _subcolumn_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    RETURN_IF_ERROR(_sparse_column_writer->write_ordinal_index());
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
                                                      const TabletSchemaSPtr& tablet_schema) {
    ColumnWriterOptions opts;
    opts.meta = _opts.footer->add_columns();

    _init_column_meta(opts.meta, cid, column);

    opts.need_zone_map = tablet_schema->keys_type() != KeysType::AGG_KEYS;
    opts.need_bloom_filter = column.is_bf_column();

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

    opts.need_bitmap_index = column.has_bitmap_index();
    const auto& index = tablet_schema->inverted_index(column.parent_unique_id());
    if (index != nullptr &&
        segment_v2::InvertedIndexColumnWriter::check_support_inverted_index(column)) {
        auto subcolumn_index = std::make_unique<TabletIndex>(*index);
        subcolumn_index->set_escaped_escaped_index_suffix_path(column.path_info_ptr()->get_path());
        opts.inverted_index = subcolumn_index.get();
        opts.need_inverted_index = true;
        DCHECK(_opts.inverted_index_file_writer != nullptr);
        opts.inverted_index_file_writer = _opts.inverted_index_file_writer;
        _subcolumns_indexes.emplace_back(std::move(subcolumn_index));
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