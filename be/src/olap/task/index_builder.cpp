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

#include "olap/task/index_builder.h"

#include "common/status.h"
#include "gutil/integral_types.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"
#include "util/trace.h"

namespace doris {

IndexBuilder::IndexBuilder(StorageEngine& engine, TabletSharedPtr tablet,
                           const std::vector<TColumn>& columns,
                           const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                           bool is_drop_op)
        : _engine(engine),
          _tablet(std::move(tablet)),
          _columns(columns),
          _alter_inverted_indexes(alter_inverted_indexes),
          _is_drop_op(is_drop_op) {
    _olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
}

IndexBuilder::~IndexBuilder() {
    _olap_data_convertor.reset();
    _inverted_index_builders.clear();
}

Status IndexBuilder::init() {
    for (auto inverted_index : _alter_inverted_indexes) {
        _alter_index_ids.insert(inverted_index.index_id);
    }
    return Status::OK();
}

Status IndexBuilder::update_inverted_index_info() {
    // just do link files
    LOG(INFO) << "begin to update_inverted_index_info, tablet=" << _tablet->tablet_id()
              << ", is_drop_op=" << _is_drop_op;
    // index ids that will not be linked
    std::set<int64_t> without_index_uids;
    _output_rowsets.reserve(_input_rowsets.size());
    _pending_rs_guards.reserve(_input_rowsets.size());
    for (auto&& input_rowset : _input_rowsets) {
        if (!input_rowset->is_local()) [[unlikely]] {
            DCHECK(false) << _tablet->tablet_id() << ' ' << input_rowset->rowset_id();
            return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                         _tablet->tablet_id(),
                                         input_rowset->rowset_id().to_string());
        }

        TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
        const auto& input_rs_tablet_schema = input_rowset->tablet_schema();
        output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);
        size_t total_index_size = 0;
        auto* beta_rowset = reinterpret_cast<BetaRowset*>(input_rowset.get());
        auto size_st = beta_rowset->get_inverted_index_size(&total_index_size);
        if (!size_st.ok() && !size_st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() &&
            !size_st.is<ErrorCode::NOT_FOUND>()) {
            return size_st;
        }
        auto num_segments = input_rowset->num_segments();
        size_t drop_index_size = 0;

        if (_is_drop_op) {
            for (const auto& t_inverted_index : _alter_inverted_indexes) {
                DCHECK_EQ(t_inverted_index.columns.size(), 1);
                auto column_name = t_inverted_index.columns[0];
                auto column_idx = output_rs_tablet_schema->field_index(column_name);
                if (column_idx < 0) {
                    LOG(WARNING) << "referenced column was missing. "
                                 << "[column=" << column_name << " referenced_column=" << column_idx
                                 << "]";
                    continue;
                }
                auto column = output_rs_tablet_schema->column(column_idx);
                const auto* index_meta = output_rs_tablet_schema->get_inverted_index(column);
                if (index_meta == nullptr) {
                    LOG(ERROR) << "failed to find column: " << column_name
                               << " index_id: " << t_inverted_index.index_id;
                    continue;
                }
                if (output_rs_tablet_schema->get_inverted_index_storage_format() ==
                    InvertedIndexStorageFormatPB::V1) {
                    const auto& fs = io::global_local_filesystem();

                    for (int seg_id = 0; seg_id < num_segments; seg_id++) {
                        auto seg_path =
                                local_segment_path(_tablet->tablet_path(),
                                                   input_rowset->rowset_id().to_string(), seg_id);
                        auto index_path = InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                index_meta->index_id(), index_meta->get_index_suffix());
                        int64_t index_size = 0;
                        RETURN_IF_ERROR(fs->file_size(index_path, &index_size));
                        VLOG_DEBUG << "inverted index file:" << index_path
                                   << " size:" << index_size;
                        drop_index_size += index_size;
                    }
                }
                _dropped_inverted_indexes.push_back(*index_meta);
                // ATTN: DO NOT REMOVE INDEX AFTER OUTPUT_ROWSET_WRITER CREATED.
                // remove dropped index_meta from output rowset tablet schema
                output_rs_tablet_schema->remove_index(index_meta->index_id());
            }
            DBUG_EXECUTE_IF("index_builder.update_inverted_index_info.drop_index", {
                auto indexes_count = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                        "index_builder.update_inverted_index_info.drop_index", "indexes_count", 0);
                if (indexes_count < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "indexes count cannot be negative");
                }
                int32_t indexes_size = 0;
                for (auto index : output_rs_tablet_schema->indexes()) {
                    if (index.index_type() == IndexType::INVERTED) {
                        indexes_size++;
                    }
                }
                if (indexes_count != indexes_size) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "indexes count not equal to expected");
                }
            })
        } else {
            // base on input rowset's tablet_schema to build
            // output rowset's tablet_schema which only add
            // the indexes specified in this build index request
            for (auto t_inverted_index : _alter_inverted_indexes) {
                TabletIndex index;
                index.init_from_thrift(t_inverted_index, *input_rs_tablet_schema);
                auto column_uid = index.col_unique_ids()[0];
                if (column_uid < 0) {
                    LOG(WARNING) << "referenced column was missing. "
                                 << "[column=" << t_inverted_index.columns[0]
                                 << " referenced_column=" << column_uid << "]";
                    output_rs_tablet_schema->append_index(index);
                    continue;
                }
                const TabletColumn& col = output_rs_tablet_schema->column_by_uid(column_uid);
                const TabletIndex* exist_index = output_rs_tablet_schema->get_inverted_index(col);
                if (exist_index && exist_index->index_id() != index.index_id()) {
                    LOG(WARNING) << fmt::format(
                            "column: {} has a exist inverted index, but the index id not equal "
                            "request's index id, , exist index id: {}, request's index id: {}, "
                            "remove exist index in new output_rs_tablet_schema",
                            column_uid, exist_index->index_id(), index.index_id());
                    without_index_uids.insert(exist_index->index_id());
                    output_rs_tablet_schema->remove_index(exist_index->index_id());
                }
                output_rs_tablet_schema->append_index(index);
            }
        }
        // construct input rowset reader
        RowsetReaderSharedPtr input_rs_reader;
        RETURN_IF_ERROR(input_rowset->create_reader(&input_rs_reader));
        // construct output rowset writer
        RowsetWriterContext context;
        context.version = input_rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = input_rowset->rowset_meta()->segments_overlap();
        context.tablet_schema = output_rs_tablet_schema;
        context.newest_write_timestamp = input_rs_reader->newest_write_timestamp();
        auto output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(context, false));
        _pending_rs_guards.push_back(_engine.add_pending_rowset(context));

        // if without_index_uids is not empty, copy _alter_index_ids to it
        // else just use _alter_index_ids to avoid copy
        if (!without_index_uids.empty()) {
            without_index_uids.insert(_alter_index_ids.begin(), _alter_index_ids.end());
        }

        // build output rowset
        RETURN_IF_ERROR(input_rowset->link_files_to(
                _tablet->tablet_path(), output_rs_writer->rowset_id(), 0,
                without_index_uids.empty() ? &_alter_index_ids : &without_index_uids));

        auto input_rowset_meta = input_rowset->rowset_meta();
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_num_rows(input_rowset_meta->num_rows());
        if (output_rs_tablet_schema->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::V1) {
            if (_is_drop_op) {
                VLOG_DEBUG << "data_disk_size:" << input_rowset_meta->data_disk_size()
                           << " total_disk_size:" << input_rowset_meta->data_disk_size()
                           << " index_disk_size:" << input_rowset_meta->index_disk_size()
                           << " drop_index_size:" << drop_index_size;
                rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size() -
                                                 drop_index_size);
                rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size() -
                                                drop_index_size);
                rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size() -
                                                 drop_index_size);
            } else {
                rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size());
                rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
                rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size());
            }
        } else {
            for (int seg_id = 0; seg_id < num_segments; seg_id++) {
                auto seg_path = DORIS_TRY(input_rowset->segment_path(seg_id));
                auto idx_file_reader = std::make_unique<InvertedIndexFileReader>(
                        context.fs(),
                        std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
                        output_rs_tablet_schema->get_inverted_index_storage_format());
                auto st = idx_file_reader->init();
                if (!st.ok() && !st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
                    return st;
                }
                _inverted_index_file_readers.emplace(
                        std::make_pair(output_rs_writer->rowset_id().to_string(), seg_id),
                        std::move(idx_file_reader));
            }
            rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size() -
                                             total_index_size);
            rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size() - total_index_size);
            rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size() -
                                             total_index_size);
        }
        rowset_meta->set_empty(input_rowset_meta->empty());
        rowset_meta->set_num_segments(input_rowset_meta->num_segments());
        rowset_meta->set_segments_overlap(input_rowset_meta->segments_overlap());
        rowset_meta->set_rowset_state(input_rowset_meta->rowset_state());
        std::vector<KeyBoundsPB> key_bounds;
        RETURN_IF_ERROR(input_rowset->get_segments_key_bounds(&key_bounds));
        rowset_meta->set_segments_key_bounds(key_bounds);
        auto output_rowset = output_rs_writer->manual_build(rowset_meta);
        if (input_rowset_meta->has_delete_predicate()) {
            output_rowset->rowset_meta()->set_delete_predicate(
                    input_rowset_meta->delete_predicate());
        }
        _output_rowsets.push_back(output_rowset);
    }

    return Status::OK();
}

Status IndexBuilder::handle_single_rowset(RowsetMetaSharedPtr output_rowset_meta,
                                          std::vector<segment_v2::SegmentSharedPtr>& segments) {
    if (!output_rowset_meta->is_local()) [[unlikely]] {
        DCHECK(false) << _tablet->tablet_id() << ' ' << output_rowset_meta->rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _tablet->tablet_id(),
                                     output_rowset_meta->rowset_id().to_string());
    }

    if (_is_drop_op) {
        const auto& output_rs_tablet_schema = output_rowset_meta->tablet_schema();
        if (output_rs_tablet_schema->get_inverted_index_storage_format() !=
            InvertedIndexStorageFormatPB::V1) {
            const auto& fs = output_rowset_meta->fs();

            const auto& output_rowset_schema = output_rowset_meta->tablet_schema();
            size_t inverted_index_size = 0;
            for (auto& seg_ptr : segments) {
                auto idx_file_reader_iter = _inverted_index_file_readers.find(
                        std::make_pair(output_rowset_meta->rowset_id().to_string(), seg_ptr->id()));
                if (idx_file_reader_iter == _inverted_index_file_readers.end()) {
                    LOG(ERROR) << "idx_file_reader_iter" << output_rowset_meta->rowset_id() << ":"
                               << seg_ptr->id() << " cannot be found";
                    continue;
                }
                auto dirs = DORIS_TRY(idx_file_reader_iter->second->get_all_directories());

                std::string index_path_prefix {
                        InvertedIndexDescriptor::get_index_file_path_prefix(local_segment_path(
                                _tablet->tablet_path(), output_rowset_meta->rowset_id().to_string(),
                                seg_ptr->id()))};

                auto inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                        fs, std::move(index_path_prefix),
                        output_rowset_meta->rowset_id().to_string(), seg_ptr->id(),
                        output_rowset_schema->get_inverted_index_storage_format());
                RETURN_IF_ERROR(inverted_index_file_writer->initialize(dirs));
                // create inverted index writer
                for (auto& index_meta : _dropped_inverted_indexes) {
                    RETURN_IF_ERROR(inverted_index_file_writer->delete_index(&index_meta));
                }
                _inverted_index_file_writers.emplace(seg_ptr->id(),
                                                     std::move(inverted_index_file_writer));
            }
            for (auto&& [seg_id, inverted_index_writer] : _inverted_index_file_writers) {
                auto st = inverted_index_writer->close();
                if (!st.ok()) {
                    LOG(ERROR) << "close inverted_index_writer error:" << st;
                    return st;
                }
                inverted_index_size += inverted_index_writer->get_index_file_total_size();
            }
            _inverted_index_file_writers.clear();
            output_rowset_meta->set_data_disk_size(output_rowset_meta->data_disk_size() +
                                                   inverted_index_size);
            output_rowset_meta->set_total_disk_size(output_rowset_meta->total_disk_size() +
                                                    inverted_index_size);
            output_rowset_meta->set_index_disk_size(output_rowset_meta->index_disk_size() +
                                                    inverted_index_size);
        }
        LOG(INFO) << "all row nums. source_rows=" << output_rowset_meta->num_rows();
        return Status::OK();
    } else {
        // create inverted index writer
        const auto& fs = io::global_local_filesystem();
        auto output_rowset_schema = output_rowset_meta->tablet_schema();
        size_t inverted_index_size = 0;
        for (auto& seg_ptr : segments) {
            std::string index_path_prefix {
                    InvertedIndexDescriptor::get_index_file_path_prefix(local_segment_path(
                            _tablet->tablet_path(), output_rowset_meta->rowset_id().to_string(),
                            seg_ptr->id()))};
            std::vector<ColumnId> return_columns;
            std::vector<std::pair<int64_t, int64_t>> inverted_index_writer_signs;
            _olap_data_convertor->reserve(_alter_inverted_indexes.size());

            std::unique_ptr<InvertedIndexFileWriter> inverted_index_file_writer = nullptr;
            if (output_rowset_schema->get_inverted_index_storage_format() >=
                InvertedIndexStorageFormatPB::V2) {
                auto idx_file_reader_iter = _inverted_index_file_readers.find(
                        std::make_pair(output_rowset_meta->rowset_id().to_string(), seg_ptr->id()));
                if (idx_file_reader_iter == _inverted_index_file_readers.end()) {
                    LOG(ERROR) << "idx_file_reader_iter" << output_rowset_meta->rowset_id() << ":"
                               << seg_ptr->id() << " cannot be found";
                    continue;
                }
                auto dirs = DORIS_TRY(idx_file_reader_iter->second->get_all_directories());
                inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                        fs, index_path_prefix, output_rowset_meta->rowset_id().to_string(),
                        seg_ptr->id(), output_rowset_schema->get_inverted_index_storage_format());
                RETURN_IF_ERROR(inverted_index_file_writer->initialize(dirs));
            } else {
                inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                        fs, index_path_prefix, output_rowset_meta->rowset_id().to_string(),
                        seg_ptr->id(), output_rowset_schema->get_inverted_index_storage_format());
            }
            // create inverted index writer
            for (auto inverted_index : _alter_inverted_indexes) {
                DCHECK_EQ(inverted_index.columns.size(), 1);
                auto index_id = inverted_index.index_id;
                auto column_name = inverted_index.columns[0];
                auto column_idx = output_rowset_schema->field_index(column_name);
                if (column_idx < 0) {
                    LOG(WARNING) << "referenced column was missing. "
                                 << "[column=" << column_name << " referenced_column=" << column_idx
                                 << "]";
                    continue;
                }
                auto column = output_rowset_schema->column(column_idx);
                if (!InvertedIndexColumnWriter::check_support_inverted_index(column)) {
                    continue;
                }
                DCHECK(output_rowset_schema->has_inverted_index_with_index_id(index_id, ""));
                _olap_data_convertor->add_column_data_convertor(column);
                return_columns.emplace_back(column_idx);
                std::unique_ptr<Field> field(FieldFactory::create(column));
                const auto* index_meta = output_rowset_schema->get_inverted_index(column);
                std::unique_ptr<segment_v2::InvertedIndexColumnWriter> inverted_index_builder;
                try {
                    RETURN_IF_ERROR(segment_v2::InvertedIndexColumnWriter::create(
                            field.get(), &inverted_index_builder, inverted_index_file_writer.get(),
                            index_meta));
                } catch (const std::exception& e) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occured: {}", e.what());
                }

                if (inverted_index_builder) {
                    auto writer_sign = std::make_pair(seg_ptr->id(), index_id);
                    _inverted_index_builders.insert(
                            std::make_pair(writer_sign, std::move(inverted_index_builder)));
                    inverted_index_writer_signs.emplace_back(writer_sign);
                }
            }

            if (return_columns.empty()) {
                // no columns to read
                break;
            }

            _inverted_index_file_writers.emplace(seg_ptr->id(),
                                                 std::move(inverted_index_file_writer));

            // create iterator for each segment
            StorageReadOptions read_options;
            OlapReaderStatistics stats;
            read_options.stats = &stats;
            read_options.tablet_schema = output_rowset_schema;
            std::shared_ptr<Schema> schema =
                    std::make_shared<Schema>(output_rowset_schema->columns(), return_columns);
            std::unique_ptr<RowwiseIterator> iter;
            auto res = seg_ptr->new_iterator(schema, read_options, &iter);
            if (!res.ok()) {
                LOG(WARNING) << "failed to create iterator[" << seg_ptr->id()
                             << "]: " << res.to_string();
                return Status::Error<ErrorCode::ROWSET_READER_INIT>(res.to_string());
            }

            auto block = vectorized::Block::create_unique(
                    output_rowset_schema->create_block(return_columns));
            while (true) {
                auto status = iter->next_batch(block.get());
                DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset", {
                    status = Status::Error<ErrorCode::SCHEMA_CHANGE_INFO_INVALID>(
                            "next_batch fault injection");
                });
                if (!status.ok()) {
                    if (status.is<ErrorCode::END_OF_FILE>()) {
                        break;
                    }
                    LOG(WARNING)
                            << "failed to read next block when schema change for inverted index."
                            << ", err=" << status.to_string();
                    return status;
                }

                // write inverted index data
                if (_write_inverted_index_data(output_rowset_schema, iter->data_id(),
                                               block.get()) != Status::OK()) {
                    return Status::Error<ErrorCode::SCHEMA_CHANGE_INFO_INVALID>(
                            "failed to write block.");
                }
                block->clear_column_data();
            }

            // finish write inverted index, flush data to compound file
            for (auto& writer_sign : inverted_index_writer_signs) {
                try {
                    if (_inverted_index_builders[writer_sign]) {
                        RETURN_IF_ERROR(_inverted_index_builders[writer_sign]->finish());
                    }
                } catch (const std::exception& e) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occured: {}", e.what());
                }
            }

            _olap_data_convertor->reset();
        }
        for (auto&& [seg_id, inverted_index_file_writer] : _inverted_index_file_writers) {
            auto st = inverted_index_file_writer->close();
            if (!st.ok()) {
                LOG(ERROR) << "close inverted_index_writer error:" << st;
                return st;
            }
            inverted_index_size += inverted_index_file_writer->get_index_file_total_size();
        }
        _inverted_index_builders.clear();
        _inverted_index_file_writers.clear();
        output_rowset_meta->set_data_disk_size(output_rowset_meta->data_disk_size() +
                                               inverted_index_size);
        output_rowset_meta->set_total_disk_size(output_rowset_meta->total_disk_size() +
                                                inverted_index_size);
        output_rowset_meta->set_index_disk_size(output_rowset_meta->index_disk_size() +
                                                inverted_index_size);
        LOG(INFO) << "all row nums. source_rows=" << output_rowset_meta->num_rows();
    }

    return Status::OK();
}

Status IndexBuilder::_write_inverted_index_data(TabletSchemaSPtr tablet_schema, int32_t segment_idx,
                                                vectorized::Block* block) {
    VLOG_DEBUG << "begin to write inverted index";
    // converter block data
    _olap_data_convertor->set_source_content(block, 0, block->rows());
    for (auto i = 0; i < _alter_inverted_indexes.size(); ++i) {
        auto inverted_index = _alter_inverted_indexes[i];
        auto index_id = inverted_index.index_id;
        auto column_name = inverted_index.columns[0];
        auto column_idx = tablet_schema->field_index(column_name);
        if (column_idx < 0) {
            LOG(WARNING) << "referenced column was missing. "
                         << "[column=" << column_name << " referenced_column=" << column_idx << "]";
            continue;
        }
        auto column = tablet_schema->column(column_idx);
        auto writer_sign = std::make_pair(segment_idx, index_id);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        auto converted_result = _olap_data_convertor->convert_column_data(i);
        if (converted_result.first != Status::OK()) {
            LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }
        const auto* ptr = (const uint8_t*)converted_result.second->get_data();
        if (converted_result.second->get_nullmap()) {
            RETURN_IF_ERROR(_add_nullable(column_name, writer_sign, field.get(),
                                          converted_result.second->get_nullmap(), &ptr,
                                          block->rows()));
        } else {
            RETURN_IF_ERROR(_add_data(column_name, writer_sign, field.get(), &ptr, block->rows()));
        }
    }
    _olap_data_convertor->clear_source_content();

    return Status::OK();
}

Status IndexBuilder::_add_nullable(const std::string& column_name,
                                   const std::pair<int64_t, int64_t>& index_writer_sign,
                                   Field* field, const uint8_t* null_map, const uint8_t** ptr,
                                   size_t num_rows) {
    size_t offset = 0;
    auto next_run_step = [&]() {
        size_t step = 1;
        for (auto i = offset + 1; i < num_rows; ++i) {
            if (null_map[offset] == null_map[i]) {
                step++;
            } else {
                break;
            }
        }
        return step;
    };
    // TODO: need to process null data for inverted index
    if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(field->get_sub_field_count() == 1);
        // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
        // total number length
        auto element_cnt = size_t((unsigned long)(*data_ptr));
        auto offset_data = *(data_ptr + 1);
        const auto* offsets_ptr = (const uint8_t*)offset_data;
        try {
            if (element_cnt > 0) {
                auto data = *(data_ptr + 2);
                auto nested_null_map = *(data_ptr + 3);
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                        field->get_sub_field(0)->size(), reinterpret_cast<const void*>(data),
                        reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, num_rows));
            }
        } catch (const std::exception& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occured: {}", e.what());
        }
        return Status::OK();
    }

    try {
        do {
            auto step = next_run_step();
            if (null_map[offset]) {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_nulls(step));
            } else {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(
                        column_name, *ptr, step));
            }
            *ptr += field->size() * step;
            offset += step;
        } while (offset < num_rows);
    } catch (const std::exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    }

    return Status::OK();
}

Status IndexBuilder::_add_data(const std::string& column_name,
                               const std::pair<int64_t, int64_t>& index_writer_sign, Field* field,
                               const uint8_t** ptr, size_t num_rows) {
    try {
        if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            DCHECK(field->get_sub_field_count() == 1);
            // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
            const auto* data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
            // total number length
            auto element_cnt = size_t((unsigned long)(*data_ptr));
            auto offset_data = *(data_ptr + 1);
            const auto* offsets_ptr = (const uint8_t*)offset_data;
            if (element_cnt > 0) {
                auto data = *(data_ptr + 2);
                auto nested_null_map = *(data_ptr + 3);
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                        field->get_sub_field(0)->size(), reinterpret_cast<const void*>(data),
                        reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, num_rows));
            }
        } else {
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(
                    column_name, *ptr, num_rows));
        }
    } catch (const std::exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occured: {}",
                                                                      e.what());
    }

    return Status::OK();
}

Status IndexBuilder::handle_inverted_index_data() {
    LOG(INFO) << "begin to handle_inverted_index_data";
    DCHECK(_input_rowsets.size() == _output_rowsets.size());
    for (auto& _output_rowset : _output_rowsets) {
        SegmentCacheHandle segment_cache_handle;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(_output_rowset), &segment_cache_handle));
        auto output_rowset_meta = _output_rowset->rowset_meta();
        auto& segments = segment_cache_handle.get_segments();
        RETURN_IF_ERROR(handle_single_rowset(output_rowset_meta, segments));
    }
    return Status::OK();
}

Status IndexBuilder::do_build_inverted_index() {
    LOG(INFO) << "begin to do_build_inverted_index, tablet=" << _tablet->tablet_id()
              << ", is_drop_op=" << _is_drop_op;
    if (_alter_inverted_indexes.empty()) {
        return Status::OK();
    }

    std::unique_lock<std::mutex> schema_change_lock(_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>("try schema_change_lock failed");
    }
    // Check executing serially with compaction task.
    std::unique_lock<std::mutex> base_compaction_lock(_tablet->get_base_compaction_lock(),
                                                      std::try_to_lock);
    if (!base_compaction_lock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>("try base_compaction_lock failed");
    }
    std::unique_lock<std::mutex> cumu_compaction_lock(_tablet->get_cumulative_compaction_lock(),
                                                      std::try_to_lock);
    if (!cumu_compaction_lock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>("try cumu_compaction_lock failed");
    }

    std::unique_lock<std::mutex> cold_compaction_lock(_tablet->get_cold_compaction_lock(),
                                                      std::try_to_lock);
    if (!cold_compaction_lock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>("try cold_compaction_lock failed");
    }

    std::unique_lock<std::mutex> build_inverted_index_lock(_tablet->get_build_inverted_index_lock(),
                                                           std::try_to_lock);
    if (!build_inverted_index_lock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>(
                "failed to obtain build inverted index lock. tablet={}", _tablet->tablet_id());
    }

    std::shared_lock migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
    if (!migration_rlock.owns_lock()) {
        return Status::Error<ErrorCode::TRY_LOCK_FAILED>("got migration_rlock failed. tablet={}",
                                                         _tablet->tablet_id());
    }

    _input_rowsets =
            _tablet->pick_candidate_rowsets_to_build_inverted_index(_alter_index_ids, _is_drop_op);
    if (_input_rowsets.empty()) {
        LOG(INFO) << "_input_rowsets is empty";
        return Status::OK();
    }

    auto st = update_inverted_index_info();
    if (!st.ok()) {
        LOG(WARNING) << "failed to update_inverted_index_info. "
                     << "tablet=" << _tablet->tablet_id() << ", error=" << st;
        gc_output_rowset();
        return st;
    }

    // create inverted index file for output rowset
    st = handle_inverted_index_data();
    if (!st.ok()) {
        LOG(WARNING) << "failed to handle_inverted_index_data. "
                     << "tablet=" << _tablet->tablet_id() << ", error=" << st;
        gc_output_rowset();
        return st;
    }

    // modify rowsets in memory
    st = modify_rowsets();
    if (!st.ok()) {
        LOG(WARNING) << "failed to modify rowsets in memory. "
                     << "tablet=" << _tablet->tablet_id() << ", error=" << st;
        gc_output_rowset();
        return st;
    }
    return Status::OK();
}

Status IndexBuilder::modify_rowsets(const Merger::Statistics* stats) {
    DCHECK(std::ranges::all_of(
            _output_rowsets.begin(), _output_rowsets.end(), [&engine = _engine](auto&& rs) {
                if (engine.check_rowset_id_in_unused_rowsets(rs->rowset_id())) {
                    LOG(ERROR) << "output rowset: " << rs->rowset_id() << " in unused rowsets";
                    return false;
                }
                return true;
            }));

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::mutex> rowset_update_wlock(_tablet->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> meta_wlock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(_tablet->tablet_id());
        for (auto i = 0; i < _input_rowsets.size(); ++i) {
            RowsetId input_rowset_id = _input_rowsets[i]->rowset_id();
            RowsetId output_rowset_id = _output_rowsets[i]->rowset_id();
            for (const auto& [k, v] : _tablet->tablet_meta()->delete_bitmap().delete_bitmap) {
                RowsetId rs_id = std::get<0>(k);
                if (rs_id == input_rowset_id) {
                    DeleteBitmap::BitmapKey output_rs_key = {output_rowset_id, std::get<1>(k),
                                                             std::get<2>(k)};
                    auto res = delete_bitmap->set(output_rs_key, v);
                    DCHECK(res > 0) << "delete_bitmap set failed, res=" << res;
                }
            }
        }
        _tablet->tablet_meta()->delete_bitmap().merge(*delete_bitmap);

        // modify_rowsets will remove the delete_bitmap for input rowsets,
        // should call it after merge delete_bitmap
        RETURN_IF_ERROR(_tablet->modify_rowsets(_output_rowsets, _input_rowsets, true));
    } else {
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
        RETURN_IF_ERROR(_tablet->modify_rowsets(_output_rowsets, _input_rowsets, true));
    }

    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _tablet->save_meta();
    }
    return Status::OK();
}

void IndexBuilder::gc_output_rowset() {
    for (auto&& output_rowset : _output_rowsets) {
        if (!output_rowset->is_local()) {
            _tablet->record_unused_remote_rowset(output_rowset->rowset_id(),
                                                 output_rowset->rowset_meta()->resource_id(),
                                                 output_rowset->num_segments());
            return;
        }
        _engine.add_unused_rowset(output_rowset);
    }
}

} // namespace doris
