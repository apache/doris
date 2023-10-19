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
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"

namespace doris {

IndexBuilder::IndexBuilder(const TabletSharedPtr& tablet, const std::vector<TColumn>& columns,
                           const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                           bool is_drop_op)
        : _tablet(tablet),
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
    std::set<int32_t> without_index_uids;
    for (auto i = 0; i < _input_rowsets.size(); ++i) {
        auto input_rowset = _input_rowsets[i];
        TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
        auto input_rs_tablet_schema = input_rowset->tablet_schema();
        output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);
        if (_is_drop_op) {
            // base on input rowset's tablet_schema to build
            // output rowset's tablet_schema which only remove
            // the indexes specified in this drop index request
            for (auto t_inverted_index : _alter_inverted_indexes) {
                output_rs_tablet_schema->remove_index(t_inverted_index.index_id);
            }
        } else {
            // base on input rowset's tablet_schema to build
            // output rowset's tablet_schema which only add
            // the indexes specified in this build index request
            for (auto t_inverted_index : _alter_inverted_indexes) {
                TabletIndex index;
                index.init_from_thrift(t_inverted_index, *input_rs_tablet_schema);
                auto column_uid = index.col_unique_ids()[0];
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
        std::unique_ptr<RowsetWriter> output_rs_writer;
        RowsetWriterContext context;
        context.version = input_rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = input_rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = output_rs_tablet_schema;
        context.newest_write_timestamp = input_rs_reader->newest_write_timestamp();
        context.fs = input_rs_reader->rowset()->rowset_meta()->fs();
        Status status = _tablet->create_rowset_writer(context, &output_rs_writer);
        if (!status.ok()) {
            return Status::Error<ErrorCode::ROWSET_BUILDER_INIT>(status.to_string());
        }

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
        rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size());
        rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
        rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size());
        rowset_meta->set_empty(input_rowset_meta->empty());
        rowset_meta->set_num_segments(input_rowset_meta->num_segments());
        rowset_meta->set_segments_overlap(input_rowset_meta->segments_overlap());
        rowset_meta->set_rowset_state(input_rowset_meta->rowset_state());
        std::vector<KeyBoundsPB> key_bounds;
        input_rowset->get_segments_key_bounds(&key_bounds);
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
    if (_is_drop_op) {
        // delete invertd index file by gc thread when gc input rowset
        return Status::OK();
    } else {
        // create inverted index writer
        std::string segment_dir = _tablet->tablet_path();
        auto fs = output_rowset_meta->fs();
        auto output_rowset_schema = output_rowset_meta->tablet_schema();
        for (auto& seg_ptr : segments) {
            std::string segment_filename = fmt::format(
                    "{}_{}.dat", output_rowset_meta->rowset_id().to_string(), seg_ptr->id());
            std::vector<ColumnId> return_columns;
            std::vector<std::pair<int64_t, int64_t>> inverted_index_writer_signs;
            _olap_data_convertor->reserve(_alter_inverted_indexes.size());
            // create inverted index writer
            for (auto i = 0; i < _alter_inverted_indexes.size(); ++i) {
                auto inverted_index = _alter_inverted_indexes[i];
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
                DCHECK(output_rowset_schema->has_inverted_index_with_index_id(index_id, ""));
                _olap_data_convertor->add_column_data_convertor(column);
                return_columns.emplace_back(column_idx);
                std::unique_ptr<Field> field(FieldFactory::create(column));
                auto index_meta = output_rowset_schema->get_inverted_index(column);
                std::unique_ptr<segment_v2::InvertedIndexColumnWriter> inverted_index_builder;
                try {
                    RETURN_IF_ERROR(segment_v2::InvertedIndexColumnWriter::create(
                            field.get(), &inverted_index_builder, segment_filename, segment_dir,
                            index_meta, fs));
                } catch (const std::exception& e) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occured: {}", e.what());
                }

                if (inverted_index_builder) {
                    auto writer_sign = std::make_pair(seg_ptr->id(), index_id);
                    _inverted_index_builders.insert(
                            std::make_pair(writer_sign, std::move(inverted_index_builder)));
                    inverted_index_writer_signs.push_back(writer_sign);
                }
            }

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

            std::shared_ptr<vectorized::Block> block = std::make_shared<vectorized::Block>(
                    output_rowset_schema->create_block(return_columns));
            while (true) {
                auto st = iter->next_batch(block.get());
                if (!st.ok()) {
                    if (st.is<ErrorCode::END_OF_FILE>()) {
                        break;
                    }
                    LOG(WARNING)
                            << "failed to read next block when schema change for inverted index."
                            << ", err=" << st.to_string();
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
                        _inverted_index_builders[writer_sign]->finish();
                    }
                } catch (const std::exception& e) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occured: {}", e.what());
                }
            }

            _olap_data_convertor->reset();
        }
        _inverted_index_builders.clear();
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
        auto converted_result = _olap_data_convertor->convert_column_data(i);
        if (!converted_result.first.ok()) {
            LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }

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
    if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(field->get_sub_field_count() == 1);
        BitmapIterator null_iter(null_map, num_rows);
        bool is_null = false;
        size_t this_run = 0;
        while ((this_run = null_iter.Next(&is_null)) > 0) {
            if (is_null) {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_nulls(this_run));
            } else {
                // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
                auto data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
                // total number length
                size_t element_cnt = size_t((unsigned long)(*data_ptr));
                auto offset_data = *(data_ptr + 1);
                const uint8_t* offsets_ptr = (const uint8_t*)offset_data;
                if (element_cnt > 0) {
                    auto data = *(data_ptr + 2);
                    auto nested_null_map = *(data_ptr + 3);
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                            field->get_sub_field(0)->size(), reinterpret_cast<const void*>(data),
                            reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr,
                            num_rows));
                }
            }
        }
        return Status::OK();
    }

    try {
        do {
            auto step = next_run_step();
            if (null_map[offset]) {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_nulls(step));
            } else {
                if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                    DCHECK(field->get_sub_field_count() == 1);
                    const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                            field->get_sub_field(0)->size(), col_cursor, step));
                } else {
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(
                            column_name, *ptr, step));
                }
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
            const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                    field->get_sub_field(0)->size(), col_cursor, num_rows));
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
    for (auto i = 0; i < _output_rowsets.size(); ++i) {
        SegmentCacheHandle segment_cache_handle;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                std::static_pointer_cast<BetaRowset>(_output_rowsets[i]), &segment_cache_handle));
        auto output_rowset_meta = _output_rowsets[i]->rowset_meta();
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
                                                         _tablet->full_name());
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
    for (auto rowset_ptr : _output_rowsets) {
        auto rowset_id = rowset_ptr->rowset_id();
        if (StorageEngine::instance()->check_rowset_id_in_unused_rowsets(rowset_id)) {
            DCHECK(false) << "output rowset: " << rowset_id.to_string() << " in unused rowsets";
        }
    }

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        std::lock_guard<std::mutex> rowset_update_wlock(_tablet->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> meta_wlock(_tablet->get_header_lock());
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

        // modify_rowsets will remove the delete_bimap for input rowsets,
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
    for (auto output_rowset : _output_rowsets) {
        if (!output_rowset->is_local()) {
            Tablet::erase_pending_remote_rowset(output_rowset->rowset_id().to_string());
            _tablet->record_unused_remote_rowset(output_rowset->rowset_id(),
                                                 output_rowset->rowset_meta()->resource_id(),
                                                 output_rowset->num_segments());
            return;
        }
        StorageEngine::instance()->add_unused_rowset(output_rowset);
    }
}

} // namespace doris
