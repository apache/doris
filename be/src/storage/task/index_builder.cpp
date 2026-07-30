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

#include "storage/task/index_builder.h"

#include <mutex>

#include "common/cast_set.h"
#include "common/logging.h"
#include "common/status.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h" // IndexColumnWriter, complete type for member calls
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
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
    _olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
}

IndexBuilder::~IndexBuilder() {
    _olap_data_convertor.reset();
    _index_column_writers.clear();
}

Status IndexBuilder::init() {
    for (auto inverted_index : _alter_inverted_indexes) {
        _alter_index_ids.insert(inverted_index.index_id);
    }
    return Status::OK();
}

Status IndexBuilder::plan_snii_index_rewrite(
        const TabletSchema& input_schema, const TabletSchema& output_schema,
        const std::set<int64_t>& alter_index_ids,
        const std::function<Status(const TabletIndex&, bool*)>& container_has,
        SniiIndexRewritePlan* plan) {
    DORIS_CHECK(plan != nullptr);
    plan->inherit_keys.clear();
    plan->build_columns.clear();
    // Keyed and ordered by column unique id, so one raw column read feeds every
    // index on that column and the output layout is deterministic.
    std::map<int32_t, std::vector<const TabletIndex*>> build_by_column;
    std::set<std::pair<uint64_t, std::string>> seen_keys;
    for (const TabletIndex* index : output_schema.inverted_indexes()) {
        const auto key =
                std::make_pair(cast_set<uint64_t>(index->index_id()), index->get_index_suffix());
        // The target schema holds each logical index exactly once; the final
        // directory could not hold a duplicate key anyway.
        DORIS_CHECK(seen_keys.insert(key).second);
        bool in_container = false;
        RETURN_IF_ERROR(container_has(*index, &in_container));
        const TabletIndex* input_index = nullptr;
        for (const TabletIndex* candidate : input_schema.inverted_indexes()) {
            if (candidate->index_id() == index->index_id() &&
                candidate->get_index_suffix() == index->get_index_suffix()) {
                input_index = candidate;
                break;
            }
        }
        const bool definition_unchanged =
                input_index != nullptr && input_index->properties() == index->properties();
        if (in_container && definition_unchanged) {
            plan->inherit_keys.push_back({.index_id = key.first, .index_suffix = key.second});
            continue;
        }
        if (!in_container && !alter_index_ids.contains(index->index_id())) {
            // Not requested and nothing to inherit: the index stays absent for
            // this rowset, exactly as the V2 path leaves it.
            LOG(INFO) << "SNII index " << index->index_id()
                      << " is absent from the source container and was not requested; "
                         "it stays absent from the rewritten rowset";
            continue;
        }
        // Requested and buildable, or present under the same key with a CHANGED
        // definition: the final directory must match the target schema, so the
        // old metadata is dropped and the index is rebuilt from the raw column.
        //
        // Only THIS branch needs a column to read: inheriting copies raw bytes by
        // key, and the "stays absent" branch above reads nothing. So an index
        // that binds no column fails the rewrite only when it actually has to be
        // rebuilt -- a malformed index elsewhere in the schema, neither requested
        // nor present, must not block building the ones that are fine.
        if (index->col_unique_ids().empty()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                    "SNII rewrite: index {} must be rebuilt but binds no column unique id",
                    index->index_id());
        }
        build_by_column[index->col_unique_ids()[0]].push_back(index);
    }
    plan->build_columns.assign(build_by_column.begin(), build_by_column.end());
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
        bool is_local_rowset = input_rowset->is_local();
        DBUG_EXECUTE_IF("IndexBuilder::update_inverted_index_info_is_local_rowset",
                        { is_local_rowset = false; })
        if (!is_local_rowset) [[unlikely]] {
            // DCHECK(false) << _tablet->tablet_id() << ' ' << input_rowset->rowset_id();
            return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                         _tablet->tablet_id(),
                                         input_rowset->rowset_id().to_string());
        }

        TabletSchemaSPtr output_rs_tablet_schema = std::make_shared<TabletSchema>();
        const auto& input_rs_tablet_schema = input_rowset->tablet_schema();
        output_rs_tablet_schema->copy_from(*input_rs_tablet_schema);
        const bool is_snii_drop =
                _is_drop_op && input_rs_tablet_schema->get_inverted_index_storage_format() ==
                                       InvertedIndexStorageFormatPB::SNII;
        int64_t total_index_size = 0;
        if (!is_snii_drop) {
            auto* beta_rowset = reinterpret_cast<BetaRowset*>(input_rowset.get());
            auto size_st = beta_rowset->get_inverted_index_size(&total_index_size);
            DBUG_EXECUTE_IF("IndexBuilder::update_inverted_index_info_size_st_not_ok", {
                size_st = Status::Error<ErrorCode::INIT_FAILED>("debug point: get fs failed");
            })
            if (!size_st.ok() && !size_st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() &&
                !size_st.is<ErrorCode::NOT_FOUND>()) {
                return size_st;
            }
        }
        auto num_segments = input_rowset->num_segments();
        size_t drop_index_size = 0;

        if (_is_drop_op) {
            for (const auto& t_inverted_index : _alter_inverted_indexes) {
                DCHECK_EQ(t_inverted_index.columns.size(), 1);
                auto column_name = t_inverted_index.columns[0];
                auto column_idx = output_rs_tablet_schema->field_index(column_name);
                if (column_idx < 0) {
                    if (!t_inverted_index.column_unique_ids.empty()) {
                        auto column_unique_id = t_inverted_index.column_unique_ids[0];
                        column_idx = output_rs_tablet_schema->field_index(column_unique_id);
                    }
                    if (column_idx < 0) {
                        LOG(WARNING) << "referenced column was missing. "
                                     << "[column=" << column_name
                                     << " referenced_column=" << column_idx << "]";
                        continue;
                    }
                }
                auto column = output_rs_tablet_schema->column(column_idx);

                // inverted index
                auto index_metas = output_rs_tablet_schema->inverted_indexs(column);
                for (const auto& index_meta : index_metas) {
                    // Only drop the index that matches the requested index_id,
                    // not all indexes on this column
                    if (index_meta->index_id() != t_inverted_index.index_id) {
                        continue;
                    }
                    if (output_rs_tablet_schema->get_inverted_index_storage_format() ==
                        InvertedIndexStorageFormatPB::V1) {
                        const auto& fs = io::global_local_filesystem();

                        for (int seg_id = 0; seg_id < num_segments; seg_id++) {
                            auto seg_path = local_segment_path(
                                    _tablet->tablet_path(), input_rowset->rowset_id().to_string(),
                                    seg_id);
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

                // ann index
                const auto* ann_index = output_rs_tablet_schema->ann_index(column);
                if (!ann_index) {
                    continue;
                }
                // Only drop the ann index that matches the requested index_id
                if (ann_index->index_id() != t_inverted_index.index_id) {
                    continue;
                }
                DCHECK(output_rs_tablet_schema->get_inverted_index_storage_format() !=
                       InvertedIndexStorageFormatPB::V1);
                _dropped_inverted_indexes.push_back(*ann_index);
                // ATTN: DO NOT REMOVE INDEX AFTER OUTPUT_ROWSET_WRITER CREATED.
                // remove dropped index_meta from output rowset tablet schema
                output_rs_tablet_schema->remove_index(ann_index->index_id());
            }

            DBUG_EXECUTE_IF("index_builder.update_inverted_index_info.drop_index", {
                auto indexes_count = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                        "index_builder.update_inverted_index_info.drop_index", "indexes_count", 0);
                if (indexes_count < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "indexes count cannot be negative");
                }
                auto indexes_size = output_rs_tablet_schema->inverted_indexes().size();
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
                    continue;
                }
                const TabletColumn& col = output_rs_tablet_schema->column_by_uid(column_uid);

                // inverted index
                auto exist_indexs = output_rs_tablet_schema->inverted_indexs(col);
                for (const auto& exist_index : exist_indexs) {
                    if (exist_index->index_id() != index.index_id()) {
                        if (exist_index->is_same_except_id(&index)) {
                            LOG(WARNING) << fmt::format(
                                    "column: {} has a exist inverted index, but the index id not "
                                    "equal "
                                    "request's index id, , exist index id: {}, request's index id: "
                                    "{}, "
                                    "remove exist index in new output_rs_tablet_schema",
                                    column_uid, exist_index->index_id(), index.index_id());
                            without_index_uids.insert(exist_index->index_id());
                            output_rs_tablet_schema->remove_index(exist_index->index_id());
                        }
                    }
                }

                // ann index
                const auto* exist_index = output_rs_tablet_schema->ann_index(col);
                if (exist_index && exist_index->index_id() != index.index_id()) {
                    if (exist_index->is_same_except_id(&index)) {
                        LOG(WARNING) << fmt::format(
                                "column: {} has a exist ann index, but the index id not "
                                "equal request's index id, , exist index id: {}, request's index "
                                "id: {}, remove exist index in new output_rs_tablet_schema",
                                column_uid, exist_index->index_id(), index.index_id());
                        without_index_uids.insert(exist_index->index_id());
                        output_rs_tablet_schema->remove_index(exist_index->index_id());
                    }
                }

                output_rs_tablet_schema->append_index(std::move(index));
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
        if (!_is_drop_op && output_rs_tablet_schema->get_inverted_index_storage_format() ==
                                    InvertedIndexStorageFormatPB::SNII) {
            // The rewrite plan compares index definitions between the input and
            // output schema (inherit vs rebuild); keep the input schema reachable
            // from the output rowset id.
            _input_rowset_schemas.emplace(output_rs_writer->rowset_id().to_string(),
                                          input_rs_tablet_schema);
        }

        // if without_index_uids is not empty, copy _alter_index_ids to it
        // else just use _alter_index_ids to avoid copy
        if (!without_index_uids.empty()) {
            without_index_uids.insert(_alter_index_ids.begin(), _alter_index_ids.end());
        }

        const bool preserve_snii_container =
                is_snii_drop && (output_rs_tablet_schema->has_inverted_index() ||
                                 output_rs_tablet_schema->has_ann_index());
        std::set<int64_t>* excluded_index_ids =
                without_index_uids.empty() ? &_alter_index_ids : &without_index_uids;
        if (preserve_snii_container) {
            // SNII logical indexes share one immutable container. Preserve that container in O(1)
            // while the output schema hides the dropped logical index; compaction reclaims its
            // physical bytes later. When no logical index survives, keep the exclusion set so no
            // container is linked.
            excluded_index_ids = nullptr;
        }

        // build output rowset
        RETURN_IF_ERROR(input_rowset->link_files_to(
                _tablet->tablet_path(), output_rs_writer->rowset_id(), 0, excluded_index_ids));

        auto input_rowset_meta = input_rowset->rowset_meta();
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_num_rows(input_rowset_meta->num_rows());
        if (output_rs_tablet_schema->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::V1) {
            if (_is_drop_op) {
                VLOG_DEBUG << "data_disk_size:" << input_rowset_meta->data_disk_size()
                           << " total_disk_size:" << input_rowset_meta->total_disk_size()
                           << " index_disk_size:" << input_rowset_meta->index_disk_size()
                           << " drop_index_size:" << drop_index_size;
                rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size() -
                                                 drop_index_size);
                rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
                rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size() -
                                                 drop_index_size);
            } else {
                rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size());
                rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
                rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size());
            }
        } else if (is_snii_drop) {
            rowset_meta->set_total_disk_size(preserve_snii_container
                                                     ? input_rowset_meta->total_disk_size()
                                                     : input_rowset_meta->data_disk_size());
            rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
            rowset_meta->set_index_disk_size(
                    preserve_snii_container ? input_rowset_meta->index_disk_size() : 0);
        } else {
            for (int seg_id = 0; seg_id < num_segments; seg_id++) {
                auto seg_path = DORIS_TRY(input_rowset->segment_path(seg_id));
                auto idx_file_reader = std::make_unique<IndexFileReader>(
                        context.fs(),
                        std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
                        output_rs_tablet_schema->get_inverted_index_storage_format(),
                        InvertedIndexFileInfo(), _tablet->tablet_id());
                auto st = idx_file_reader->init();
                DBUG_EXECUTE_IF(
                        "IndexBuilder::update_inverted_index_info_index_file_reader_init_not_ok", {
                            st = Status::Error<ErrorCode::INIT_FAILED>(
                                    "debug point: reader init error");
                        })
                if (!st.ok() && !st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
                    return st;
                }
                _index_file_readers.emplace(
                        std::make_pair(output_rs_writer->rowset_id().to_string(), seg_id),
                        std::move(idx_file_reader));
            }
            rowset_meta->set_total_disk_size(input_rowset_meta->total_disk_size() -
                                             total_index_size);
            rowset_meta->set_data_disk_size(input_rowset_meta->data_disk_size());
            rowset_meta->set_index_disk_size(input_rowset_meta->index_disk_size() -
                                             total_index_size);
        }
        rowset_meta->set_empty(input_rowset_meta->empty());
        rowset_meta->set_num_segments(input_rowset_meta->num_segments());
        rowset_meta->set_segments_overlap(input_rowset_meta->segments_overlap());
        rowset_meta->set_rowset_state(input_rowset_meta->rowset_state());
        std::vector<KeyBoundsPB> key_bounds;
        RETURN_IF_ERROR(input_rowset->get_segments_key_bounds(&key_bounds));
        rowset_meta->set_segments_key_bounds_truncated(
                input_rowset_meta->is_segments_key_bounds_truncated());
        // preserve aggregated layout via the setter so the aggregated flag is not
        // clobbered by set_segments_key_bounds's default reset path.
        rowset_meta->set_segments_key_bounds(
                key_bounds, input_rowset_meta->is_segments_key_bounds_aggregated());
        std::vector<uint32_t> num_segment_rows;
        input_rowset_meta->get_num_segment_rows(&num_segment_rows);
        rowset_meta->set_num_segment_rows(num_segment_rows);
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
    bool is_local_rowset = output_rowset_meta->is_local();
    DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_is_local_rowset",
                    { is_local_rowset = false; })
    if (!is_local_rowset) [[unlikely]] {
        // DCHECK(false) << _tablet->tablet_id() << ' ' << output_rowset_meta->rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _tablet->tablet_id(),
                                     output_rowset_meta->rowset_id().to_string());
    }

    if (_is_drop_op) {
        const auto& output_rs_tablet_schema = output_rowset_meta->tablet_schema();
        if (output_rs_tablet_schema->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::SNII) {
            LOG(INFO) << "skip physical SNII inverted index rewrite for drop index. tablet_id="
                      << _tablet->tablet_id()
                      << " rowset_id=" << output_rowset_meta->rowset_id().to_string();
            return Status::OK();
        }
        if (output_rs_tablet_schema->get_inverted_index_storage_format() !=
            InvertedIndexStorageFormatPB::V1) {
            const auto& fs = output_rowset_meta->fs();

            const auto& output_rowset_schema = output_rowset_meta->tablet_schema();
            size_t inverted_index_size = 0;
            for (auto& seg_ptr : segments) {
                auto idx_file_reader_iter = _index_file_readers.find(
                        std::make_pair(output_rowset_meta->rowset_id().to_string(), seg_ptr->id()));
                DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_can_not_find_reader_drop_op",
                                { idx_file_reader_iter = _index_file_readers.end(); })
                if (idx_file_reader_iter == _index_file_readers.end()) {
                    LOG(ERROR) << "idx_file_reader_iter" << output_rowset_meta->rowset_id() << ":"
                               << seg_ptr->id() << " cannot be found";
                    continue;
                }
                auto dirs = DORIS_TRY(idx_file_reader_iter->second->get_all_directories());

                std::string index_path_prefix {
                        InvertedIndexDescriptor::get_index_file_path_prefix(local_segment_path(
                                _tablet->tablet_path(), output_rowset_meta->rowset_id().to_string(),
                                seg_ptr->id()))};

                std::string index_path =
                        InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
                io::FileWriterPtr file_writer;
                Status st = fs->create_file(index_path, &file_writer);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to create writable file. path=" << index_path
                                 << ", err: " << st;
                    return st;
                }
                auto index_file_writer = std::make_unique<IndexFileWriter>(
                        fs, std::move(index_path_prefix),
                        output_rowset_meta->rowset_id().to_string(), seg_ptr->id(),
                        output_rowset_schema->get_inverted_index_storage_format(),
                        std::move(file_writer), true /* can_use_ram_dir */, _tablet->tablet_id());
                RETURN_IF_ERROR(index_file_writer->initialize(dirs));
                // create inverted index writer
                for (auto& index_meta : _dropped_inverted_indexes) {
                    RETURN_IF_ERROR(index_file_writer->delete_index(&index_meta));
                }
                _index_file_writers.emplace(seg_ptr->id(), std::move(index_file_writer));
            }
            for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
                auto st = index_file_writer->begin_close();
                if (!st.ok()) {
                    LOG(ERROR) << "close index_file_writer error:" << st;
                    return st;
                }
                inverted_index_size += index_file_writer->get_index_file_total_size();
            }
            for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
                auto st = index_file_writer->finish_close();
                if (!st.ok()) {
                    LOG(ERROR) << "wait close index_file_writer error:" << st;
                    return st;
                }
            }
            _index_file_writers.clear();
            output_rowset_meta->set_data_disk_size(output_rowset_meta->data_disk_size());
            output_rowset_meta->set_total_disk_size(output_rowset_meta->total_disk_size() +
                                                    inverted_index_size);
            output_rowset_meta->set_index_disk_size(output_rowset_meta->index_disk_size() +
                                                    inverted_index_size);
        }
        LOG(INFO) << "all row nums. source_rows=" << output_rowset_meta->num_rows();
        return Status::OK();
    } else {
        // create inverted or ann index writer
        const auto& fs = output_rowset_meta->fs();
        auto output_rowset_schema = output_rowset_meta->tablet_schema();
        if (output_rowset_schema->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::SNII) {
            return _handle_single_rowset_snii(output_rowset_meta, segments);
        }
        size_t inverted_index_size = 0;
        for (auto& seg_ptr : segments) {
            std::string index_path_prefix {
                    InvertedIndexDescriptor::get_index_file_path_prefix(local_segment_path(
                            _tablet->tablet_path(), output_rowset_meta->rowset_id().to_string(),
                            seg_ptr->id()))};
            std::vector<ColumnId> return_columns;
            std::vector<std::pair<int64_t, int64_t>> inverted_index_writer_signs;
            _olap_data_convertor->reserve(_alter_inverted_indexes.size());

            std::unique_ptr<IndexFileWriter> index_file_writer = nullptr;
            if (output_rowset_schema->get_inverted_index_storage_format() >=
                InvertedIndexStorageFormatPB::V2) {
                auto idx_file_reader_iter = _index_file_readers.find(
                        std::make_pair(output_rowset_meta->rowset_id().to_string(), seg_ptr->id()));
                DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_can_not_find_reader",
                                { idx_file_reader_iter = _index_file_readers.end(); })
                if (idx_file_reader_iter == _index_file_readers.end()) {
                    LOG(ERROR) << "idx_file_reader_iter" << output_rowset_meta->rowset_id() << ":"
                               << seg_ptr->id() << " cannot be found";
                    continue;
                }
                std::string index_path =
                        InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
                io::FileWriterPtr file_writer;
                Status st = fs->create_file(index_path, &file_writer);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to create writable file. path=" << index_path
                                 << ", err: " << st;
                    return st;
                }
                auto dirs = DORIS_TRY(idx_file_reader_iter->second->get_all_directories());
                index_file_writer = std::make_unique<IndexFileWriter>(
                        fs, index_path_prefix, output_rowset_meta->rowset_id().to_string(),
                        seg_ptr->id(), output_rowset_schema->get_inverted_index_storage_format(),
                        std::move(file_writer), true /* can_use_ram_dir */, _tablet->tablet_id());
                RETURN_IF_ERROR(index_file_writer->initialize(dirs));
            } else {
                index_file_writer = std::make_unique<IndexFileWriter>(
                        fs, index_path_prefix, output_rowset_meta->rowset_id().to_string(),
                        seg_ptr->id(), output_rowset_schema->get_inverted_index_storage_format(),
                        nullptr, true /* can_use_ram_dir */, _tablet->tablet_id());
            }
            // create inverted index writer, or ann index writer
            for (auto inverted_index : _alter_inverted_indexes) {
                DCHECK(inverted_index.index_type == TIndexType::INVERTED ||
                       inverted_index.index_type == TIndexType::ANN);
                DCHECK_EQ(inverted_index.columns.size(), 1);
                auto index_id = inverted_index.index_id;
                auto column_name = inverted_index.columns[0];
                auto column_idx = output_rowset_schema->field_index(column_name);
                if (column_idx < 0) {
                    if (inverted_index.__isset.column_unique_ids &&
                        !inverted_index.column_unique_ids.empty()) {
                        column_idx = output_rowset_schema->field_index(
                                inverted_index.column_unique_ids[0]);
                    }
                    if (column_idx < 0) {
                        LOG(WARNING) << "referenced column was missing. "
                                     << "[column=" << column_name
                                     << " referenced_column=" << column_idx << "]";
                        continue;
                    }
                }
                auto column = output_rowset_schema->column(column_idx);
                // variant column is not support for building index
                auto is_support_inverted_index =
                        IndexColumnWriter::check_support_inverted_index(column);
                auto is_support_ann_index = IndexColumnWriter::check_support_ann_index(column);
                DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_support_inverted_index",
                                { is_support_inverted_index = false; })
                if (!is_support_inverted_index && !is_support_ann_index) {
                    continue;
                }
                DCHECK(output_rowset_schema->has_inverted_index_with_index_id(index_id));
                _olap_data_convertor->add_column_data_convertor(column);
                return_columns.emplace_back(column_idx);

                if (inverted_index.index_type == TIndexType::INVERTED) {
                    // inverted index
                    auto index_metas = output_rowset_schema->inverted_indexs(column);
                    for (const auto& index_meta : index_metas) {
                        if (index_meta->index_id() != index_id) {
                            continue;
                        }
                        std::unique_ptr<segment_v2::IndexColumnWriter> inverted_index_builder;
                        try {
                            RETURN_IF_ERROR(segment_v2::IndexColumnWriter::create(
                                    &column, &inverted_index_builder, index_file_writer.get(),
                                    index_meta));
                            DBUG_EXECUTE_IF(
                                    "IndexBuilder::handle_single_rowset_index_column_writer_create_"
                                    "error",
                                    {
                                        _CLTHROWA(CL_ERR_IO,
                                                  "debug point: "
                                                  "handle_single_rowset_index_column_writer_create_"
                                                  "error");
                                    })
                        } catch (const std::exception& e) {
                            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                                    "CLuceneError occurred: {}", e.what());
                        }

                        if (inverted_index_builder) {
                            auto writer_sign = std::make_pair(seg_ptr->id(), index_id);
                            auto [index_column_writer_it, inserted] = _index_column_writers.insert(
                                    std::make_pair(writer_sign, std::move(inverted_index_builder)));
                            DORIS_CHECK(inserted);
                            DORIS_CHECK(index_column_writer_it->second != nullptr);
                            inverted_index_writer_signs.emplace_back(writer_sign);
                        }
                    }
                } else if (inverted_index.index_type == TIndexType::ANN) {
                    // ann index
                    const auto* index_meta = output_rowset_schema->ann_index(column);
                    if (index_meta && index_meta->index_id() == index_id) {
                        std::unique_ptr<segment_v2::IndexColumnWriter> index_writer;
                        try {
                            RETURN_IF_ERROR(segment_v2::IndexColumnWriter::create(
                                    &column, &index_writer, index_file_writer.get(), index_meta));
                            DBUG_EXECUTE_IF(
                                    "IndexBuilder::handle_single_rowset_index_column_writer_create_"
                                    "error",
                                    {
                                        _CLTHROWA(CL_ERR_IO,
                                                  "debug point: "
                                                  "handle_single_rowset_index_column_writer_create_"
                                                  "error");
                                    })
                        } catch (const std::exception& e) {
                            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                                    "CLuceneError occurred: {}", e.what());
                        }

                        if (index_writer) {
                            auto writer_sign = std::make_pair(seg_ptr->id(), index_id);
                            auto [index_column_writer_it, inserted] = _index_column_writers.insert(
                                    std::make_pair(writer_sign, std::move(index_writer)));
                            DORIS_CHECK(inserted);
                            DORIS_CHECK(index_column_writer_it->second != nullptr);
                            inverted_index_writer_signs.emplace_back(writer_sign);
                        }
                    }
                }
            }

            // DO NOT forget index_file_writer for the segment, otherwise, original inverted index will be deleted.
            auto [index_file_writer_it, inserted] =
                    _index_file_writers.emplace(seg_ptr->id(), std::move(index_file_writer));
            DORIS_CHECK(inserted);
            DORIS_CHECK(index_file_writer_it->second != nullptr);
            if (return_columns.empty()) {
                // no columns to read
                continue;
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
            DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_create_iterator_error", {
                res = Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "debug point: handle_single_rowset_create_iterator_error");
            })
            if (!res.ok()) {
                LOG(WARNING) << "failed to create iterator[" << seg_ptr->id()
                             << "]: " << res.to_string();
                return Status::Error<ErrorCode::ROWSET_READER_INIT>(res.to_string());
            }

            auto block = Block::create_unique(output_rowset_schema->create_block(return_columns));
            while (true) {
                auto status = iter->next_batch(block.get());
                DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_iterator_next_batch_error", {
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

                // write inverted index data, or ann index data
                status = _write_inverted_index_data(output_rowset_schema, iter->data_id(),
                                                    block.get());
                DBUG_EXECUTE_IF(
                        "IndexBuilder::handle_single_rowset_write_inverted_index_data_error", {
                            status = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                    "debug point: "
                                    "handle_single_rowset_write_inverted_index_data_error");
                        })
                if (!status.ok()) {
                    return status;
                }
                block->clear_column_data();
            }

            // finish write inverted index, flush data to compound file
            for (auto& writer_sign : inverted_index_writer_signs) {
                try {
                    auto index_column_writer_it = _index_column_writers.find(writer_sign);
                    DORIS_CHECK(index_column_writer_it != _index_column_writers.end());
                    DORIS_CHECK(index_column_writer_it->second != nullptr);
                    RETURN_IF_ERROR(index_column_writer_it->second->finish());
                    DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_index_build_finish_error", {
                        _CLTHROWA(CL_ERR_IO,
                                  "debug point: handle_single_rowset_index_build_finish_error");
                    })
                } catch (const std::exception& e) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "CLuceneError occurred: {}", e.what());
                }
            }

            _olap_data_convertor->reset();
        }
        for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
            auto st = index_file_writer->begin_close();
            DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_file_writer_close_error", {
                st = Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "debug point: handle_single_rowset_file_writer_close_error");
            })
            if (!st.ok()) {
                LOG(ERROR) << "close index_file_writer error:" << st;
                return st;
            }
            inverted_index_size += index_file_writer->get_index_file_total_size();
        }
        for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
            auto st = index_file_writer->finish_close();
            if (!st.ok()) {
                LOG(ERROR) << "wait close index_file_writer error:" << st;
                return st;
            }
        }
        _index_column_writers.clear();
        _index_file_writers.clear();
        output_rowset_meta->set_data_disk_size(output_rowset_meta->data_disk_size());
        output_rowset_meta->set_total_disk_size(output_rowset_meta->total_disk_size() +
                                                inverted_index_size);
        output_rowset_meta->set_index_disk_size(output_rowset_meta->index_disk_size() +
                                                inverted_index_size);
        LOG(INFO) << "all row nums. source_rows=" << output_rowset_meta->num_rows();
    }

    return Status::OK();
}

Status IndexBuilder::_handle_single_rowset_snii(
        RowsetMetaSharedPtr output_rowset_meta,
        std::vector<segment_v2::SegmentSharedPtr>& segments) {
    const std::string rowset_id = output_rowset_meta->rowset_id().to_string();
    auto input_schema_it = _input_rowset_schemas.find(rowset_id);
    DORIS_CHECK(input_schema_it != _input_rowset_schemas.end());

    for (auto& seg_ptr : segments) {
        RETURN_IF_ERROR(_rewrite_single_segment_snii(output_rowset_meta->fs(),
                                                     output_rowset_meta->tablet_schema(),
                                                     *input_schema_it->second, rowset_id, seg_ptr));
    }
    size_t inverted_index_size = 0;
    for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
        RETURN_IF_ERROR(index_file_writer->begin_close());
        inverted_index_size += index_file_writer->get_index_file_total_size();
    }
    for (auto&& [seg_id, index_file_writer] : _index_file_writers) {
        RETURN_IF_ERROR(index_file_writer->finish_close());
    }
    _index_column_writers.clear();
    _index_file_writers.clear();
    output_rowset_meta->set_data_disk_size(output_rowset_meta->data_disk_size());
    output_rowset_meta->set_total_disk_size(output_rowset_meta->total_disk_size() +
                                            inverted_index_size);
    output_rowset_meta->set_index_disk_size(output_rowset_meta->index_disk_size() +
                                            inverted_index_size);
    LOG(INFO) << "all row nums. source_rows=" << output_rowset_meta->num_rows();
    return Status::OK();
}

Status IndexBuilder::_rewrite_single_segment_snii(const io::FileSystemSPtr& fs,
                                                  const TabletSchemaSPtr& output_rowset_schema,
                                                  const TabletSchema& input_schema,
                                                  const std::string& rowset_id,
                                                  const segment_v2::SegmentSharedPtr& seg_ptr) {
    // The source reader was registered in update_inverted_index_info. A rowset
    // written before any index existed has no container file at all; everything
    // requested is then built fresh.
    auto reader_it = _index_file_readers.find(std::make_pair(rowset_id, seg_ptr->id()));
    DORIS_CHECK(reader_it != _index_file_readers.end());
    IndexFileReader* source_reader = reader_it->second.get();
    bool has_container = true;
    {
        Status init_status = source_reader->init();
        if (init_status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
            has_container = false;
        } else if (!init_status.ok()) {
            return init_status;
        }
    }
    // The rewrite plan below can only represent IndexType::INVERTED, on BOTH
    // sides, and a shortfall on either side is a silent drop:
    //   * target schema: a non-text index there is never planned, so the sealed
    //     container would simply not have it while the schema claims it does;
    //   * source container: a blob logical index there is neither inherited nor
    //     rebuilt, so it would not be carried over.
    // prepare_rewrite_snapshot refuses blob-bearing containers, but only when
    // something is inheritable, so it cannot be relied on here. FE rejects ANN on
    // SNII in CREATE TABLE and ADD INDEX, and the storage format is immutable
    // after creation -- but RESTORE re-validates almost nothing, so treat these
    // as reachable rather than impossible, and keep them refused once that gate
    // is relaxed for real.
    if (output_rowset_schema->has_ann_index()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "SNII rewrite: the target schema holds a non-text index this rewrite can neither "
                "inherit nor rebuild. tablet_id={} rowset_id={} segment_id={}",
                _tablet->tablet_id(), rowset_id, seg_ptr->id());
    }
    if (has_container && source_reader->snii_has_blob_index()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "SNII rewrite over a container holding a blob logical index is not supported; "
                "carrying blob entries through a rewrite needs them re-emitted into the new "
                "container. tablet_id={} rowset_id={} segment_id={}",
                _tablet->tablet_id(), rowset_id, seg_ptr->id());
    }
    const auto container_has = [source_reader, has_container](const TabletIndex& index,
                                                              bool* exists) -> Status {
        if (!has_container) {
            *exists = false;
            return Status::OK();
        }
        return source_reader->index_file_exist(&index, exists);
    };
    SniiIndexRewritePlan plan;
    RETURN_IF_ERROR(plan_snii_index_rewrite(input_schema, *output_rowset_schema, _alter_index_ids,
                                            container_has, &plan));

    std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
            local_segment_path(_tablet->tablet_path(), rowset_id, seg_ptr->id()))};
    std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(index_path, &file_writer));
    auto index_file_writer = std::make_unique<IndexFileWriter>(
            fs, index_path_prefix, rowset_id, seg_ptr->id(), InvertedIndexStorageFormatPB::SNII,
            std::move(file_writer), true /* can_use_ram_dir */, _tablet->tablet_id());

    // ONE inheritance of the source's physical prefix per segment container:
    // unchanged indexes cost no analyzer, no postings decode and no encode.
    if (!plan.inherit_keys.empty()) {
        DORIS_CHECK(has_container);
        snii::reader::SniiRewriteSnapshot snapshot;
        RETURN_IF_ERROR(source_reader->prepare_snii_rewrite_snapshot(
                plan.inherit_keys, seg_ptr->num_rows(), &snapshot));
        RETURN_IF_ERROR(index_file_writer->inherit_snii(snapshot, source_reader->snii_io_reader()));
    }
    if (!plan.build_columns.empty()) {
        RETURN_IF_ERROR(_build_snii_indexes_for_segment(output_rowset_schema, plan,
                                                        index_file_writer.get(), seg_ptr));
    }
    auto [file_writer_it, inserted] =
            _index_file_writers.emplace(seg_ptr->id(), std::move(index_file_writer));
    DORIS_CHECK(inserted);
    DORIS_CHECK(file_writer_it->second != nullptr);
    return Status::OK();
}

Status IndexBuilder::_build_snii_indexes_for_segment(const TabletSchemaSPtr& output_rowset_schema,
                                                     const SniiIndexRewritePlan& plan,
                                                     IndexFileWriter* index_file_writer,
                                                     const segment_v2::SegmentSharedPtr& seg_ptr) {
    // One raw column read per column group; every writer on the column is fed
    // from the same converted data.
    std::vector<std::vector<std::pair<int64_t, int64_t>>> group_writer_signs;
    std::vector<ColumnId> return_columns;
    _olap_data_convertor->reserve(plan.build_columns.size());
    for (const auto& [col_unique_id, index_metas] : plan.build_columns) {
        const int32_t column_idx = output_rowset_schema->field_index(col_unique_id);
        DORIS_CHECK_GE(column_idx, 0);
        const TabletColumn& column = output_rowset_schema->column(column_idx);
        DORIS_CHECK(segment_v2::IndexColumnWriter::check_support_inverted_index(column));
        _olap_data_convertor->add_column_data_convertor(column);
        return_columns.emplace_back(column_idx);
        std::vector<std::pair<int64_t, int64_t>> signs;
        for (const TabletIndex* index_meta : index_metas) {
            std::unique_ptr<segment_v2::IndexColumnWriter> index_column_writer;
            try {
                RETURN_IF_ERROR(segment_v2::IndexColumnWriter::create(
                        &column, &index_column_writer, index_file_writer, index_meta));
            } catch (const std::exception& e) {
                return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "CLuceneError occurred: {}", e.what());
            }
            auto writer_sign =
                    std::make_pair<int64_t, int64_t>(seg_ptr->id(), index_meta->index_id());
            auto [writer_it, inserted] = _index_column_writers.insert(
                    std::make_pair(writer_sign, std::move(index_column_writer)));
            DORIS_CHECK(inserted);
            DORIS_CHECK(writer_it->second != nullptr);
            signs.push_back(writer_sign);
        }
        group_writer_signs.push_back(std::move(signs));
    }

    StorageReadOptions read_options;
    OlapReaderStatistics stats;
    read_options.stats = &stats;
    read_options.tablet_schema = output_rowset_schema;
    std::shared_ptr<Schema> schema =
            std::make_shared<Schema>(output_rowset_schema->columns(), return_columns);
    std::unique_ptr<RowwiseIterator> iter;
    RETURN_IF_ERROR(seg_ptr->new_iterator(schema, read_options, &iter));

    auto block = Block::create_unique(output_rowset_schema->create_block(return_columns));
    while (true) {
        Status status = iter->next_batch(block.get());
        if (!status.ok()) {
            if (status.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            LOG(WARNING) << "failed to read next block when building SNII index."
                         << ", err=" << status.to_string();
            return status;
        }
        RETURN_IF_ERROR(_write_snii_index_data(output_rowset_schema, block.get(), plan,
                                               group_writer_signs));
        block->clear_column_data();
    }
    for (const auto& signs : group_writer_signs) {
        for (const auto& writer_sign : signs) {
            auto writer_it = _index_column_writers.find(writer_sign);
            DORIS_CHECK(writer_it != _index_column_writers.end());
            RETURN_IF_ERROR(writer_it->second->finish());
            DBUG_EXECUTE_IF("IndexBuilder::handle_single_rowset_snii_index_build_finish_error", {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "debug point: handle_single_rowset_snii_index_build_finish_error");
            })
        }
    }
    _olap_data_convertor->reset();
    return Status::OK();
}

Status IndexBuilder::_write_snii_index_data(
        const TabletSchemaSPtr& tablet_schema, Block* block, const SniiIndexRewritePlan& plan,
        const std::vector<std::vector<std::pair<int64_t, int64_t>>>& group_writer_signs) {
    _olap_data_convertor->set_source_content(block, 0, block->rows());
    for (size_t group = 0; group < group_writer_signs.size(); ++group) {
        auto converted_result = _olap_data_convertor->convert_column_data(group);
        if (!converted_result.first.ok()) {
            LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }
        const TabletColumn& column = tablet_schema->column_by_uid(plan.build_columns[group].first);
        const auto* base_ptr = (const uint8_t*)converted_result.second->get_data();
        const auto* null_map = converted_result.second->get_nullmap();
        for (const auto& writer_sign : group_writer_signs[group]) {
            // _add_nullable/_add_data advance the value pointer as they consume
            // rows; every writer on the column starts from the SAME converted
            // data, which is exactly the shared-column-read guarantee.
            const uint8_t* ptr = base_ptr;
            if (null_map) {
                RETURN_IF_ERROR(_add_nullable(column.name(), writer_sign, &column, null_map, &ptr,
                                              block->rows()));
            } else {
                RETURN_IF_ERROR(
                        _add_data(column.name(), writer_sign, &column, &ptr, block->rows()));
            }
        }
    }
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

Status IndexBuilder::_write_inverted_index_data(TabletSchemaSPtr tablet_schema, int64_t segment_idx,
                                                Block* block) {
    VLOG_DEBUG << "begin to write inverted/ann index";
    // converter block data
    _olap_data_convertor->set_source_content(block, 0, block->rows());
    for (auto i = 0; i < _alter_inverted_indexes.size(); ++i) {
        auto inverted_index = _alter_inverted_indexes[i];
        auto index_id = inverted_index.index_id;
        auto column_name = inverted_index.columns[0];
        auto column_idx = tablet_schema->field_index(column_name);
        DBUG_EXECUTE_IF("IndexBuilder::_write_inverted_index_data_column_idx_is_negative",
                        { column_idx = -1; })
        if (column_idx < 0) {
            if (!inverted_index.column_unique_ids.empty()) {
                auto column_unique_id = inverted_index.column_unique_ids[0];
                column_idx = tablet_schema->field_index(column_unique_id);
            }
            if (column_idx < 0) {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_idx
                             << "]";
                continue;
            }
        }
        const auto& column = tablet_schema->column(column_idx);
        auto writer_sign = std::make_pair(segment_idx, index_id);
        auto converted_result = _olap_data_convertor->convert_column_data(i);
        DBUG_EXECUTE_IF("IndexBuilder::_write_inverted_index_data_convert_column_data_error", {
            converted_result.first = Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "debug point: _write_inverted_index_data_convert_column_data_error");
        })
        if (converted_result.first != Status::OK()) {
            LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }
        const auto* ptr = (const uint8_t*)converted_result.second->get_data();
        const auto* null_map = converted_result.second->get_nullmap();
        if (null_map) {
            RETURN_IF_ERROR(_add_nullable(column_name, writer_sign, &column, null_map, &ptr,
                                          block->rows()));
        } else {
            RETURN_IF_ERROR(_add_data(column_name, writer_sign, &column, &ptr, block->rows()));
        }
    }
    _olap_data_convertor->clear_source_content();

    return Status::OK();
}

Status IndexBuilder::_add_nullable(const std::string& column_name,
                                   const std::pair<int64_t, int64_t>& index_writer_sign,
                                   const TabletColumn* column, const uint8_t* null_map,
                                   const uint8_t** ptr, size_t num_rows) {
    auto index_column_writer_it = _index_column_writers.find(index_writer_sign);
    DORIS_CHECK(index_column_writer_it != _index_column_writers.end());
    DORIS_CHECK(index_column_writer_it->second != nullptr);
    auto* index_column_writer = index_column_writer_it->second.get();

    // TODO: need to process null data for inverted index
    if (column->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        DCHECK(column->get_subtype_count() == 1);
        // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
        const auto* data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
        // total number length
        auto offset_data = *(data_ptr + 1);
        const auto* offsets_ptr = (const uint8_t*)offset_data;
        try {
            auto data = *(data_ptr + 2);
            auto nested_null_map = *(data_ptr + 3);
            RETURN_IF_ERROR(index_column_writer->add_array_values(
                    field_type_size(column->get_sub_column(0).type()),
                    reinterpret_cast<const void*>(data),
                    reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, num_rows));
            DBUG_EXECUTE_IF("IndexBuilder::_add_nullable_add_array_values_error", {
                _CLTHROWA(CL_ERR_IO, "debug point: _add_nullable_add_array_values_error");
            })
            RETURN_IF_ERROR(index_column_writer->add_array_nulls(null_map, num_rows));
        } catch (const std::exception& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occurred: {}", e.what());
        }

        return Status::OK();
    }
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
    try {
        do {
            auto step = next_run_step();
            if (null_map[offset]) {
                RETURN_IF_ERROR(index_column_writer->add_nulls(static_cast<uint32_t>(step)));
            } else {
                RETURN_IF_ERROR(index_column_writer->add_values(column_name, *ptr, step));
            }
            *ptr += field_type_size(column->type()) * step;
            offset += step;
            DBUG_EXECUTE_IF("IndexBuilder::_add_nullable_throw_exception",
                            { _CLTHROWA(CL_ERR_IO, "debug point: _add_nullable_throw_exception"); })
        } while (offset < num_rows);
    } catch (const std::exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occurred: {}",
                                                                      e.what());
    }

    return Status::OK();
}

Status IndexBuilder::_add_data(const std::string& column_name,
                               const std::pair<int64_t, int64_t>& index_writer_sign,
                               const TabletColumn* column, const uint8_t** ptr, size_t num_rows) {
    auto index_column_writer_it = _index_column_writers.find(index_writer_sign);
    DORIS_CHECK(index_column_writer_it != _index_column_writers.end());
    DORIS_CHECK(index_column_writer_it->second != nullptr);
    auto* index_column_writer = index_column_writer_it->second.get();

    try {
        if (column->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            DCHECK(column->get_subtype_count() == 1);
            // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
            const auto* data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
            // total number length
            auto element_cnt = size_t((unsigned long)(*data_ptr));
            auto offset_data = *(data_ptr + 1);
            const auto* offsets_ptr = (const uint8_t*)offset_data;
            if (element_cnt > 0) {
                auto data = *(data_ptr + 2);
                auto nested_null_map = *(data_ptr + 3);
                RETURN_IF_ERROR(index_column_writer->add_array_values(
                        field_type_size(column->get_sub_column(0).type()),
                        reinterpret_cast<const void*>(data),
                        reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, num_rows));
            }
        } else {
            RETURN_IF_ERROR(index_column_writer->add_values(column_name, *ptr, num_rows));
        }
        DBUG_EXECUTE_IF("IndexBuilder::_add_data_throw_exception",
                        { _CLTHROWA(CL_ERR_IO, "debug point: _add_data_throw_exception"); })
    } catch (const std::exception& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("CLuceneError occurred: {}",
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
    DBUG_EXECUTE_IF("IndexBuilder::do_build_inverted_index_alter_inverted_indexes_empty",
                    { _alter_inverted_indexes.clear(); })
    if (_alter_inverted_indexes.empty()) {
        return Status::OK();
    }

    static constexpr long TRY_LOCK_TIMEOUT = 30;
    std::unique_lock schema_change_lock(_tablet->get_schema_change_lock(), std::defer_lock);
    bool owns_lock = schema_change_lock.try_lock_for(std::chrono::seconds(TRY_LOCK_TIMEOUT));

    if (!owns_lock) {
        return Status::ObtainLockFailed(
                "try schema_change_lock failed. There might be schema change or cooldown running "
                "on "
                "tablet={} ",
                _tablet->tablet_id());
    }
    // Check executing serially with compaction task.
    std::unique_lock<std::mutex> base_compaction_lock(_tablet->get_base_compaction_lock(),
                                                      std::try_to_lock);
    if (!base_compaction_lock.owns_lock()) {
        return Status::ObtainLockFailed("try base_compaction_lock failed. tablet={} ",
                                        _tablet->tablet_id());
    }
    std::unique_lock<std::mutex> cumu_compaction_lock(_tablet->get_cumulative_compaction_lock(),
                                                      std::try_to_lock);
    if (!cumu_compaction_lock.owns_lock()) {
        return Status::ObtainLockFailed("try cumu_compaction_lock failed. tablet={}",
                                        _tablet->tablet_id());
    }

    std::unique_lock<std::mutex> cold_compaction_lock(_tablet->get_cold_compaction_lock(),
                                                      std::try_to_lock);
    if (!cold_compaction_lock.owns_lock()) {
        return Status::ObtainLockFailed("try cold_compaction_lock failed. tablet={}",
                                        _tablet->tablet_id());
    }

    std::unique_lock<std::mutex> build_inverted_index_lock(_tablet->get_build_inverted_index_lock(),
                                                           std::try_to_lock);
    if (!build_inverted_index_lock.owns_lock()) {
        return Status::ObtainLockFailed("failed to obtain build inverted index lock. tablet={}",
                                        _tablet->tablet_id());
    }

    std::shared_lock migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
    if (!migration_rlock.owns_lock()) {
        return Status::ObtainLockFailed("got migration_rlock failed. tablet={}",
                                        _tablet->tablet_id());
    }

    DCHECK(!_alter_index_ids.empty());
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
    DBUG_EXECUTE_IF("IndexBuilder::do_build_inverted_index_modify_rowsets_status_error", {
        st = Status::Error<ErrorCode::DELETE_VERSION_ERROR>(
                "debug point: do_build_inverted_index_modify_rowsets_status_error");
    })
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
        std::lock_guard meta_wlock(_tablet->get_header_lock());
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
        std::lock_guard wrlock(_tablet->get_header_lock());
        RETURN_IF_ERROR(_tablet->modify_rowsets(_output_rowsets, _input_rowsets, true));
    }

#ifndef BE_TEST
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _tablet->save_meta();
    }
#endif
    return Status::OK();
}

void IndexBuilder::gc_output_rowset() {
    for (auto&& output_rowset : _output_rowsets) {
        auto is_local_rowset = output_rowset->is_local();
        DBUG_EXECUTE_IF("IndexBuilder::gc_output_rowset_is_local_rowset",
                        { is_local_rowset = false; })
        if (!is_local_rowset) {
            _tablet->record_unused_remote_rowset(output_rowset->rowset_id(),
                                                 output_rowset->rowset_meta()->resource_id(),
                                                 output_rowset->num_segments());
            return;
        }
        _engine.add_unused_rowset(output_rowset);
    }
}

} // namespace doris
