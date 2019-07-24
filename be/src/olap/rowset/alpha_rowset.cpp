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

#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "util/hash_util.hpp"

namespace doris {

AlphaRowset::AlphaRowset(const TabletSchema* schema,
                         const std::string rowset_path,
                         DataDir* data_dir,
                         RowsetMetaSharedPtr rowset_meta)
      : _schema(schema),
        _rowset_path(rowset_path),
        _data_dir(data_dir),
        _rowset_meta(rowset_meta),
        _is_cumulative_rowset(false),
        _is_pending_rowset(false) {
    if (!_rowset_meta->has_version()) {
        _is_pending_rowset = true;
    }
    if (!_is_pending_rowset) {
        Version version = _rowset_meta->version();
        if (version.first == version.second) {
            _is_cumulative_rowset = false;
        } else {
            _is_cumulative_rowset = true;
        }
    }
}

OLAPStatus AlphaRowset::init() {
    if (is_inited()) {
        return OLAP_SUCCESS;
    }
    OLAPStatus status = _init_segment_groups();
    set_inited(true);
    return status;
}

OLAPStatus AlphaRowset::load(bool use_cache) {
    // load is depend on init, so that check if init here and do init if not
    if (!is_inited()) {
        OLAPStatus res = init();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to init rowset before load"
                         << " rowset id " << rowset_id();
            return res;
        }
    }
    if (is_loaded()) {
        return OLAP_SUCCESS;
    }
    for (auto& segment_group: _segment_groups) {
        // validate segment group
        if (segment_group->validate() != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to validate segment_group. [version="<< start_version()
                    << "-" << end_version() << " version_hash=" << version_hash();
                // if load segment group failed, rowset init failed
            return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
        }
        OLAPStatus res = segment_group->load(use_cache);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load segment_group. res=" << res << ", "
                    << "version=" << start_version() << "-"
                    << end_version() << ", "
                    << "version_hash=" << version_hash();
            return res;
        }
    }
    set_loaded(true);
    return OLAP_SUCCESS;
}

std::shared_ptr<RowsetReader> AlphaRowset::create_reader() {
    if (!is_loaded()) {
        OLAPStatus status = load();
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "alpha rowset load failed. rowset path:" << _rowset_path;
            return nullptr;
        }
        set_loaded(true);
    }
    return std::shared_ptr<RowsetReader>(new AlphaRowsetReader(
            _schema->num_rows_per_row_block(), shared_from_this()));
}

OLAPStatus AlphaRowset::remove() {
    LOG(INFO) << "begin to remove rowset: " << rowset_id();
    for (auto segment_group : _segment_groups) {
        bool ret = segment_group->delete_all_files();
        if (!ret) {
            LOG(WARNING) << "delete segment group files failed."
                         << " tablet id:" << segment_group->get_tablet_id()
                         << ", rowset path:" << segment_group->rowset_path_prefix();
            return OLAP_ERR_ROWSET_DELETE_SEGMENT_GROUP_FILE_FAILED;
        }
    }
    return OLAP_SUCCESS;
}

RowsetMetaSharedPtr AlphaRowset::rowset_meta() const {
    return _rowset_meta;
}

void AlphaRowset::set_version_and_version_hash(Version version,  VersionHash version_hash) {
    _rowset_meta->set_version(version);
    _rowset_meta->set_version_hash(version_hash);
    // set the rowset state to VISIBLE
    _rowset_meta->set_rowset_state(VISIBLE);

    if (rowset_meta()->has_delete_predicate()) {
        rowset_meta()->mutable_delete_predicate()->set_version(version.first);
        return;
    }

    AlphaRowsetMetaSharedPtr alpha_rowset_meta =
            std::dynamic_pointer_cast<AlphaRowsetMeta>(_rowset_meta);
    vector<SegmentGroupPB> published_segment_groups;
    alpha_rowset_meta->get_segment_groups(&published_segment_groups);
    int32_t segment_group_idx = 0;
    for (auto segment_group : _segment_groups) {
        segment_group->set_version(version);
        segment_group->set_version_hash(version_hash);
        segment_group->set_pending_finished();
        published_segment_groups.at(segment_group_idx).clear_load_id();
        ++segment_group_idx;
    }
    alpha_rowset_meta->clear_segment_group();
    for (auto& segment_group_meta : published_segment_groups) {
        alpha_rowset_meta->add_segment_group(segment_group_meta);
    }

    _is_pending_rowset = false;
}

OLAPStatus AlphaRowset::make_snapshot(const std::string& snapshot_path,
                                      std::vector<std::string>* success_links) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->make_snapshot(snapshot_path, success_links);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "create hard links failed for segment group:"
                         << segment_group->segment_group_id();
            return status;
        }
    }
    return OLAP_SUCCESS;
}
                                    
OLAPStatus AlphaRowset::copy_files_to_path(const std::string& dest_path,
                                           std::vector<std::string>* success_files) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->copy_files_to_path(dest_path, success_files);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "copy files failed for segment group."
                         << " segment_group_id:" << segment_group->segment_group_id()
                         << ", dest_path:" << dest_path;
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::convert_from_old_files(const std::string& snapshot_path,
                                      std::vector<std::string>* success_files) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->convert_from_old_files(snapshot_path, success_files);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "create hard links failed for segment group:"
                         << segment_group->segment_group_id();
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::convert_to_old_files(const std::string& snapshot_path, 
                                std::vector<std::string>* success_files) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->convert_to_old_files(snapshot_path, success_files);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "create hard links failed for segment group:"
                         << segment_group->segment_group_id();
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::remove_old_files(std::vector<std::string>* files_to_remove) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->remove_old_files(files_to_remove);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "remove old files failed for segment group:"
                         << segment_group->segment_group_id();
            return status;
        }
    }
    return OLAP_SUCCESS;
}

bool AlphaRowset::is_pending() const {
    return _is_pending_rowset;
}

OLAPStatus AlphaRowset::split_range(
            const RowCursor& start_key,
            const RowCursor& end_key,
            uint64_t request_block_row_count,
            vector<OlapTuple>* ranges) {
    EntrySlice entry;
    RowBlockPosition start_pos;
    RowBlockPosition end_pos;
    RowBlockPosition step_pos;

    std::shared_ptr<SegmentGroup> largest_segment_group = _segment_group_with_largest_size();
    if (largest_segment_group == nullptr) {
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
        return OLAP_SUCCESS;
    }
    uint64_t expected_rows = request_block_row_count
            / largest_segment_group->current_num_rows_per_row_block();
    if (expected_rows == 0) {
        LOG(WARNING) << "expected_rows less than 1. [request_block_row_count = "
                     << request_block_row_count << "]";
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 找到startkey对应的起始位置
    RowCursor helper_cursor;
    if (helper_cursor.init(*_schema, _schema->num_short_key_columns()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to parse strings to key with RowCursor type.";
        return OLAP_ERR_INVALID_SCHEMA;
    }
    if (largest_segment_group->find_short_key(start_key, &helper_cursor, false, &start_pos) != OLAP_SUCCESS) {
        if (largest_segment_group->find_first_row_block(&start_pos) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to get first block pos";
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    step_pos = start_pos;
    VLOG(3) << "start_pos=" << start_pos.segment << ", " << start_pos.index_offset;

    //find last row_block is end_key is given, or using last_row_block
    if (largest_segment_group->find_short_key(end_key, &helper_cursor, false, &end_pos) != OLAP_SUCCESS) {
        if (largest_segment_group->find_last_row_block(&end_pos) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail find last row block.";
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    VLOG(3) << "end_pos=" << end_pos.segment << ", " << end_pos.index_offset;

    //get rows between first and last
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor cur_start_key;
    RowCursor last_start_key;

    if (cur_start_key.init(*_schema, _schema->num_short_key_columns()) != OLAP_SUCCESS
            || last_start_key.init(*_schema, _schema->num_short_key_columns()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init cursor";
        return OLAP_ERR_INIT_FAILED;
    }

    if (largest_segment_group->get_row_block_entry(start_pos, &entry) != OLAP_SUCCESS) {
        LOG(WARNING) << "get block entry failed.";
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    cur_start_key.attach(entry.data);
    last_start_key.allocate_memory_for_string_type(*_schema);
    last_start_key.copy_without_pool(cur_start_key);
    // start_key是last start_key, 但返回的实际上是查询层给出的key
    ranges->emplace_back(start_key.to_tuple());

    while (end_pos > step_pos) {
        res = largest_segment_group->advance_row_block(expected_rows, &step_pos);
        if (res == OLAP_ERR_INDEX_EOF || !(end_pos > step_pos)) {
            break;
        } else if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "advance_row_block failed.";
            return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
        }

        if (largest_segment_group->get_row_block_entry(step_pos, &entry) != OLAP_SUCCESS) {
            LOG(WARNING) << "get block entry failed.";
            return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
        }
        cur_start_key.attach(entry.data);

        if (cur_start_key.cmp(last_start_key) != 0) {
            ranges->emplace_back(cur_start_key.to_tuple()); // end of last section
            ranges->emplace_back(cur_start_key.to_tuple()); // start a new section
            last_start_key.copy_without_pool(cur_start_key);
        }
    }

    ranges->emplace_back(end_key.to_tuple());
    return OLAP_SUCCESS;
}

bool AlphaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (auto segment_group : _segment_groups) {
        for (int i = 0; i < segment_group->num_segments(); ++i) {
            std::string data_path = segment_group->construct_data_file_path(i);
            std::string index_path = segment_group->construct_index_file_path(i);
            valid_paths.insert(data_path);
            valid_paths.insert(index_path);
        }
    }
    return valid_paths.find(path) != valid_paths.end();
}

OLAPStatus AlphaRowset::_init_segment_groups() {
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMetaSharedPtr _alpha_rowset_meta = std::dynamic_pointer_cast<AlphaRowsetMeta>(_rowset_meta);
    _alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    for (auto& segment_group_meta : segment_group_metas) {
        std::shared_ptr<SegmentGroup> segment_group;
        if (_is_pending_rowset) {
            segment_group.reset(new SegmentGroup(_rowset_meta->tablet_id(),
                    _rowset_meta->rowset_id(), _schema, _rowset_path, false, segment_group_meta.segment_group_id(),
                    segment_group_meta.num_segments(), true, 
                    _rowset_meta->partition_id(), _rowset_meta->txn_id()));
        } else {
            segment_group.reset(new SegmentGroup(_rowset_meta->tablet_id(),
                _rowset_meta->rowset_id(), _schema, _rowset_path, 
                _rowset_meta->version(), _rowset_meta->version_hash(),
                false, segment_group_meta.segment_group_id(), segment_group_meta.num_segments()));
        }
        if (segment_group == nullptr) {
            LOG(WARNING) << "fail to create olap segment_group. rowset_id='" << _rowset_meta->rowset_id();
            return OLAP_ERR_CREATE_FILE_ERROR;
        }
        if (segment_group_meta.has_empty()) {
            segment_group->set_empty(segment_group_meta.empty());
        }

        if (segment_group_meta.zone_maps_size() != 0) {
            size_t zone_maps_size = segment_group_meta.zone_maps_size();
            size_t num_key_columns = _schema->num_key_columns();
            if (num_key_columns != zone_maps_size) {
                LOG(ERROR) << "column pruning size is error."
                        << "zone_maps_size=" << zone_maps_size << ", "
                        << "num_key_columns=" << _schema->num_key_columns();
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
            }
            std::vector<std::pair<std::string, std::string>> zone_map_strings(num_key_columns);
            std::vector<bool> null_vec(num_key_columns);
            for (size_t j = 0; j < num_key_columns; ++j) {
                const ZoneMap& zone_map = segment_group_meta.zone_maps(j);
                zone_map_strings[j].first = zone_map.min();
                zone_map_strings[j].second = zone_map.max();
                if (zone_map.has_null_flag()) {
                    null_vec[j] = zone_map.null_flag();
                } else {
                    null_vec[j] = false;
                }
            }
            OLAPStatus status = segment_group->add_zone_maps(zone_map_strings, null_vec);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "segment group add column statistics failed, status:" << status;
                return status;
            }
        }
        _segment_groups.push_back(segment_group);
    }
    if (_is_cumulative_rowset && _segment_groups.size() > 1) {
        LOG(WARNING) << "invalid segment group meta for cumulative rowset. segment group size:"
                << _segment_groups.size();
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }
    return OLAP_SUCCESS;
}

std::shared_ptr<SegmentGroup> AlphaRowset::_segment_group_with_largest_size() {
    std::shared_ptr<SegmentGroup> largest_segment_group = nullptr;
    size_t largest_segment_group_sizes = 0;

    for (auto segment_group : _segment_groups) {
        if (segment_group->empty() || segment_group->zero_num_rows()) {
            continue;
        }
        if (segment_group->index_size() > largest_segment_group_sizes) {
            largest_segment_group = segment_group;
            largest_segment_group_sizes = segment_group->index_size();
        }
    }
    return largest_segment_group;
}

OLAPStatus AlphaRowset::reset_sizeinfo() {
    if (!is_loaded()) {
        RETURN_NOT_OK(load());
    }
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMetaSharedPtr alpha_rowset_meta = std::dynamic_pointer_cast<AlphaRowsetMeta>(_rowset_meta);
    alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    int32_t segment_group_idx = 0;
    for (auto segment_group : _segment_groups) {
        alpha_rowset_meta->set_data_disk_size(alpha_rowset_meta->data_disk_size() + segment_group->data_size());
        alpha_rowset_meta->set_index_disk_size(alpha_rowset_meta->index_disk_size() + segment_group->index_size());
        alpha_rowset_meta->set_total_disk_size(alpha_rowset_meta->total_disk_size()
                + segment_group->index_size() + segment_group->data_size());
        alpha_rowset_meta->set_num_rows(alpha_rowset_meta->num_rows() + segment_group->num_rows());
        segment_group_metas.at(segment_group_idx).set_index_size(segment_group->index_size());
        segment_group_metas.at(segment_group_idx).set_data_size(segment_group->data_size());
        segment_group_metas.at(segment_group_idx).set_num_rows(segment_group->num_rows());
        ++segment_group_idx;
    }
    alpha_rowset_meta->clear_segment_group();
    for (auto& segment_group_meta : segment_group_metas) {
        alpha_rowset_meta->add_segment_group(segment_group_meta);
    }
    return OLAP_SUCCESS;
}

std::string AlphaRowset::unique_id() {
    // rowset path + rowset_id is unique for a rowset
    return _rowset_path + "/" + std::to_string(rowset_id());
}

}  // namespace doris
