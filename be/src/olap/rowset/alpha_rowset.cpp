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

#include "olap/row.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/alpha_rowset_reader.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "util/hash_util.hpp"
#include <util/file_utils.h>

namespace doris {

AlphaRowset::AlphaRowset(const TabletSchema* schema, const FilePathDesc& rowset_path_desc,
                         RowsetMetaSharedPtr rowset_meta)
        : Rowset(schema, rowset_path_desc, std::move(rowset_meta)) {}

OLAPStatus AlphaRowset::do_load(bool use_cache) {
    for (auto& segment_group : _segment_groups) {
        // validate segment group
        if (segment_group->validate() != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to validate segment_group. [version=" << start_version() << "-"
                         << end_version();
            // if load segment group failed, rowset init failed
            return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
        }
        OLAPStatus res = segment_group->load(use_cache);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load segment_group. res=" << res << ", "
                         << "version=" << start_version() << "-" << end_version();
            return res;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::create_reader(std::shared_ptr<RowsetReader>* result) {
    result->reset(new AlphaRowsetReader(_schema->num_rows_per_row_block(),
                                        std::static_pointer_cast<AlphaRowset>(shared_from_this())));
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::create_reader(const std::shared_ptr<MemTracker>& parent_tracker,
                                      std::shared_ptr<RowsetReader>* result) {
    result->reset(new AlphaRowsetReader(_schema->num_rows_per_row_block(),
                                        std::static_pointer_cast<AlphaRowset>(shared_from_this()),
                                        parent_tracker));
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::remove() {
    VLOG_NOTICE << "begin to remove files in rowset " << unique_id() << ", version:" << start_version()
            << "-" << end_version() << ", tabletid:" << _rowset_meta->tablet_id();
    for (auto segment_group : _segment_groups) {
        bool ret = segment_group->delete_all_files();
        if (!ret) {
            LOG(WARNING) << "delete segment group files failed."
                         << " tablet id:" << segment_group->get_tablet_id()
                         << ", rowset path:" << segment_group->rowset_path_prefix();
            return OLAP_ERR_ROWSET_DELETE_FILE_FAILED;
        }
    }
    return OLAP_SUCCESS;
}

void AlphaRowset::make_visible_extra(Version version) {
    AlphaRowsetMetaSharedPtr alpha_rowset_meta =
            std::dynamic_pointer_cast<AlphaRowsetMeta>(_rowset_meta);
    std::vector<SegmentGroupPB> published_segment_groups;
    alpha_rowset_meta->get_segment_groups(&published_segment_groups);
    int32_t segment_group_idx = 0;
    for (auto& segment_group : _segment_groups) {
        segment_group->set_version(version);
        segment_group->set_pending_finished();
        published_segment_groups.at(segment_group_idx).clear_load_id();
        ++segment_group_idx;
    }
    alpha_rowset_meta->clear_segment_group();
    for (auto& segment_group_meta : published_segment_groups) {
        alpha_rowset_meta->add_segment_group(segment_group_meta);
    }
}

OLAPStatus AlphaRowset::link_files_to(const FilePathDesc& dir_desc, RowsetId new_rowset_id) {
    for (auto& segment_group : _segment_groups) {
        auto status = segment_group->link_segments_to_path(dir_desc.filepath, new_rowset_id);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "create hard links failed for segment group:"
                         << segment_group->segment_group_id();
            return status;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::copy_files_to(const std::string& dir) {
    for (auto& segment_group : _segment_groups) {
        OLAPStatus status = segment_group->copy_files_to(dir);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "copy files failed for segment group."
                         << " segment_group_id:" << segment_group->segment_group_id()
                         << ", dest_path:" << dir;
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

OLAPStatus AlphaRowset::split_range(const RowCursor& start_key, const RowCursor& end_key,
                                    uint64_t request_block_row_count, size_t key_num,
                                    std::vector<OlapTuple>* ranges) {
    if (key_num > _schema->num_short_key_columns()) {
        // should not happen
        // But since aloha rowset is deprecated in future and it will not fail the query,
        // just use VLOG to avoid too many warning logs.
        VLOG_NOTICE << "key num " << key_num << " should less than or equal to short key column number: "
                << _schema->num_short_key_columns();
        return OLAP_ERR_INVALID_SCHEMA;
    }
    EntrySlice entry;
    RowBlockPosition start_pos;
    RowBlockPosition end_pos;
    RowBlockPosition step_pos;

    std::shared_ptr<SegmentGroup> largest_segment_group = _segment_group_with_largest_size();
    if (largest_segment_group == nullptr ||
        largest_segment_group->current_num_rows_per_row_block() == 0) {
        VLOG_NOTICE << "failed to get largest_segment_group. is null: "
                     << (largest_segment_group == nullptr) << ". version: " << start_version()
                     << "-" << end_version() << ". tablet: " << rowset_meta()->tablet_id();
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
        return OLAP_SUCCESS;
    }
    uint64_t expected_rows =
            request_block_row_count / largest_segment_group->current_num_rows_per_row_block();
    if (expected_rows == 0) {
        LOG(WARNING) << "expected_rows less than 1. [request_block_row_count = "
                     << request_block_row_count << "]";
        return OLAP_ERR_INVALID_SCHEMA;
    }

    // find the start position of start key
    RowCursor helper_cursor;
    if (helper_cursor.init(*_schema, key_num) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to parse strings to key with RowCursor type.";
        return OLAP_ERR_INVALID_SCHEMA;
    }
    if (largest_segment_group->find_short_key(start_key, &helper_cursor, false, &start_pos) !=
        OLAP_SUCCESS) {
        if (largest_segment_group->find_first_row_block(&start_pos) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to get first block pos";
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    step_pos = start_pos;
    VLOG_NOTICE << "start_pos=" << start_pos.segment << ", " << start_pos.index_offset;

    //find last row_block is end_key is given, or using last_row_block
    if (largest_segment_group->find_short_key(end_key, &helper_cursor, false, &end_pos) !=
        OLAP_SUCCESS) {
        if (largest_segment_group->find_last_row_block(&end_pos) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail find last row block.";
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    VLOG_NOTICE << "end_pos=" << end_pos.segment << ", " << end_pos.index_offset;

    //get rows between first and last
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor cur_start_key;
    RowCursor last_start_key;

    if (cur_start_key.init(*_schema, key_num) != OLAP_SUCCESS ||
        last_start_key.init(*_schema, key_num) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init cursor";
        return OLAP_ERR_INIT_FAILED;
    }

    std::vector<uint32_t> cids;
    for (uint32_t cid = 0; cid < key_num; ++cid) {
        cids.push_back(cid);
    }

    if (largest_segment_group->get_row_block_entry(start_pos, &entry) != OLAP_SUCCESS) {
        LOG(WARNING) << "get block entry failed.";
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    cur_start_key.attach(entry.data);
    last_start_key.allocate_memory_for_string_type(*_schema);
    direct_copy_row(&last_start_key, cur_start_key);
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

        if (!equal_row(cids, cur_start_key, last_start_key)) {
            ranges->emplace_back(cur_start_key.to_tuple()); // end of last section
            ranges->emplace_back(cur_start_key.to_tuple()); // start a new section
            direct_copy_row(&last_start_key, cur_start_key);
        }
    }

    ranges->emplace_back(end_key.to_tuple());
    return OLAP_SUCCESS;
}

bool AlphaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (auto& segment_group : _segment_groups) {
        for (int i = 0; i < segment_group->num_segments(); ++i) {
            std::string data_path = segment_group->construct_data_file_path(i);
            std::string index_path = segment_group->construct_index_file_path(i);
            valid_paths.insert(data_path);
            valid_paths.insert(index_path);
        }
    }
    return valid_paths.find(path) != valid_paths.end();
}

bool AlphaRowset::check_file_exist() {
    for (auto& segment_group : _segment_groups) {
        for (int i = 0; i < segment_group->num_segments(); ++i) {
            std::string data_path = segment_group->construct_data_file_path(i);
            if (!FileUtils::check_exist(data_path)) {
                LOG(WARNING) << "data file not existed: " << data_path << " for rowset_id: " << rowset_id();
                return false;
            }
            std::string index_path = segment_group->construct_index_file_path(i);
            if (!FileUtils::check_exist(index_path)) {
                LOG(WARNING) << "index file not existed: " << index_path << " for rowset_id: " << rowset_id();
                return false;
            }
        }
    }
    return true;
}

OLAPStatus AlphaRowset::init() {
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMetaSharedPtr _alpha_rowset_meta =
            std::dynamic_pointer_cast<AlphaRowsetMeta>(_rowset_meta);
    _alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    for (auto& segment_group_meta : segment_group_metas) {
        std::shared_ptr<SegmentGroup> segment_group;
        if (_is_pending) {
            segment_group.reset(new SegmentGroup(
                    _rowset_meta->tablet_id(), _rowset_meta->rowset_id(), _schema, _rowset_path_desc.filepath,
                    false, segment_group_meta.segment_group_id(), segment_group_meta.num_segments(),
                    true, _rowset_meta->partition_id(), _rowset_meta->txn_id()));
        } else {
            segment_group.reset(new SegmentGroup(
                    _rowset_meta->tablet_id(), _rowset_meta->rowset_id(), _schema, _rowset_path_desc.filepath,
                    _rowset_meta->version(), false,
                    segment_group_meta.segment_group_id(), segment_group_meta.num_segments()));
        }
        if (segment_group == nullptr) {
            LOG(WARNING) << "fail to create olap segment_group. rowset_id='"
                         << _rowset_meta->rowset_id();
            return OLAP_ERR_CREATE_FILE_ERROR;
        }
        if (segment_group_meta.has_empty()) {
            segment_group->set_empty(segment_group_meta.empty());
        }

        if (segment_group_meta.zone_maps_size() != 0) {
            size_t zone_maps_size = segment_group_meta.zone_maps_size();
            // after 0.12.10 the value column in duplicate table also has zone map.
            // after 0.14 the value column in duplicate table also has zone map.
            size_t expect_zone_maps_num = _schema->keys_type() != KeysType::AGG_KEYS
                                                  ? _schema->num_columns()
                                                  : _schema->num_key_columns();
            if ((_schema->keys_type() == KeysType::AGG_KEYS &&
                 expect_zone_maps_num != zone_maps_size) ||
                (_schema->keys_type() != KeysType::AGG_KEYS &&
                 expect_zone_maps_num < zone_maps_size)) {
                LOG(ERROR) << "column pruning size is error. "
                           << "KeysType=" << KeysType_Name(_schema->keys_type()) << ", "
                           << "zone_maps_size=" << zone_maps_size << ", "
                           << "num_key_columns=" << _schema->num_key_columns() << ", "
                           << "num_columns=" << _schema->num_columns();
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
            }
            // Before 0.12.10, the zone map columns number in duplicate/unique table is the same with the key column numbers,
            // but after 0.12.10 we build zone map for duplicate table value column, after 0.14 we build zone map for unique
            // table value column, so when first start the two number is not the same,
            // it causes start failed. When `expect_zone_maps_num > zone_maps_size` it may be the first start after upgrade
            if (expect_zone_maps_num > zone_maps_size) {
                VLOG_CRITICAL << "tablet: " << _rowset_meta->tablet_id() << " expect zone map size is "
                        << expect_zone_maps_num << ", actual num is " << zone_maps_size
                        << ". If this is not the first start after upgrade, please pay attention!";
            }
            zone_maps_size = std::min(zone_maps_size, expect_zone_maps_num);
            std::vector<std::pair<std::string, std::string>> zone_map_strings(zone_maps_size);
            std::vector<bool> null_vec(zone_maps_size);
            for (size_t j = 0; j < zone_maps_size; ++j) {
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
    if (_is_cumulative && _segment_groups.size() > 1) {
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
        if (!segment_group->index_loaded()) {
            continue;
        }
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

} // namespace doris
