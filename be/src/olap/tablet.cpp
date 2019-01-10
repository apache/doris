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

#include "olap/tablet.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>

#include <algorithm>
#include <map>
#include <set>

#include <boost/filesystem.hpp>

#include "olap/field.h"
#include "olap/rowset/column_data.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/olap_index.h"
#include "olap/reader.h"
#include "olap/data_dir.h"
#include "olap/row_cursor.h"
#include "util/defer_op.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/column_data.h"
#include "olap/rowset/segment_group.h"

using std::pair;
using std::map;
using std::nothrow;
using std::set;
using std::sort;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

Tablet::Tablet(TabletMeta* tablet_meta, DataDir* data_dir) {
    _data_dir = data_dir;
    _rs_graph->construct_rowset_graph(tablet_meta->all_rs_metas());
}

Tablet::~Tablet() {
    // ensure that there is nobody using Tablet, like acquiring OLAPData(SegmentGroup)
    WriteLock wrlock(&_meta_lock);
    _rs_version_map.clear();
}

OLAPStatus Tablet::init_once() {
    ReadLock rdlock(&_meta_lock);
    for (auto& it : _tablet_meta.all_rs_metas()) {
        Version version = it->version();
        RowsetSharedPtr rowset(Rowset::create());
        rowset->init();
        _rs_version_map[version] = rowset;
        rowset->init();
    }
    return OLAP_SUCCESS;
}

bool Tablet::can_do_compaction() {
    // 如果table正在做schema change，则通过选路判断数据是否转换完成
    // 如果选路成功，则转换完成，可以进行BE
    // 如果选路失败，则转换未完成，不能进行BE
    ReadLock rdlock(&_meta_lock);
    const PDelta* lastest_delta = lastest_version();
    if (lastest_delta == NULL) {
        return false;
    }

    Version test_version = Version(0, lastest_delta->end_version());
    vector<Version> path_versions;
    if (OLAP_SUCCESS != _rs_graph->capture_consistent_versions(test_version, &path_versions)) {
        LOG(WARNING) << "tablet has missed version. tablet=" << full_name();
        return false;
    }

    if (this->is_schema_changing()) {
        Version test_version = Version(0, lastest_delta->end_version());
        vector<Version> path_versions;
        if (OLAP_SUCCESS != _rs_graph->capture_consistent_versions(test_version, &path_versions)) {
            return false;
        }
    }

    return true;
}

OLAPStatus Tablet::compute_all_versions_hash(const vector<Version>& versions,
                                             VersionHash* version_hash) const {
    DCHECK(version_hash != nullptr) << "invalid parameter, version_hash is nullptr";

    int64_t v_hash  = 0L;
    for (auto version : versions) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset. "
                << "version=" << version.first << "-" << version.second;
            return OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR;
        }
        v_hash ^= it->second->version_hash();
    }
    *version_hash = v_hash;
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_consistent_rowsets(const Version& spec_version,
                                              vector<RowsetSharedPtr>* rowsets) const {
    vector<Version> version_path;
    _rs_graph->capture_consistent_versions(spec_version, &version_path);

    capture_consistent_rowsets(version_path, rowsets);
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_consistent_rowsets(const vector<Version>& version_path,
                                        vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr && rowsets->empty());
    for (auto& version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name()
                         << ", version='" << version.first << "-" << version.second;
            release_rowsets(rowsets);
            return OLAP_SUCCESS;
        }

        rowsets->push_back(it->second);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::release_rowsets(vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr) << "rowsets is null. tablet=" << full_name();
    rowsets->clear();
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_rs_readers(const Version& spec_version,
                                      vector<RowsetReaderSharedPtr>* rs_readers) const {
    vector<Version> version_path;
    _rs_graph->capture_consistent_versions(spec_version, &version_path);

    capture_rs_readers(version_path, rs_readers);
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_rs_readers(const vector<Version>& version_path,
                                vector<RowsetReaderSharedPtr>* rs_readers) const {
    DCHECK(rs_readers != NULL && rs_readers->empty());
    for (auto version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            LOG(WARNING) << "fail to find Rowset for version. tablet=" << full_name()
                         << ", version='" << version.first << "-" << version.second;
            release_rs_readers(rs_readers);
            return OLAP_SUCCESS;
        }

        std::shared_ptr<RowsetReader> rs_reader(it->second->create_reader());
        rs_readers->push_back(rs_reader);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::release_rs_readers(vector<RowsetReaderSharedPtr>* rs_readers) const {
    DCHECK(rs_readers != nullptr) << "rs_readers is null. tablet=" << full_name();
    rs_readers->clear();
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::capture_consistent_versions(
                        const Version& version, vector<Version>* span_versions) const {
    OLAPStatus status = _rs_graph->capture_consistent_versions(version, span_versions);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to generate shortest version path. tablet=" << full_name()
                     << ", version='" << version.first << "-" << version.second;
    }
    return status;
}

OLAPStatus Tablet::add_inc_rowset(const Rowset& rowset) {
    return _tablet_meta.add_inc_rs_meta(rowset.rowset_meta());
}

OLAPStatus Tablet::delete_expired_inc_rowset() {
    time_t now = time(NULL);
    vector<Version> expired_versions;
    WriteLock wrlock(&_meta_lock);
    for (auto& it : _tablet_meta.all_inc_rs_metas()) {
        double diff = difftime(now, it->creation_time());
        if (diff >= config::inc_rowset_expired_sec) {
            expired_versions.push_back(it->version());
        }
    }
    for (auto& it : expired_versions) {
        delete_inc_rowset_by_version(it);
        VLOG(3) << "delete expired inc_rowset. tablet=" << full_name()
                << ", version=" << it.first << "-" << it.second;
    }

    if (_tablet_meta.save_meta() != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet_meta when delete expired inc_rowset. "
                   << "tablet=" << full_name();
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::delete_inc_rowset_by_version(const Version& version) {
    _tablet_meta.delete_inc_rs_meta_by_version(version);
    VLOG(3) << "delete inc rowset. tablet=" << full_name()
            << ", version=" << version.first << "-" << version.second;
    return OLAP_SUCCESS;
}

void Tablet::calc_missed_versions(int64_t spec_version,
                                 vector<Version>* missed_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;
    std::list<Version> existing_versions;
    for (auto& rs : _tablet_meta.all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    existing_versions.sort([](const Version& a, const Version& b) {
        // simple because 2 versions are certainly not overlapping
        return a.first < b.first;
    });

    // find the missing version until spec_version
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first; ++i) {
                missed_versions->emplace_back(i, i);
            }
        }
        last_version = version.second;
        if (spec_version <= last_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions->emplace_back(i, i);
    }
}

OLAPStatus Tablet::modify_rowsets(std::vector<Version>* old_version,
                                  vector<RowsetSharedPtr>* to_add,
                                  vector<RowsetSharedPtr>* to_delete) {
    return OLAP_SUCCESS;
}

RowsetSharedPtr Tablet::rowset_with_largest_size() {
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        // use segment_group of base file as target segment_group when base is not empty,
        // or try to find the biggest segment_group.
        if (largest_rowset->empty() || largest_rowset->zero_num_rows()) {
            continue;
        }
        if (it.second->rowset_meta()->index_disk_size()
                > largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

OLAPStatus Tablet::split_range(
        const OlapTuple& start_key_strings,
        const OlapTuple& end_key_strings,
        uint64_t request_block_row_count,
        vector<OlapTuple>* ranges) {
    if (ranges == NULL) {
        OLAP_LOG_WARNING("parameter end_row is null.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    EntrySlice entry;
    RowCursor start_key;
    RowCursor end_key;
    RowCursor helper_cursor;
    RowBlockPosition start_pos;
    RowBlockPosition end_pos;
    RowBlockPosition step_pos;

    // 此helper用于辅助查找，注意它的内容不能拿来使用，是不可预知的，仅作为辅助使用
    if (helper_cursor.init(tablet_schema(), num_short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
        return OLAP_ERR_INVALID_SCHEMA;
    }

    // 如果有startkey，用startkey初始化；反之则用minkey初始化
    if (start_key_strings.size() > 0) {
        if (start_key.init_scan_key(tablet_schema(), start_key_strings.values()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        if (start_key.from_tuple(start_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (start_key.init(tablet_schema(), num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        start_key.allocate_memory_for_string_type(tablet_schema());
        start_key.build_min_key();
    }

    // 和startkey一样处理，没有则用maxkey初始化
    if (end_key_strings.size() > 0) {
        if (OLAP_SUCCESS != end_key.init_scan_key(tablet_schema(), end_key_strings.values())) {
            OLAP_LOG_WARNING("fail to parse strings to key with RowCursor type.");
            return OLAP_ERR_INVALID_SCHEMA;
        }

        if (end_key.from_tuple(end_key_strings) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("init end key failed");
            return OLAP_ERR_INVALID_SCHEMA;
        }
    } else {
        if (end_key.init(tablet_schema(), num_short_key_fields()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to initial key strings with RowCursor type.");
            return OLAP_ERR_INIT_FAILED;
        }

        end_key.allocate_memory_for_string_type(tablet_schema());
        end_key.build_max_key();
    }

    ReadLock rdlock(get_header_lock_ptr());
    SegmentGroup* base_index = get_largest_index();

    // 如果找不到合适的segment_group，就直接返回startkey，endkey
    if (base_index == NULL) {
        VLOG(3) << "there is no base file now, may be tablet is empty.";
        // it may be right if the tablet is empty, so we return success.
        ranges->emplace_back(start_key.to_tuple());
        ranges->emplace_back(end_key.to_tuple());
        return OLAP_SUCCESS;
    }

    uint64_t expected_rows = request_block_row_count
            / base_index->current_num_rows_per_row_block();
    if (expected_rows == 0) {
        OLAP_LOG_WARNING("expected_rows less than 1. [request_block_row_count = '%d']",
                         request_block_row_count);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 找到startkey对应的起始位置
    if (base_index->find_short_key(start_key, &helper_cursor, false, &start_pos) != OLAP_SUCCESS) {
        if (base_index->find_first_row_block(&start_pos) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get first block pos");
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    step_pos = start_pos;
    VLOG(3) << "start_pos=" << start_pos.segment << ", " << start_pos.index_offset;

    //find last row_block is end_key is given, or using last_row_block
    if (base_index->find_short_key(end_key, &helper_cursor, false, &end_pos) != OLAP_SUCCESS) {
        if (base_index->find_last_row_block(&end_pos) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail find last row block.");
            return OLAP_ERR_TABLE_INDEX_FIND_ERROR;
        }
    }

    VLOG(3) << "end_pos=" << end_pos.segment << ", " << end_pos.index_offset;

    //get rows between first and last
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor cur_start_key;
    RowCursor last_start_key;

    if (cur_start_key.init(tablet_schema(), num_short_key_fields()) != OLAP_SUCCESS
            || last_start_key.init(tablet_schema(), num_short_key_fields()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init cursor");
        return OLAP_ERR_INIT_FAILED;
    }

    if (base_index->get_row_block_entry(start_pos, &entry) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get block entry failed.");
        return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
    }

    cur_start_key.attach(entry.data);
    last_start_key.allocate_memory_for_string_type(tablet_schema());
    last_start_key.copy_without_pool(cur_start_key);
    // start_key是last start_key, 但返回的实际上是查询层给出的key
    ranges->emplace_back(start_key.to_tuple());

    while (end_pos > step_pos) {
        res = base_index->advance_row_block(expected_rows, &step_pos);
        if (res == OLAP_ERR_INDEX_EOF || !(end_pos > step_pos)) {
            break;
        } else if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("advance_row_block failed.");
            return OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION;
        }

        if (base_index->get_row_block_entry(step_pos, &entry) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("get block entry failed.");
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

bool Tablet::has_version(const Version& version) const {
    return (_rs_version_map.find(version) != _rs_version_map.end());
}

void Tablet::list_versions(vector<Version>* versions) const {
    DCHECK(versions != nullptr && versions->empty());

    // versions vector is not sorted.
    for (auto& it : _rs_version_map) {
        versions->push_back(it.first);
    }
}

void Tablet::list_entities(vector<VersionEntity>* entities) const {
    DCHECK(entities != nullptr && entities->empty());

    for (auto& it : _rs_version_map) {
        RowsetSharedPtr rowset = it.second;
        VersionEntity entity(it.first, 0);
        entities->push_back(entity);
    }
}

size_t Tablet::get_field_index(const string& field_name) const {
    return _schema->get_field_index(field_name);
}

size_t Tablet::get_row_size() const {
    return _schema->get_row_size();
}

size_t Tablet::get_data_size() const {
    size_t total_size = 0;
    for (auto& it : _rs_version_map) {
        total_size += it.second->data_disk_size();
    }
    return total_size;
}

size_t Tablet::num_rows() const {
    size_t num_rows = 0;
    for (auto& it : _rs_version_map) {
        num_rows += it.second->num_rows();
    }
    return num_rows;
}

bool Tablet::is_deletion_rowset(const Version& version) {
    if (version.first != version.second) {
        return false;
    }

    for (auto& it : _tablet_meta.delete_predicates()) {
        if (it.version() == version.first
              && it.version() == version.second) {
            return true;
        }
    }

    return false;
}

bool Tablet::is_schema_changing() {
    bool is_schema_changing = false;

    ReadLock rdlock(&_meta_lock);
    if (_tablet_meta.alter_state() != AlterTabletState::ALTER_NONE) {
        is_schema_changing = true;
    }

    return is_schema_changing;
}

bool Tablet::get_schema_change_request(int64_t* tablet_id, TSchemaHash* schema_hash,
                                       vector<Version>* versions_to_alter,
                                       AlterTabletType* alter_tablet_type) const {
    if (_tablet_meta.alter_state() == AlterTabletState::ALTER_NONE) {
        return false;
    }

    const AlterTabletTask& alter_task = _tablet_meta.alter_task();
    *tablet_id = alter_task.related_tablet_id();
    *schema_hash = alter_task.related_schema_hash();
    *alter_tablet_type = alter_task.alter_type();
    for (auto& rs : alter_task.rowsets_to_alter()) {
        versions_to_alter->push_back(rs->version());
    }

    return true;
}

void Tablet::set_schema_change_request(int64_t tablet_id,
                                       int64_t schema_hash,
                                       const vector<Version>& versions_to_alter,
                                       const AlterTabletType alter_type) {
    clear_schema_change_request();
    AlterTabletTask alter_task;
    alter_task.set_related_tablet_id(tablet_id);
    alter_task.set_related_schema_hash(schema_hash);
    for (auto& version : versions_to_alter) {
        RowsetMeta* rs_meta = new RowsetMeta();
        rs_meta->set_version(version);
    }

    alter_task.set_alter_type(alter_type);
    _tablet_meta.add_alter_task(alter_task);
}

bool Tablet::remove_last_schema_change_version(TabletSharedPtr new_tablet) {
    return true;
}

void Tablet::clear_schema_change_request() {
    LOG(INFO) << "clear schema change status. [tablet='" << full_name() << "']";
    _tablet_meta.delete_alter_task();
}

void Tablet::set_io_error() {
    OLAP_LOG_WARNING("io error occur.[tablet_full_name='%s', root_path_name='%s']",
                     full_name().c_str());
}

bool Tablet::is_used() {
    return !_is_bad && _data_dir->is_used();
}

VersionEntity Tablet::get_version_entity_by_version(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map[version];
    VersionEntity entity(version, 0);
    return entity;
}

size_t Tablet::get_version_data_size(const Version& version) {
    RowsetSharedPtr rowset = _rs_version_map[version];
    return rowset->data_disk_size();
}

OLAPStatus Tablet::recover_tablet_until_specfic_version(const int64_t& spec_version,
                                                        const int64_t& version_hash) {
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::test_version(const Version& version) {
    vector<Version> span_versions;
    ReadLock rdlock(&_meta_lock);
    OLAPStatus res = _rs_graph->capture_consistent_versions(version, &span_versions);

    return res;
}

OLAPStatus Tablet::register_tablet_into_dir() {
    return _data_dir->register_tablet(this);
}

OLAPStatus Tablet::clear_schema_change_info(
        AlterTabletType* type,
        bool only_one,
        bool check_only) {
    if (!check_only) {
        WriteLock w(&_meta_lock);
        _unprotect_clear_schema_change_info(type, only_one, check_only);
    } else {
        ReadLock r(&_meta_lock);
        _unprotect_clear_schema_change_info(type, only_one, check_only);
    }
    return OLAP_SUCCESS;
}

OLAPStatus Tablet::_unprotect_clear_schema_change_info(
        AlterTabletType* type,
        bool only_one,
        bool check_only) {
    OLAPStatus res = OLAP_SUCCESS;

    vector<Version> versions_to_be_changed;
    if (this->get_schema_change_request(NULL,
                                              NULL,
                                              &versions_to_be_changed,
                                              NULL)) {
        if (versions_to_be_changed.size() != 0) {
            OLAP_LOG_WARNING("schema change is not allowed now, "
                             "until previous schema change is done. [tablet='%s']",
                             full_name().c_str());
            return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
        }
    }

    if (!check_only) {
        VLOG(3) << "broke old schema change chain";
        clear_schema_change_request();
    }

    return res;
}

}  // namespace doris
