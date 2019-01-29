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

namespace doris {

AlphaRowset::AlphaRowset(const TabletSchema* schema,
                         const std::string rowset_path,
                         DataDir* data_dir,
                         RowsetMetaSharedPtr rowset_meta)
      : _schema(schema),
        _rowset_path(rowset_path),
        _data_dir(data_dir),
        _rowset_meta(rowset_meta),
        _segment_group_size(0),
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
    _ref_count = 0;
}

OLAPStatus AlphaRowset::init() {
    _init_segment_groups();
    return OLAP_SUCCESS;
}

std::unique_ptr<RowsetReader> AlphaRowset::create_reader() {
    return std::unique_ptr<RowsetReader>(new AlphaRowsetReader(
            _schema->num_key_columns(), _schema->num_short_key_columns(),
            _schema->num_rows_per_row_block(), _rowset_path,
            _rowset_meta.get(), _segment_groups));
}

OLAPStatus AlphaRowset::copy(RowsetWriter* dest_rowset_writer) {
    return OLAP_SUCCESS;
}

OLAPStatus AlphaRowset::remove() {
    OlapMeta* meta = _data_dir->get_meta();
    OLAPStatus status = RowsetMetaManager::remove(meta, rowset_id());
    if (status != OLAP_SUCCESS) {
        LOG(FATAL) << "failed to remove meta of rowset_id:" << rowset_id();
        return status;
    }
    for (auto segment_group : _segment_groups) {
        bool ret = segment_group->delete_all_files();
        if (!ret) {
            LOG(FATAL) << "delete segment group files failed."
                       << " tablet id:" << segment_group->get_tablet_id()
                       << " rowset path:" << segment_group->get_rowset_path_prefix();
            return OLAP_ERR_ROWSET_DELETE_SEGMENT_GROUP_FILE_FAILED;
        }
    }
    return OLAP_SUCCESS;
}

void AlphaRowset::to_rowset_pb(RowsetMetaPB* rs_meta) {
    return _rowset_meta->to_rowset_pb(rs_meta);
}

RowsetMetaSharedPtr AlphaRowset::rowset_meta() const {
    return _rowset_meta;
}

void AlphaRowset::set_version(Version version) {
    _rowset_meta->set_version(version);
    // set the rowset state to VISIBLE
    _rowset_meta->set_rowset_state(VISIBLE);
    _is_pending_rowset = false;
}

Version AlphaRowset::version() const {
    return _rowset_meta->version();
}

VersionHash AlphaRowset::version_hash() const {
    return _rowset_meta->version_hash();
}

bool AlphaRowset::in_use() const {
    return _ref_count > 0;
}

void AlphaRowset::acquire() {
    atomic_inc(&_ref_count);
}

void AlphaRowset::release() {
    atomic_dec(&_ref_count);
}
    
int64_t AlphaRowset::ref_count() const {
    return _ref_count;
}

RowsetId AlphaRowset::rowset_id() const {
    return _rowset_meta->rowset_id();
}

int64_t AlphaRowset::end_version() const {
    return _rowset_meta->version().second;
}

int64_t AlphaRowset::start_version() const {
    return _rowset_meta->version().first;
}

bool AlphaRowset::make_snapshot(std::vector<std::string>* success_links) {
    for (auto segment_group : _segment_groups) {
        bool  ret = segment_group->make_snapshot(success_links);
        if (!ret) {
            LOG(WARNING) << "create hard links failed for segment group:"
                << segment_group->segment_group_id();
            return false;
        }
    }
    return true;
}

bool AlphaRowset::remove_old_files(std::vector<std::string>* removed_links) {
    for (auto segment_group : _segment_groups) {
        bool  ret = segment_group->remove_old_files(removed_links);
        if (!ret) {
            LOG(WARNING) << "remove old files failed for segment group:"
                << segment_group->segment_group_id();
            return false;
        }
    }
    return true;
}

int AlphaRowset::data_disk_size() const {
    return _rowset_meta->total_disk_size();
}

int AlphaRowset::index_disk_size() const {
    return _rowset_meta->index_disk_size();
}

bool AlphaRowset::empty() const {
    return _rowset_meta->empty();
}

bool AlphaRowset::zero_num_rows() const {
    return _rowset_meta->num_rows() == 0;
}

size_t AlphaRowset::num_rows() const {
    return _rowset_meta->num_rows();
}

void AlphaRowset::set_version_hash(VersionHash version_hash) {
    _rowset_meta->set_version_hash(version_hash);
}

int64_t AlphaRowset::create_time() {
    return _rowset_meta->create_time();
}

OLAPStatus AlphaRowset::_init_segment_groups() {
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMeta* _alpha_rowset_meta = (AlphaRowsetMeta*)_rowset_meta.get();
    _alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    for (auto& segment_group_meta : segment_group_metas) {
        Version version = _rowset_meta->version();
        int64_t version_hash = _rowset_meta->version_hash();
        std::shared_ptr<SegmentGroup> segment_group(new SegmentGroup(_rowset_meta->tablet_id(),
                _rowset_meta->rowset_id(), _schema, _rowset_path, version, version_hash,
                false, segment_group_meta.segment_group_id(), segment_group_meta.num_segments()));
        if (segment_group.get() == nullptr) {
            LOG(WARNING) << "fail to create olap segment_group. [version='" << version.first
                << "-" << version.second << "' rowset_id='" << _rowset_meta->rowset_id() << "']";
            return OLAP_ERR_CREATE_FILE_ERROR;
        }
        _segment_groups.push_back(segment_group);
        if (segment_group_meta.has_empty()) {
            segment_group->set_empty(segment_group_meta.empty());
        }

        // validate segment group
        if (segment_group->validate() != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to validate segment_group. [version="<< version.first
                    << "-" << version.second << " version_hash=" << version_hash;
                // if load segment group failed, rowset init failed
            return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
        }

        if (segment_group_meta.column_pruning_size() != 0) {
            size_t column_pruning_size = segment_group_meta.column_pruning_size();
            size_t num_key_columns = _schema->num_key_columns();
            if (num_key_columns != column_pruning_size) {
                LOG(ERROR) << "column pruning size is error."
                        << "column_pruning_size=" << column_pruning_size << ", "
                        << "num_key_columns=" << _schema->num_key_columns();
                return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR; 
            }
            std::vector<std::pair<std::string, std::string>> column_statistic_strings(num_key_columns);
            std::vector<bool> null_vec(num_key_columns);
            for (size_t j = 0; j < num_key_columns; ++j) {
                ColumnPruning column_pruning = segment_group_meta.column_pruning(j);
                column_statistic_strings[j].first = column_pruning.min();
                column_statistic_strings[j].second = column_pruning.max();
                if (column_pruning.has_null_flag()) {
                    null_vec[j] = column_pruning.null_flag();
                } else {
                    null_vec[j] = false;
                }
            }
            OLAPStatus status = segment_group->add_column_statistics(column_statistic_strings, null_vec);
            if (status != OLAP_SUCCESS) {
                return status;
            }

            OLAPStatus res = segment_group->load();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to load segment_group. res=" << res << ", "
                        << "version=" << version.first << "-"
                        << version.second << ", "
                        << "version_hash=" << version_hash;
                return res; 
            }
        }
    }
    _segment_group_size = _segment_groups.size();
    if (_is_cumulative_rowset && _segment_group_size > 1) {
        LOG(WARNING) << "invalid segment group meta for cumulative rowset. segment group size:"
                << _segment_group_size;
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR; 
    }
    return OLAP_SUCCESS;
}

}  // namespace doris