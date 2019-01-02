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

namespace doris {

AlphaRowset::AlphaRowset(const RowFields& tablet_schema,
        int num_key_fields, int num_short_key_fields,
        int num_rows_per_row_block, const std::string rowset_path,
        RowsetMetaSharedPtr rowset_meta) : _tablet_schema(tablet_schema),
        _num_key_fields(num_key_fields),
        _num_short_key_fields(num_short_key_fields),
        _num_rows_per_row_block(num_rows_per_row_block),
        _rowset_path(rowset_path),
        _rowset_meta(rowset_meta),
        _segment_group_size(0),
        _is_cumulative_rowset(false) {
    Version version = _rowset_meta->get_version();
    if (version.first == version.second) {
        _is_cumulative_rowset = false;
    } else {
        _is_cumulative_rowset = true;
    }
}

NewStatus AlphaRowset::init() {
    _init_segment_groups();
    return NewStatus::OK();
}

std::unique_ptr<RowsetReader> AlphaRowset::create_reader() {
    std::vector<SegmentGroup*> segment_groups;
    for (auto& segment_group : _segment_groups) {
        segment_groups.push_back(segment_group.get());
    }
    return std::unique_ptr<RowsetReader>(new AlphaRowsetReader(_tablet_schema,
            _num_key_fields, _num_short_key_fields, _num_rows_per_row_block,
            _rowset_path, _rowset_meta.get(), _segment_groups));
}

NewStatus AlphaRowset::copy(RowsetBuilder* dest_rowset_builder) {
    return NewStatus::OK();
}

NewStatus AlphaRowset::remove() {
    // TODO(hkp) : add delete code
    // delete rowset from meta
    // delete segment groups 
    return NewStatus::OK();
}

RowsetMetaSharedPtr AlphaRowset::get_meta() {
    return _rowset_meta;
}

void AlphaRowset::set_version(Version version) {
    _rowset_meta->set_version(version);
}

NewStatus AlphaRowset::_init_segment_groups() {
    std::vector<SegmentGroupPB> segment_group_metas;
    AlphaRowsetMeta* _alpha_rowset_meta = (AlphaRowsetMeta*)_rowset_meta.get();
    _alpha_rowset_meta->get_segment_groups(&segment_group_metas);
    for (auto& segment_group_meta : segment_group_metas) {
        Version version = _rowset_meta->get_version();
        int64_t version_hash = _rowset_meta->get_version_hash();
        std::shared_ptr<SegmentGroup> segment_group(new SegmentGroup(_rowset_meta->get_tablet_id(),
                _rowset_meta->get_rowset_id(), _tablet_schema, _num_key_fields, _num_short_key_fields,
                _num_rows_per_row_block, _rowset_path, version, version_hash,
                false, segment_group_meta.segment_group_id(), segment_group_meta.num_segments()));
        if (segment_group.get() == nullptr) {
            LOG(WARNING) << "fail to create olap segment_group. [version='" << version.first
                << "-" << version.second << "' rowset_id='" << _rowset_meta->get_rowset_id() << "']";
            return NewStatus::NoSpace("new segment group failed");
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
            return NewStatus::Corruption("segment group invalidate failed");
        }

        if (segment_group_meta.column_pruning_size() != 0) {
            size_t column_pruning_size = segment_group_meta.column_pruning_size();
            if (_num_key_fields != column_pruning_size) {
                LOG(ERROR) << "column pruning size is error."
                        << "column_pruning_size=" << column_pruning_size << ", "
                        << "num_key_fields=" << _num_key_fields;
                return NewStatus::InvalidArgument("invalid segment group column prunning size");
            }
            std::vector<std::pair<std::string, std::string>> column_statistic_strings(_num_key_fields);
            std::vector<bool> null_vec(_num_key_fields);
            for (size_t j = 0; j < _num_key_fields; ++j) {
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
                return NewStatus::NotSupported("add column statistics failed");
            }

            OLAPStatus res = segment_group->load();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to load segment_group. res=" << res << ", "
                        << "version=" << version.first << "-"
                        << version.second << ", "
                        << "version_hash=" << version_hash;
                return NewStatus::Corruption("segment group load failed");
            }
        }
    }
    _segment_group_size = _segment_groups.size();
    if (_is_cumulative_rowset && _segment_group_size > 1) {
        LOG(WARNING) << "invalid segment group meta for cumulative rowset. segment group size:"
                << _segment_group_size;
        return NewStatus::InvalidArgument("invalid segment group size:" + std::to_string(_segment_group_size));
    }
    return NewStatus::OK();
}

}  // namespace doris