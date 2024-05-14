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

#include "olap/rowset/rowset.h"

#include <gen_cpp/olap_file.pb.h>

#include "olap/olap_define.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "util/time.h"

namespace doris {

Rowset::Rowset(const TabletSchemaSPtr& schema, const RowsetMetaSharedPtr& rowset_meta)
        : _rowset_meta(rowset_meta), _refs_by_reader(0) {
    _is_pending = !_rowset_meta->has_version();
    if (_is_pending) {
        _is_cumulative = false;
    } else {
        Version version = _rowset_meta->version();
        _is_cumulative = version.first != version.second;
    }
    // build schema from RowsetMeta.tablet_schema or Tablet.tablet_schema
    _schema = _rowset_meta->tablet_schema() ? _rowset_meta->tablet_schema() : schema;
}

Status Rowset::load(bool use_cache) {
    // if the state is ROWSET_UNLOADING it means close() is called
    // and the rowset is already loaded, and the resource is not closed yet.
    if (_rowset_state_machine.rowset_state() == ROWSET_LOADED) {
        return Status::OK();
    }
    {
        // before lock, if rowset state is ROWSET_UNLOADING, maybe it is doing do_close in release
        std::lock_guard load_lock(_lock);
        // after lock, if rowset state is ROWSET_UNLOADING, it is ok to return
        if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
            // first do load, then change the state
            RETURN_IF_ERROR(do_load(use_cache));
            RETURN_IF_ERROR(_rowset_state_machine.on_load());
        }
    }
    // load is done
    VLOG_CRITICAL << "rowset is loaded. " << rowset_id()
                  << ", rowset version:" << rowset_meta()->version()
                  << ", state from ROWSET_UNLOADED to ROWSET_LOADED. tabletid:"
                  << _rowset_meta->tablet_id();
    return Status::OK();
}

void Rowset::make_visible(Version version) {
    _is_pending = false;
    _rowset_meta->set_version(version);
    _rowset_meta->set_rowset_state(VISIBLE);
    // update create time to the visible time,
    // it's used to skip recently published version during compaction
    _rowset_meta->set_creation_time(UnixSeconds());

    if (_rowset_meta->has_delete_predicate()) {
        _rowset_meta->mutable_delete_predicate()->set_version(version.first);
    }
}

bool Rowset::check_rowset_segment() {
    std::lock_guard load_lock(_lock);
    return check_current_rowset_segment();
}

void Rowset::merge_rowset_meta(const RowsetMetaSharedPtr& other) {
    _rowset_meta->set_num_segments(num_segments() + other->num_segments());
    _rowset_meta->set_num_rows(num_rows() + other->num_rows());
    _rowset_meta->set_data_disk_size(data_disk_size() + other->data_disk_size());
    _rowset_meta->set_index_disk_size(index_disk_size() + other->index_disk_size());
    std::vector<KeyBoundsPB> key_bounds;
    other->get_segments_key_bounds(&key_bounds);
    for (auto key_bound : key_bounds) {
        _rowset_meta->add_segment_key_bounds(key_bound);
    }
}

void Rowset::clear_cache() {
    SegmentLoader::instance()->erase_segments(rowset_id(), num_segments());
    clear_inverted_index_cache();
}

} // namespace doris
