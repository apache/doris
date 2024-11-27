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
#include "util/trace.h"

namespace doris {

static bvar::Adder<size_t> g_total_rowset_num("doris_total_rowset_num");

Rowset::Rowset(const TabletSchemaSPtr& schema, RowsetMetaSharedPtr rowset_meta,
               std::string tablet_path)
        : _rowset_meta(std::move(rowset_meta)),
          _tablet_path(std::move(tablet_path)),
          _refs_by_reader(0) {
#ifndef BE_TEST
    DCHECK(!is_local() || !_tablet_path.empty()); // local rowset MUST has tablet path
#endif

    _is_pending = true;

    // Generally speaking, as long as a rowset has a version, it can be considered not to be in a pending state.
    // However, if the rowset was created through ingesting binlogs, it will have a version but should still be
    // considered in a pending state because the ingesting txn has not yet been committed.
    if (_rowset_meta->has_version() && _rowset_meta->start_version() > 0 &&
        _rowset_meta->rowset_state() != COMMITTED) {
        _is_pending = false;
    }

    if (_is_pending) {
        _is_cumulative = false;
    } else {
        Version version = _rowset_meta->version();
        _is_cumulative = version.first != version.second;
    }
    // build schema from RowsetMeta.tablet_schema or Tablet.tablet_schema
    _schema = _rowset_meta->tablet_schema() ? _rowset_meta->tablet_schema() : schema;
    g_total_rowset_num << 1;
}

Rowset::~Rowset() {
    g_total_rowset_num << -1;
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

void Rowset::set_version(Version version) {
    _rowset_meta->set_version(version);
}

bool Rowset::check_rowset_segment() {
    std::lock_guard load_lock(_lock);
    return check_current_rowset_segment();
}

std::string Rowset::get_rowset_info_str() {
    std::string disk_size = PrettyPrinter::print(
            static_cast<uint64_t>(_rowset_meta->total_disk_size()), TUnit::BYTES);
    return fmt::format("[{}-{}] {} {} {} {} {}", start_version(), end_version(), num_segments(),
                       _rowset_meta->has_delete_predicate() ? "DELETE" : "DATA",
                       SegmentsOverlapPB_Name(_rowset_meta->segments_overlap()),
                       rowset_id().to_string(), disk_size);
}

void Rowset::clear_cache() {
    {
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(std::chrono::seconds(1));
        SegmentLoader::instance()->erase_segments(rowset_id(), num_segments());
    }
    {
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(std::chrono::seconds(1));
        clear_inverted_index_cache();
    }
}

Result<std::string> Rowset::segment_path(int64_t seg_id) {
    if (is_local()) {
        return local_segment_path(_tablet_path, _rowset_meta->rowset_id().to_string(), seg_id);
    }

    return _rowset_meta->remote_storage_resource().transform([=, this](auto&& storage_resource) {
        return storage_resource->remote_segment_path(_rowset_meta->tablet_id(),
                                                     _rowset_meta->rowset_id().to_string(), seg_id);
    });
}

Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.size() < 2) {
        return Status::OK();
    }
    auto prev = rowsets.begin();
    for (auto it = rowsets.begin() + 1; it != rowsets.end(); ++it) {
        if ((*prev)->end_version() + 1 != (*it)->start_version()) {
            return Status::InternalError("versions are not continuity: prev={} cur={}",
                                         (*prev)->version().to_string(),
                                         (*it)->version().to_string());
        }
        prev = it;
    }
    return Status::OK();
}

void Rowset::merge_rowset_meta(const RowsetMeta& other) {
    _rowset_meta->merge_rowset_meta(other);
    // rowset->meta_meta()->tablet_schema() maybe updated so make sure _schema is
    // consistent with rowset meta
    _schema = _rowset_meta->tablet_schema();
}

} // namespace doris
