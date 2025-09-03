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

#include "common/cast_set.h"
#include "common/config.h"
#include "io/cache/block_file_cache_factory.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {

#include "common/compile_check_begin.h"

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
        _rowset_meta->mutable_delete_predicate()->set_version(cast_set<int32_t>(version.first));
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

const TabletSchemaSPtr& Rowset::tablet_schema() const {
#ifdef BE_TEST
    // for mocking tablet schema
    return _schema;
#endif
    return _rowset_meta->tablet_schema() ? _rowset_meta->tablet_schema() : _schema;
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
    if (config::enable_file_cache) {
        for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
            auto file_key = segment_v2::Segment::file_cache_key(rowset_id().to_string(), seg_id);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
        }

        // inverted index
        auto file_names = get_index_file_names();
        for (const auto& file_name : file_names) {
            auto file_key = io::BlockFileCache::hash(file_name);
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
        }
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

std::vector<std::string> Rowset::get_index_file_names() {
    std::vector<std::string> file_names;
    auto idx_version = _schema->get_inverted_index_storage_format();
    for (int64_t seg_id = 0; seg_id < num_segments(); ++seg_id) {
        if (idx_version == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : _schema->inverted_indexes()) {
                auto file_name = segment_v2::InvertedIndexDescriptor::get_index_file_name_v1(
                        rowset_id().to_string(), seg_id, index->index_id(),
                        index->get_index_suffix());
                file_names.emplace_back(std::move(file_name));
            }
        } else {
            auto file_name = segment_v2::InvertedIndexDescriptor::get_index_file_name_v2(
                    rowset_id().to_string(), seg_id);
            file_names.emplace_back(std::move(file_name));
        }
    }
    return file_names;
}

#include "common/compile_check_end.h"

} // namespace doris
