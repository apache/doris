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

#include "olap/rowset/beta_rowset.h"

#include <fmt/core.h>
#include <glog/logging.h>
#include <stdio.h>  // for remove()
#include <unistd.h> // for link()
#include <util/file_utils.h>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "io/cache/file_cache_manager.h"
#include "io/fs/s3_file_system.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"

namespace doris {

using io::FileCacheManager;

std::string BetaRowset::segment_file_path(int segment_id) {
    if (is_local()) {
        return local_segment_path(_tablet_path, rowset_id(), segment_id);
    }
#ifdef BE_TEST
    if (!config::file_cache_type.empty()) {
        return local_segment_path(_tablet_path, rowset_id(), segment_id);
    }
#endif
    return remote_segment_path(_rowset_meta->tablet_id(), rowset_id(), segment_id);
}

std::string BetaRowset::segment_cache_path(int segment_id) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{seg_num}
    return fmt::format("{}/{}_{}", _tablet_path, rowset_id().to_string(), segment_id);
}

std::string BetaRowset::local_segment_path(const std::string& tablet_path,
                                           const RowsetId& rowset_id, int segment_id) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{seg_num}.dat
    return fmt::format("{}/{}_{}.dat", tablet_path, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::remote_segment_path(int64_t tablet_id, const std::string& rowset_id,
                                            int segment_id) {
    // data/{tablet_id}/{rowset_id}_{seg_num}.dat
    return fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, segment_id);
}

std::string BetaRowset::remote_segment_path(int64_t tablet_id, const RowsetId& rowset_id,
                                            int segment_id) {
    // data/{tablet_id}/{rowset_id}_{seg_num}.dat
    return fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id.to_string(),
                       segment_id);
}

std::string BetaRowset::local_cache_path(const std::string& tablet_path, const RowsetId& rowset_id,
                                         int segment_id) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{seg_num}
    return fmt::format("{}/{}_{}", tablet_path, rowset_id.to_string(), segment_id);
}

BetaRowset::BetaRowset(TabletSchemaSPtr schema, const std::string& tablet_path,
                       RowsetMetaSharedPtr rowset_meta)
        : Rowset(schema, tablet_path, std::move(rowset_meta)) {}

BetaRowset::~BetaRowset() = default;

Status BetaRowset::init() {
    return Status::OK(); // no op
}

Status BetaRowset::do_load(bool /*use_cache*/) {
    // do nothing.
    // the segments in this rowset will be loaded by calling load_segments() explicitly.
    return Status::OK();
}

Status BetaRowset::load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments) {
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = segment_file_path(seg_id);
        auto cache_path = segment_cache_path(seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(fs, seg_path, cache_path, seg_id, _schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path << " under rowset "
                         << unique_id() << " : " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_LOAD_FAILED);
        }
        segments->push_back(std::move(segment));
    }
    return Status::OK();
}

Status BetaRowset::load_segment(int64_t seg_id, segment_v2::SegmentSharedPtr* segment) {
    DCHECK(seg_id >= 0);
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    auto seg_path = segment_file_path(seg_id);
    auto cache_path = segment_cache_path(seg_id);
    auto s = segment_v2::Segment::open(fs, seg_path, cache_path, seg_id, _schema, segment);
    if (!s.ok()) {
        LOG(WARNING) << "failed to open segment. " << seg_path << " under rowset " << unique_id()
                     << " : " << s.to_string();
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_LOAD_FAILED);
    }
    return Status::OK();
}

Status BetaRowset::create_reader(RowsetReaderSharedPtr* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this())));
    return Status::OK();
}

Status BetaRowset::split_range(const RowCursor& start_key, const RowCursor& end_key,
                               uint64_t request_block_row_count, size_t key_num,
                               std::vector<OlapTuple>* ranges) {
    ranges->emplace_back(start_key.to_tuple());
    ranges->emplace_back(end_key.to_tuple());
    return Status::OK();
}

Status BetaRowset::remove() {
    // TODO should we close and remove all segment reader first?
    VLOG_NOTICE << "begin to remove files in rowset " << unique_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    bool success = true;
    Status st;
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_file_path(i);
        LOG(INFO) << "deleting " << seg_path;
        st = fs->delete_file(seg_path);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
            success = false;
        }
        if (fs->type() != io::FileSystemType::LOCAL) {
            auto cache_path = segment_cache_path(i);
            FileCacheManager::instance()->remove_file_cache(cache_path);
        }
    }
    if (!success) {
        LOG(WARNING) << "failed to remove files in rowset " << unique_id();
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_DELETE_FILE_FAILED);
    }
    return Status::OK();
}

void BetaRowset::do_close() {
    // do nothing.
}

Status BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id) {
    DCHECK(is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = local_segment_path(dir, new_rowset_id, i);
        // TODO(lingbin): use Env API? or EnvUtil?
        if (FileUtils::check_exist(dst_path)) {
            LOG(WARNING) << "failed to create hard link, file already exist: " << dst_path;
            return Status::OLAPInternalError(OLAP_ERR_FILE_ALREADY_EXIST);
        }
        auto src_path = segment_file_path(i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        if (!fs->link_file(src_path, dst_path).ok()) {
            LOG(WARNING) << "fail to create hard link. from=" << src_path << ", "
                         << "to=" << dst_path << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) {
    DCHECK(is_local());
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = local_segment_path(dir, new_rowset_id, i);
        Status status = Env::Default()->path_exists(dst_path);
        if (status.ok()) {
            LOG(WARNING) << "file already exist: " << dst_path;
            return Status::OLAPInternalError(OLAP_ERR_FILE_ALREADY_EXIST);
        }
        if (!status.is_not_found()) {
            LOG(WARNING) << "file check exist error: " << dst_path;
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        }
        auto src_path = segment_file_path(i);
        if (!Env::Default()->copy_path(src_path, dst_path).ok()) {
            LOG(WARNING) << "fail to copy file. from=" << src_path << ", to=" << dst_path
                         << ", errno=" << Errno::no();
            return Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
        }
    }
    return Status::OK();
}

Status BetaRowset::upload_to(io::RemoteFileSystem* dest_fs, const RowsetId& new_rowset_id) {
    DCHECK(is_local());
    if (num_segments() < 1) {
        return Status::OK();
    }
    std::vector<io::Path> local_paths;
    local_paths.reserve(num_segments());
    std::vector<io::Path> dest_paths;
    dest_paths.reserve(num_segments());
    for (int i = 0; i < num_segments(); ++i) {
        // Note: Here we use relative path for remote.
        dest_paths.push_back(remote_segment_path(_rowset_meta->tablet_id(), new_rowset_id, i));
        local_paths.push_back(segment_file_path(i));
    }
    auto st = dest_fs->batch_upload(local_paths, dest_paths);
    if (st.ok()) {
        DorisMetrics::instance()->upload_rowset_count->increment(1);
        DorisMetrics::instance()->upload_total_byte->increment(data_disk_size());
    } else {
        DorisMetrics::instance()->upload_fail_count->increment(1);
    }
    return st;
}

bool BetaRowset::check_path(const std::string& path) {
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_file_path(i);
        if (seg_path == path) {
            return true;
        }
    }
    return false;
}

bool BetaRowset::check_file_exist() {
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_file_path(i);
        if (!Env::Default()->path_exists(seg_path).ok()) {
            LOG(WARNING) << "data file not existed: " << seg_path
                         << " for rowset_id: " << rowset_id();
            return false;
        }
    }
    return true;
}

bool BetaRowset::check_current_rowset_segment() {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return false;
    }
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = segment_file_path(seg_id);
        auto cache_path = segment_cache_path(seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(fs, seg_path, cache_path, seg_id, _schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "segment can not be opened. file=" << seg_path;
            return false;
        }
    }
    return true;
}

} // namespace doris
