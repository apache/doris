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

#include <stdio.h>  // for remove()
#include <unistd.h> // for link()
#include <util/file_utils.h>

#include <set>

#include "env/env_remote.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/utils.h"

namespace doris {

FilePathDesc BetaRowset::segment_file_path(const FilePathDesc& segment_dir_desc, const RowsetId& rowset_id,
                                          int segment_id) {
    FilePathDescStream path_desc_s;
    path_desc_s << segment_dir_desc << "/" << rowset_id.to_string() << "_" << segment_id << ".dat";
    return path_desc_s.path_desc();
}

BetaRowset::BetaRowset(const TabletSchema* schema, const FilePathDesc& rowset_path_desc,
                       RowsetMetaSharedPtr rowset_meta)
        : Rowset(schema, rowset_path_desc, std::move(rowset_meta)) {}

BetaRowset::~BetaRowset() {}

OLAPStatus BetaRowset::init() {
    return OLAP_SUCCESS; // no op
}

OLAPStatus BetaRowset::do_load(bool /*use_cache*/) {
    // do nothing.
    // the segments in this rowset will be loaded by calling load_segments() explicitly.
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments) {
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        FilePathDesc seg_path_desc = segment_file_path(_rowset_path_desc, rowset_id(), seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(seg_path_desc, seg_id, _schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path_desc.debug_string()
                    << " under rowset " << unique_id() << " : " << s.to_string();
            return OLAP_ERR_ROWSET_LOAD_FAILED;
        }
        segments->push_back(std::move(segment));
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::create_reader(RowsetReaderSharedPtr* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this())));
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::split_range(const RowCursor& start_key, const RowCursor& end_key,
                                   uint64_t request_block_row_count, size_t key_num,
                                   std::vector<OlapTuple>* ranges) {
    ranges->emplace_back(start_key.to_tuple());
    ranges->emplace_back(end_key.to_tuple());
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::remove() {
    // TODO should we close and remove all segment reader first?
    VLOG_NOTICE << "begin to remove files in rowset " << unique_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    bool success = true;
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);
        LOG(INFO) << "deleting " << path_desc.debug_string();
        fs::BlockManager* block_mgr = fs::fs_util::block_manager(path_desc.storage_medium);
        if (!block_mgr->delete_block(path_desc).ok()) {
            char errmsg[64];
            VLOG_NOTICE << "failed to delete file. err=" << strerror_r(errno, errmsg, 64)
                        << ", " << path_desc.debug_string();
            success = false;
        }
    }
    if (!success) {
        LOG(WARNING) << "failed to remove files in rowset " << unique_id();
        return OLAP_ERR_ROWSET_DELETE_FILE_FAILED;
    }
    return OLAP_SUCCESS;
}

void BetaRowset::do_close() {
    // do nothing.
}

OLAPStatus BetaRowset::link_files_to(const FilePathDesc& dir_desc, RowsetId new_rowset_id) {
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc dst_link_path_desc = segment_file_path(dir_desc, new_rowset_id, i);
        // TODO(lingbin): use Env API? or EnvUtil?
        if (FileUtils::check_exist(dst_link_path_desc.filepath)) {
            LOG(WARNING) << "failed to create hard link, file already exist: " << dst_link_path_desc.filepath;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        FilePathDesc src_file_path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        fs::BlockManager* block_mgr = fs::fs_util::block_manager(dir_desc.storage_medium);
        if (!block_mgr->link_file(src_file_path_desc, dst_link_path_desc).ok()) {
            LOG(WARNING) << "fail to create hard link. from=" << src_file_path_desc.debug_string() << ", "
                         << "to=" << dst_link_path_desc.debug_string() << ", errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::copy_files_to(const std::string& dir) {
    Env* env = Env::get_env(_rowset_path_desc.storage_medium);
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc dst_path_desc = segment_file_path(dir, rowset_id(), i);
        Status status = env->path_exists(dst_path_desc.filepath);
        if (status.ok()) {
            LOG(WARNING) << "file already exist: " << dst_path_desc.filepath;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        if (!status.is_not_found()) {
            LOG(WARNING) << "file check exist error: " << dst_path_desc.filepath;
            return OLAP_ERR_OS_ERROR;
        }
        FilePathDesc src_path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);
        if (!Env::get_env(_rowset_path_desc.storage_medium)->copy_path(
                src_path_desc.filepath, dst_path_desc.filepath).ok()) {
            LOG(WARNING) << "fail to copy file. from=" << src_path_desc.filepath << ", to="
                    << dst_path_desc.filepath << ", errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::upload_files_to(const FilePathDesc& dir_desc) {
    RemoteEnv* dest_env = dynamic_cast<RemoteEnv*>(Env::get_env(_rowset_path_desc.storage_medium));
    std::shared_ptr<StorageBackend> storage_backend = dest_env->get_storage_backend();
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc dst_path_desc = segment_file_path(dir_desc, rowset_id(), i);
        Status status = storage_backend->exist(dst_path_desc.remote_path);
        if (status.ok()) {
            LOG(WARNING) << "file already exist: " << dst_path_desc.remote_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        if (!status.is_not_found()) {
            LOG(WARNING) << "file check exist error: " << dst_path_desc.remote_path;
            return OLAP_ERR_OS_ERROR;
        }
        FilePathDesc src_path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);

        if (!storage_backend->upload(src_path_desc.filepath, dst_path_desc.remote_path).ok()) {
            LOG(WARNING) << "fail to upload file. from=" << src_path_desc.filepath << ", to="
                         << dst_path_desc.remote_path << ", errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
        LOG(INFO) << "succeed to upload file. from " << src_path_desc.filepath << " to "
                  << dst_path_desc.remote_path;
    }
    return OLAP_SUCCESS;
}

bool BetaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);
        valid_paths.insert(path_desc.filepath);
    }
    return valid_paths.find(path) != valid_paths.end();
}

bool BetaRowset::check_file_exist() {
    Env* env = Env::get_env(_rowset_path_desc.storage_medium);
    for (int i = 0; i < num_segments(); ++i) {
        FilePathDesc path_desc = segment_file_path(_rowset_path_desc, rowset_id(), i);
        if (!env->path_exists(path_desc.filepath).ok()) {
            LOG(WARNING) << "data file not existed: " << path_desc.filepath
                    << " for rowset_id: " << rowset_id();
            return false;
        }
    }
    return true;
}

} // namespace doris
