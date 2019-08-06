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

#include <set>
#include <stdio.h>  // for remove()
#include <unistd.h> // for link()
#include "gutil/strings/substitute.h"
#include "olap/utils.h"

namespace doris {

BetaRowset::BetaRowset(const TabletSchema* schema,
                       string rowset_path,
                       DataDir* data_dir,
                       RowsetMetaSharedPtr rowset_meta)
    : Rowset(schema, std::move(rowset_path), data_dir, std::move(rowset_meta)) {
}

OLAPStatus BetaRowset::init() {
    if (is_inited()) {
        return OLAP_SUCCESS;
    }
    // TODO init segment readers
    set_inited(true);
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::load(bool use_cache) {
    // TODO load segment footers
    return OLAP_SUCCESS;
}

std::shared_ptr<RowsetReader> BetaRowset::create_reader() {
    // TODO return BetaRowsetReader or RowwiseIterator?
    return nullptr;
}

OLAPStatus BetaRowset::remove() {
    // TODO should we close and remove all segment reader first?
    // TODO should we rename the method to remove_files() to be more specific?
    LOG(INFO) << "begin to remove files in rowset " << unique_id();
    bool success = true;
    for (int i = 0; i < num_segments(); ++i) {
        std::string path = _segment_file_path(_rowset_path, i);
        LOG(INFO) << "deleting " << path;
        if (::remove(path.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "failed to delete file. err=" << strerror_r(errno, errmsg, 64)
                         << ", path=" << path;
            success = false;
        }
    }
    if (!success) {
        LOG(WARNING) << "failed to remove files in rowset " << unique_id();
        return OLAP_ERR_ROWSET_DELETE_FILE_FAILED;
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::make_snapshot(const std::string& snapshot_path, std::vector<std::string>* success_links) {
    // TODO should we rename this method to `hard_link_files_to` to be more general?
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_link_path = _segment_file_path(snapshot_path, i);
        if (check_dir_existed(dst_link_path)) {
            LOG(WARNING) << "failed to make snapshot, file already exist: " << dst_link_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        std::string src_file_path = _segment_file_path(_rowset_path, i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            LOG(WARNING) << "fail to create hard link. from=" << src_file_path << ", "
                         << "to=" << dst_link_path << ", " << "errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
        success_links->push_back(dst_link_path);
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::copy_files_to_path(const std::string& dest_path, std::vector<std::string>* success_files) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_path = _segment_file_path(dest_path, i);
        if (check_dir_existed(dst_path)) {
            LOG(WARNING) << "file already exist: " << dst_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        std::string src_path = _segment_file_path(_rowset_path, i);
        if (copy_file(src_path, dst_path) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to copy file. from=" << src_path << ", to=" << dst_path
                         << ", errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
        success_files->push_back(dst_path);
    }
    return OLAP_SUCCESS;
}

bool BetaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (int i = 0; i < num_segments(); ++i) {
        valid_paths.insert(_segment_file_path(_rowset_path, i));
    }
    return valid_paths.find(path) != valid_paths.end();
}

std::string BetaRowset::_segment_file_path(const std::string& dir, int segment_id) {
    return strings::Substitute("$0/$1_$2.dat", dir, rowset_id(), segment_id);
}

} // namespace doris