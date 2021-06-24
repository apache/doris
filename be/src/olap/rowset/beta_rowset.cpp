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

#include "gutil/strings/substitute.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/scan_range.h"
#include "olap/utils.h"

namespace doris {

std::string BetaRowset::segment_file_path(const std::string& dir, const RowsetId& rowset_id,
                                          int segment_id) {
    return strings::Substitute("$0/$1_$2.dat", dir, rowset_id.to_string(), segment_id);
}

BetaRowset::BetaRowset(const TabletSchema* schema, string rowset_path,
                       RowsetMetaSharedPtr rowset_meta)
        : Rowset(schema, std::move(rowset_path), std::move(rowset_meta)) {}

BetaRowset::~BetaRowset() {}

OLAPStatus BetaRowset::init() {
    return OLAP_SUCCESS; // no op
}

// `use_cache` is ignored because beta rowset doesn't support fd cache now
OLAPStatus BetaRowset::do_load(bool /*use_cache*/, std::shared_ptr<MemTracker> parent) {
    // Open all segments under the current rowset
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        std::string seg_path = segment_file_path(_rowset_path, rowset_id(), seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(seg_path, seg_id, _schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment " << seg_path << " under rowset " << unique_id()
                         << " : " << s.to_string();
            return OLAP_ERR_ROWSET_LOAD_FAILED;
        }
        _segments.push_back(std::move(segment));
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::create_reader(RowsetReaderSharedPtr* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this())));
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::create_reader(const std::shared_ptr<MemTracker>& parent_tracker,
                                     std::shared_ptr<RowsetReader>* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this()),
                                       parent_tracker));
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::split_range(const RowCursor& start_key, const RowCursor& end_key,
                                   uint64_t request_block_row_count, ScanRange* ranges) {
    ranges->range_type = ScanRangeType::KEY;
    ranges->key_range.emplace_back(start_key.to_tuple());
    ranges->key_range.emplace_back(end_key.to_tuple());
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::remove() {
    // TODO should we close and remove all segment reader first?
    LOG(INFO) << "begin to remove files in rowset " << unique_id()
              << ", version:" << start_version() << "-" << end_version()
              << ", tabletid:" << _rowset_meta->tablet_id();
    bool success = true;
    for (int i = 0; i < num_segments(); ++i) {
        std::string path = segment_file_path(_rowset_path, rowset_id(), i);
        LOG(INFO) << "deleting " << path;
        // TODO(lingbin): use Env API
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

void BetaRowset::do_close() {
    _segments.clear();
}

OLAPStatus BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_link_path = segment_file_path(dir, new_rowset_id, i);
        // TODO(lingbin): use Env API? or EnvUtil?
        if (FileUtils::check_exist(dst_link_path)) {
            LOG(WARNING) << "failed to create hard link, file already exist: " << dst_link_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        std::string src_file_path = segment_file_path(_rowset_path, rowset_id(), i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            LOG(WARNING) << "fail to create hard link. from=" << src_file_path << ", "
                         << "to=" << dst_link_path << ", "
                         << "errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowset::copy_files_to(const std::string& dir) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_path = segment_file_path(dir, rowset_id(), i);
        if (FileUtils::check_exist(dst_path)) {
            LOG(WARNING) << "file already exist: " << dst_path;
            return OLAP_ERR_FILE_ALREADY_EXIST;
        }
        std::string src_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (copy_file(src_path, dst_path) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to copy file. from=" << src_path << ", to=" << dst_path
                         << ", errno=" << Errno::no();
            return OLAP_ERR_OS_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

bool BetaRowset::check_path(const std::string& path) {
    std::set<std::string> valid_paths;
    for (int i = 0; i < num_segments(); ++i) {
        valid_paths.insert(segment_file_path(_rowset_path, rowset_id(), i));
    }
    return valid_paths.find(path) != valid_paths.end();
}

bool BetaRowset::check_file_exist() {
    for (int i = 0; i < num_segments(); ++i) {
        std::string data_file = segment_file_path(_rowset_path, rowset_id(), i);
        if (!FileUtils::check_exist(data_file)) {
            LOG(WARNING) << "data file not existed: " << data_file
                         << " for rowset_id: " << rowset_id();
            return false;
        }
    }
    return true;
}

} // namespace doris
