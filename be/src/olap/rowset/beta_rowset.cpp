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

#include <ctype.h>
#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/cache/file_cache_manager.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"

namespace doris {
using namespace ErrorCode;

using io::FileCacheManager;

std::string BetaRowset::segment_file_path(int segment_id) {
#ifdef BE_TEST
    if (!config::file_cache_type.empty()) {
        return segment_file_path(_tablet_path, rowset_id(), segment_id);
    }
#endif
    return segment_file_path(_rowset_dir, rowset_id(), segment_id);
}

std::string BetaRowset::segment_cache_path(int segment_id) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{seg_num}
    return fmt::format("{}/{}_{}", _tablet_path, rowset_id().to_string(), segment_id);
}

// just check that the format is xxx_segmentid and segmentid is numeric
bool BetaRowset::is_segment_cache_dir(const std::string& cache_dir) {
    auto segment_id_pos = cache_dir.find_last_of('_') + 1;
    if (segment_id_pos >= cache_dir.size() || segment_id_pos == 0) {
        return false;
    }
    return std::all_of(cache_dir.cbegin() + segment_id_pos, cache_dir.cend(), ::isdigit);
}

std::string BetaRowset::segment_file_path(const std::string& rowset_dir, const RowsetId& rowset_id,
                                          int segment_id) {
    // {rowset_dir}/{schema_hash}/{rowset_id}_{seg_num}.dat
    return fmt::format("{}/{}_{}.dat", rowset_dir, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::remote_segment_path(int64_t tablet_id, const RowsetId& rowset_id,
                                            int segment_id) {
    // data/{tablet_id}/{rowset_id}_{seg_num}.dat
    return remote_segment_path(tablet_id, rowset_id.to_string(), segment_id);
}

std::string BetaRowset::remote_segment_path(int64_t tablet_id, const std::string& rowset_id,
                                            int segment_id) {
    // data/{tablet_id}/{rowset_id}_{seg_num}.dat
    return fmt::format("{}/{}_{}.dat", remote_tablet_path(tablet_id), rowset_id, segment_id);
}

std::string BetaRowset::local_segment_path_segcompacted(const std::string& tablet_path,
                                                        const RowsetId& rowset_id, int64_t begin,
                                                        int64_t end) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{begin_seg}-{end_seg}.dat
    return fmt::format("{}/{}_{}-{}.dat", tablet_path, rowset_id.to_string(), begin, end);
}

BetaRowset::BetaRowset(const TabletSchemaSPtr& schema, const std::string& tablet_path,
                       const RowsetMetaSharedPtr& rowset_meta)
        : Rowset(schema, tablet_path, rowset_meta) {
    if (_rowset_meta->is_local()) {
        _rowset_dir = tablet_path;
    } else {
        _rowset_dir = remote_tablet_path(_rowset_meta->tablet_id());
    }
}

BetaRowset::~BetaRowset() = default;

Status BetaRowset::init() {
    return Status::OK(); // no op
}

Status BetaRowset::do_load(bool /*use_cache*/) {
    // do nothing.
    // the segments in this rowset will be loaded by calling load_segments() explicitly.
    return Status::OK();
}

Status BetaRowset::get_segments_size(std::vector<size_t>* segments_size) {
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = segment_file_path(seg_id);
        int64_t file_size;
        RETURN_IF_ERROR(fs->file_size(seg_path, &file_size));
        segments_size->push_back(file_size);
    }
    return Status::OK();
}
Status BetaRowset::load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments) {
    return load_segments(0, num_segments(), segments);
}

Status BetaRowset::load_segments(int64_t seg_id_begin, int64_t seg_id_end,
                                 std::vector<segment_v2::SegmentSharedPtr>* segments) {
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    int64_t seg_id = seg_id_begin;
    while (seg_id < seg_id_end) {
        DCHECK(seg_id >= 0);
        auto seg_path = segment_file_path(seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        io::SegmentCachePathPolicy cache_policy;
        cache_policy.set_cache_path(segment_cache_path(seg_id));
        auto type = config::enable_file_cache ? config::file_cache_type : "";
        io::FileReaderOptions reader_options(io::cache_type_from_string(type), cache_policy);
        auto s = segment_v2::Segment::open(fs, seg_path, seg_id, rowset_id(), _schema,
                                           reader_options, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path << " under rowset "
                         << unique_id() << " : " << s.to_string();
            return s;
        }
        segments->push_back(std::move(segment));
        seg_id++;
    }
    return Status::OK();
}

Status BetaRowset::create_reader(RowsetReaderSharedPtr* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this())));
    return Status::OK();
}

Status BetaRowset::remove() {
    // TODO should we close and remove all segment reader first?
    VLOG_NOTICE << "begin to remove files in rowset " << unique_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    // If the rowset was removed, it need to remove the fds in segment cache directly
    SegmentLoader::instance()->erase_segments(SegmentCache::CacheKey(rowset_id()));
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
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
        for (auto& column : _schema->columns()) {
            const TabletIndex* index_meta = _schema->get_inverted_index(column);
            if (index_meta) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_name(
                        seg_path, index_meta->index_id(),
                        index_meta->get_escaped_index_suffix_path());
                st = fs->delete_file(inverted_index_file);
                if (!st.ok()) {
                    LOG(WARNING) << st.to_string();
                    success = false;
                } else {
                    segment_v2::InvertedIndexSearcherCache::instance()->erase(inverted_index_file);
                }
            }
        }
        if (fs->type() != io::FileSystemType::LOCAL) {
            auto cache_path = segment_cache_path(i);
            FileCacheManager::instance()->remove_file_cache(cache_path);
        }
    }
    if (!success) {
        return Status::Error<ROWSET_DELETE_FILE_FAILED>("failed to remove files in rowset {}",
                                                        unique_id());
    }
    return Status::OK();
}

void BetaRowset::do_close() {
    // do nothing.
}

Status BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id,
                                 size_t new_rowset_start_seg_id,
                                 std::set<int32_t>* without_index_uids) {
    DCHECK(is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    if (fs->type() != io::FileSystemType::LOCAL) {
        return Status::InternalError("should be local file system");
    }
    io::LocalFileSystem* local_fs = (io::LocalFileSystem*)fs.get();
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = segment_file_path(dir, new_rowset_id, i + new_rowset_start_seg_id);
        bool dst_path_exist = false;
        if (!fs->exists(dst_path, &dst_path_exist).ok() || dst_path_exist) {
            return Status::Error<FILE_ALREADY_EXIST>(
                    "failed to create hard link, file already exist: {}", dst_path);
        }
        auto src_path = segment_file_path(i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        if (!local_fs->link_file(src_path, dst_path).ok()) {
            return Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                           src_path, dst_path);
        }
        for (auto& index : _schema->indexes()) {
            if (index.index_type() != IndexType::INVERTED) {
                continue;
            }

            auto index_id = index.index_id();
            if (without_index_uids != nullptr && without_index_uids->count(index_id)) {
                continue;
            }
            std::string inverted_index_src_file_path = InvertedIndexDescriptor::get_index_file_name(
                    src_path, index_id, index.get_escaped_index_suffix_path());
            std::string inverted_index_dst_file_path = InvertedIndexDescriptor::get_index_file_name(
                    dst_path, index_id, index.get_escaped_index_suffix_path());
            bool need_to_link = true;
            if (_schema->skip_write_index_on_load()) {
                local_fs->exists(inverted_index_src_file_path, &need_to_link);
                if (!need_to_link) {
                    LOG(INFO) << "skip create hard link to not existed file="
                              << inverted_index_src_file_path;
                }
            }
            if (need_to_link) {
                if (!local_fs->link_file(inverted_index_src_file_path, inverted_index_dst_file_path)
                             .ok()) {
                    return Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}",
                            inverted_index_src_file_path, inverted_index_dst_file_path,
                            Errno::no());
                }
                LOG(INFO) << "success to create hard link. from=" << inverted_index_src_file_path
                          << ", "
                          << "to=" << inverted_index_dst_file_path;
            }
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) {
    if (is_local() && num_segments() > 0) [[unlikely]] {
        DCHECK(false) << rowset_id();
        return Status::NotSupported("cannot copy remote files, rowset_id={}",
                                    rowset_id().to_string());
    }
    bool exists = false;
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = segment_file_path(dir, new_rowset_id, i);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dst_path, &exists));
        if (exists) {
            return Status::Error<FILE_ALREADY_EXIST>("file already exist: {}", dst_path);
        }
        auto src_path = segment_file_path(i);
        RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(src_path, dst_path));
        for (auto& column : _schema->columns()) {
            // if (column.has_inverted_index()) {
            const TabletIndex* index_meta = _schema->get_inverted_index(column);
            if (index_meta) {
                std::string inverted_index_src_file_path =
                        InvertedIndexDescriptor::get_index_file_name(
                                src_path, index_meta->index_id(),
                                index_meta->get_escaped_index_suffix_path());
                std::string inverted_index_dst_file_path =
                        InvertedIndexDescriptor::get_index_file_name(
                                dst_path, index_meta->index_id(),
                                index_meta->get_escaped_index_suffix_path()); 
                RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(
                        inverted_index_src_file_path, inverted_index_dst_file_path));
                LOG(INFO) << "success to copy file. from=" << inverted_index_src_file_path << ", "
                          << "to=" << inverted_index_dst_file_path;
            }
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
        auto remote_seg_path = remote_segment_path(_rowset_meta->tablet_id(), new_rowset_id, i);
        auto local_seg_path = segment_file_path(i);
        dest_paths.push_back(remote_seg_path);
        local_paths.push_back(local_seg_path);
        for (auto& column : _schema->columns()) {
            // if (column.has_inverted_index()) {
            const TabletIndex* index_meta = _schema->get_inverted_index(column);
            if (index_meta) {
                std::string remote_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(
                                remote_seg_path, index_meta->index_id(),
                                index_meta->get_escaped_index_suffix_path());
                std::string local_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(
                                local_seg_path, index_meta->index_id(),
                                index_meta->get_escaped_index_suffix_path());
                dest_paths.push_back(remote_inverted_index_file);
                local_paths.push_back(local_inverted_index_file);
            }
        }
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
        auto fs = _rowset_meta->fs();
        if (!fs) {
            return false;
        }
        bool seg_file_exist = false;
        if (!fs->exists(seg_path, &seg_file_exist).ok() || !seg_file_exist) {
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
        std::shared_ptr<segment_v2::Segment> segment;
        io::SegmentCachePathPolicy cache_policy;
        cache_policy.set_cache_path(segment_cache_path(seg_id));
        auto type = config::enable_file_cache ? config::file_cache_type : "";
        io::FileReaderOptions reader_options(io::cache_type_from_string(type), cache_policy);
        auto s = segment_v2::Segment::open(fs, seg_path, seg_id, rowset_id(), _schema,
                                           reader_options, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "segment can not be opened. file=" << seg_path;
            return false;
        }
    }
    return true;
}

Status BetaRowset::add_to_binlog() {
    // FIXME(Drogon): not only local file system
    DCHECK(is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    if (fs->type() != io::FileSystemType::LOCAL) {
        return Status::InternalError("should be local file system");
    }
    io::LocalFileSystem* local_fs = static_cast<io::LocalFileSystem*>(fs.get());

    // all segments are in the same directory, so cache binlog_dir without multi times check
    std::string binlog_dir;

    auto segments_num = num_segments();
    VLOG_DEBUG << fmt::format("add rowset to binlog. rowset_id={}, segments_num={}",
                              rowset_id().to_string(), segments_num);
    for (int i = 0; i < segments_num; ++i) {
        auto seg_file = segment_file_path(i);

        if (binlog_dir.empty()) {
            binlog_dir = std::filesystem::path(seg_file).parent_path().append("_binlog").string();

            bool exists = true;
            RETURN_IF_ERROR(local_fs->exists(binlog_dir, &exists));
            if (!exists) {
                RETURN_IF_ERROR(local_fs->create_directory(binlog_dir));
            }
        }

        auto binlog_file =
                (std::filesystem::path(binlog_dir) / std::filesystem::path(seg_file).filename())
                        .string();
        VLOG_DEBUG << "link " << seg_file << " to " << binlog_file;
        if (!local_fs->link_file(seg_file, binlog_file).ok()) {
            return Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                           seg_file, binlog_file, Errno::no());
        }
    }

    return Status::OK();
}

} // namespace doris
