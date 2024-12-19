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
#include <errno.h>
#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <ostream>
#include <utility>

#include "beta_rowset.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/crc32c.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"

namespace doris {
using namespace ErrorCode;

std::string BetaRowset::segment_file_path(int segment_id) const {
    return segment_file_path(_rowset_dir, rowset_id(), segment_id);
}

std::string BetaRowset::segment_file_path(const std::string& rowset_dir, const RowsetId& rowset_id,
                                          int segment_id) {
    // {rowset_dir}/{rowset_id}_{seg_num}.dat
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
        : Rowset(schema, rowset_meta), _rowset_dir(tablet_path) {}

BetaRowset::~BetaRowset() = default;

Status BetaRowset::init() {
    return Status::OK(); // no op
}

Status BetaRowset::do_load(bool /*use_cache*/) {
    // do nothing.
    // the segments in this rowset will be loaded by calling load_segments() explicitly.
    return Status::OK();
}

Status BetaRowset::get_inverted_index_size(size_t* index_size) {
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        auto indices = _schema->indexes();
        for (auto& index : indices) {
            // only get file_size for inverted index
            if (index.index_type() != IndexType::INVERTED) {
                continue;
            }
            for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
                auto seg_path = segment_file_path(seg_id);
                int64_t file_size = 0;

                std::string inverted_index_file_path = InvertedIndexDescriptor::get_index_file_name(
                        seg_path, index.index_id(), index.get_index_suffix());
                RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
                *index_size += file_size;
            }
        }
    } else {
        for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
            auto seg_path = segment_file_path(seg_id);
            int64_t file_size = 0;

            std::string inverted_index_file_path =
                    InvertedIndexDescriptor::get_index_file_name(seg_path);
            RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
            *index_size += file_size;
        }
    }
    return Status::OK();
}

void BetaRowset::clear_inverted_index_cache() {
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_file_path(i);
        for (auto& column : tablet_schema()->columns()) {
            const TabletIndex* index_meta = tablet_schema()->get_inverted_index(*column);
            if (index_meta) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_name(
                        seg_path, index_meta->index_id(), index_meta->get_index_suffix());
                (void)segment_v2::InvertedIndexSearcherCache::instance()->erase(
                        inverted_index_file);
            }
        }
    }
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
    int64_t seg_id = seg_id_begin;
    while (seg_id < seg_id_end) {
        std::shared_ptr<segment_v2::Segment> segment;
        RETURN_IF_ERROR(load_segment(seg_id, &segment));
        segments->push_back(std::move(segment));
        seg_id++;
    }
    return Status::OK();
}

Status BetaRowset::load_segment(int64_t seg_id, segment_v2::SegmentSharedPtr* segment) {
    auto fs = _rowset_meta->fs();
    if (!fs || _schema == nullptr) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    DCHECK(seg_id >= 0);
    auto seg_path = segment_file_path(seg_id);
    io::FileReaderOptions reader_options {
            .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                    : io::FileCachePolicy::NO_CACHE,
            .is_doris_table = true,
    };
    auto s = segment_v2::Segment::open(fs, seg_path, seg_id, rowset_id(), _schema, reader_options,
                                       segment);
    if (!s.ok()) {
        LOG(WARNING) << "failed to open segment. " << seg_path << " under rowset " << rowset_id()
                     << " : " << s.to_string();
        return s;
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
    VLOG_NOTICE << "begin to remove files in rowset " << rowset_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    // If the rowset was removed, it need to remove the fds in segment cache directly
    SegmentLoader::instance()->erase_segments(_rowset_meta->rowset_id(),
                                              _rowset_meta->num_segments());
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
        if (_schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1 &&
            _schema->has_inverted_index()) {
            std::string inverted_index_file =
                    InvertedIndexDescriptor::get_index_file_name(seg_path);
            st = fs->delete_file(inverted_index_file);
            if (!st.ok()) {
                LOG(WARNING) << st.to_string();
                success = false;
            }
        }
        for (auto& column : _schema->columns()) {
            const TabletIndex* index_meta = _schema->get_inverted_index(*column);
            if (index_meta) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_name(
                        seg_path, index_meta->index_id(), index_meta->get_index_suffix());
                if (_schema->get_inverted_index_storage_format() ==
                    InvertedIndexStorageFormatPB::V1) {
                    st = fs->delete_file(inverted_index_file);
                    if (!st.ok()) {
                        LOG(WARNING) << st.to_string();
                        success = false;
                    }
                }
                if (success) {
                    RETURN_IF_ERROR(segment_v2::InvertedIndexSearcherCache::instance()->erase(
                            inverted_index_file));
                }
            }
        }
    }
    if (!success) {
        return Status::Error<ROWSET_DELETE_FILE_FAILED>("failed to remove files in rowset {}",
                                                        rowset_id().to_string());
    }
    return Status::OK();
}

void BetaRowset::do_close() {
    // do nothing.
}

Status BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id,
                                 size_t new_rowset_start_seg_id,
                                 std::set<int64_t>* without_index_uids) {
    DCHECK(is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    if (fs->type() != io::FileSystemType::LOCAL) {
        return Status::InternalError("should be local file system");
    }

    Status status;
    std::vector<string> linked_success_files;
    Defer remove_linked_files {[&]() { // clear linked files if errors happen
        if (!status.ok()) {
            LOG(WARNING) << "will delete linked success files due to error " << status;
            std::vector<io::Path> paths;
            for (auto& file : linked_success_files) {
                paths.emplace_back(file);
                LOG(WARNING) << "will delete linked success file " << file << " due to error";
            }
            static_cast<void>(fs->batch_delete(paths));
            LOG(WARNING) << "done delete linked success files due to error " << status;
        }
    }};

    io::LocalFileSystem* local_fs = (io::LocalFileSystem*)fs.get();
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = segment_file_path(dir, new_rowset_id, i + new_rowset_start_seg_id);
        bool dst_path_exist = false;
        if (!fs->exists(dst_path, &dst_path_exist).ok() || dst_path_exist) {
            status = Status::Error<FILE_ALREADY_EXIST>(
                    "failed to create hard link, file already exist: {}", dst_path);
            return status;
        }
        auto src_path = segment_file_path(i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        if (!local_fs->link_file(src_path, dst_path).ok()) {
            status = Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                             src_path, dst_path, Errno::no());
            return status;
        }
        linked_success_files.push_back(dst_path);
        DBUG_EXECUTE_IF("fault_inject::BetaRowset::link_files_to::_link_inverted_index_file", {
            status = Status::Error<OS_ERROR>("fault_inject link_file error");
            return status;
        });
        if (_schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1) {
            if (_schema->has_inverted_index() &&
                (without_index_uids == nullptr || without_index_uids->empty())) {
                std::string inverted_index_file_src =
                        InvertedIndexDescriptor::get_index_file_name(src_path);
                std::string inverted_index_file_dst =
                        InvertedIndexDescriptor::get_index_file_name(dst_path);
                bool index_dst_path_exist = false;

                if (!fs->exists(inverted_index_file_dst, &index_dst_path_exist).ok() ||
                    index_dst_path_exist) {
                    status = Status::Error<FILE_ALREADY_EXIST>(
                            "failed to create hard link, file already exist: {}",
                            inverted_index_file_dst);
                    return status;
                }
                if (!local_fs->link_file(inverted_index_file_src, inverted_index_file_dst).ok()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}",
                            inverted_index_file_src, inverted_index_file_dst, Errno::no());
                    return status;
                }
                linked_success_files.push_back(inverted_index_file_dst);
            }
        } else {
            for (const auto& index : _schema->indexes()) {
                if (index.index_type() != IndexType::INVERTED) {
                    continue;
                }

                auto index_id = index.index_id();
                if (without_index_uids != nullptr && without_index_uids->count(index_id)) {
                    continue;
                }
                std::string inverted_index_src_file_path =
                        InvertedIndexDescriptor::get_index_file_name(src_path, index_id,
                                                                     index.get_index_suffix());
                std::string inverted_index_dst_file_path =
                        InvertedIndexDescriptor::get_index_file_name(dst_path, index_id,
                                                                     index.get_index_suffix());
                bool index_file_exists = true;
                RETURN_IF_ERROR(local_fs->exists(inverted_index_src_file_path, &index_file_exists));
                if (index_file_exists) {
                    DBUG_EXECUTE_IF(
                            "fault_inject::BetaRowset::link_files_to::_link_inverted_index_file", {
                                status = Status::Error<OS_ERROR>(
                                        "fault_inject link_file error from={}, to={}",
                                        inverted_index_src_file_path, inverted_index_dst_file_path);
                                return status;
                            });
                    if (!local_fs->link_file(inverted_index_src_file_path,
                                             inverted_index_dst_file_path)
                                 .ok()) {
                        status = Status::Error<OS_ERROR>(
                                "fail to create hard link. from={}, to={}, errno={}",
                                inverted_index_src_file_path, inverted_index_dst_file_path,
                                Errno::no());
                        return status;
                    }
                    linked_success_files.push_back(inverted_index_dst_file_path);
                    LOG(INFO) << "success to create hard link. from="
                              << inverted_index_src_file_path << ", "
                              << "to=" << inverted_index_dst_file_path;
                } else {
                    LOG(WARNING) << "skip create hard link to not existed index file="
                                 << inverted_index_src_file_path;
                }
            }
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) {
    DCHECK(is_local());
    bool exists = false;
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = segment_file_path(dir, new_rowset_id, i);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dst_path, &exists));
        if (exists) {
            return Status::Error<FILE_ALREADY_EXIST>("file already exist: {}", dst_path);
        }
        auto src_path = segment_file_path(i);
        RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(src_path, dst_path));
        if (_schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1) {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_dst_file =
                        InvertedIndexDescriptor::get_index_file_name(dst_path);
                std::string inverted_index_src_file =
                        InvertedIndexDescriptor::get_index_file_name(src_path);
                RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(inverted_index_src_file,
                                                                         inverted_index_dst_file));
                LOG(INFO) << "success to copy file. from=" << inverted_index_src_file << ", "
                          << "to=" << inverted_index_dst_file;
            }
        } else {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_src_file_path =
                            InvertedIndexDescriptor::get_index_file_name(
                                    src_path, index_meta->index_id(),
                                    index_meta->get_index_suffix());
                    std::string inverted_index_dst_file_path =
                            InvertedIndexDescriptor::get_index_file_name(
                                    dst_path, index_meta->index_id(),
                                    index_meta->get_index_suffix());
                    RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(
                            inverted_index_src_file_path, inverted_index_dst_file_path));
                    LOG(INFO) << "success to copy file. from=" << inverted_index_src_file_path
                              << ", "
                              << "to=" << inverted_index_dst_file_path;
                }
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
        dest_paths.emplace_back(remote_seg_path);
        local_paths.emplace_back(local_seg_path);
        if (_schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1) {
            if (_schema->has_inverted_index()) {
                std::string remote_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(remote_seg_path);
                std::string local_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(local_seg_path);
                dest_paths.emplace_back(remote_inverted_index_file);
                local_paths.emplace_back(local_inverted_index_file);
            }
        } else {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string remote_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_name(
                                    remote_seg_path, index_meta->index_id(),
                                    index_meta->get_index_suffix());
                    std::string local_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_name(
                                    local_seg_path, index_meta->index_id(),
                                    index_meta->get_index_suffix());
                    dest_paths.emplace_back(remote_inverted_index_file);
                    local_paths.emplace_back(local_inverted_index_file);
                }
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
        io::FileReaderOptions reader_options {
                .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                        : io::FileCachePolicy::NO_CACHE,
                .is_doris_table = true};
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
    auto* local_fs = static_cast<io::LocalFileSystem*>(fs.get());

    auto segments_num = num_segments();
    VLOG_DEBUG << fmt::format("add rowset to binlog. rowset_id={}, segments_num={}",
                              rowset_id().to_string(), segments_num);

    Status status;
    std::vector<string> linked_success_files;
    Defer remove_linked_files {[&]() { // clear linked files if errors happen
        if (!status.ok()) {
            LOG(WARNING) << "will delete linked success files due to error " << status;
            std::vector<io::Path> paths;
            for (auto& file : linked_success_files) {
                paths.emplace_back(file);
                LOG(WARNING) << "will delete linked success file " << file << " due to error";
            }
            static_cast<void>(local_fs->batch_delete(paths));
            LOG(WARNING) << "done delete linked success files due to error " << status;
        }
    }};

    // The publish_txn might fail even if the add_to_binlog success, so we need to check
    // whether a file already exists before linking.
    auto errno_is_file_exists = []() { return Errno::no() == EEXIST; };

    // all segments are in the same directory, so cache binlog_dir without multi times check
    std::string binlog_dir;
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
        if (!local_fs->link_file(seg_file, binlog_file).ok() && !errno_is_file_exists()) {
            status = Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                             seg_file, binlog_file, Errno::no());
            return status;
        }
        linked_success_files.push_back(binlog_file);

        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : _schema->indexes()) {
                if (index.index_type() != IndexType::INVERTED) {
                    continue;
                }
                auto index_id = index.index_id();
                auto index_file = InvertedIndexDescriptor::get_index_file_name(
                        seg_file, index_id, index.get_index_suffix());
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                if (!local_fs->link_file(index_file, binlog_index_file).ok() &&
                    !errno_is_file_exists()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}", index_file,
                            binlog_index_file, Errno::no());
                    return status;
                }
                linked_success_files.push_back(binlog_index_file);
            }
        } else {
            if (_schema->has_inverted_index()) {
                auto index_file = InvertedIndexDescriptor::get_index_file_name(seg_file);
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                if (!local_fs->link_file(index_file, binlog_index_file).ok() &&
                    !errno_is_file_exists()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}", index_file,
                            binlog_index_file, Errno::no());
                    return status;
                }
                linked_success_files.push_back(binlog_index_file);
            }
        }
    }

    return Status::OK();
}

Status BetaRowset::calc_local_file_crc(uint32_t* crc_value, int64_t* file_count) {
    DCHECK(is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    if (fs->type() != io::FileSystemType::LOCAL) {
        return Status::InternalError("should be local file system");
    }

    if (num_segments() < 1) {
        *crc_value = 0x92a8fc17; // magic code from crc32c table
        return Status::OK();
    }

    // 1. pick up all the files including dat file and idx file
    std::vector<io::Path> local_paths;
    for (int i = 0; i < num_segments(); ++i) {
        auto local_seg_path = segment_file_path(i);
        local_paths.emplace_back(local_seg_path);
        if (_schema->get_inverted_index_storage_format() != InvertedIndexStorageFormatPB::V1) {
            if (_schema->has_inverted_index()) {
                std::string local_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(local_seg_path);
                local_paths.emplace_back(local_inverted_index_file);
            }
        } else {
            for (auto& column : _schema->columns()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string local_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_name(
                                    local_seg_path, index_meta->index_id(),
                                    index_meta->get_index_suffix());
                    local_paths.emplace_back(local_inverted_index_file);
                }
            }
        }
    }

    // 2. calculate the md5sum of each file
    auto* local_fs = static_cast<io::LocalFileSystem*>(fs.get());
    DCHECK(local_paths.size() > 0);
    std::vector<std::string> all_file_md5;
    all_file_md5.reserve(local_paths.size());
    for (const auto& file_path : local_paths) {
        DBUG_EXECUTE_IF("fault_inject::BetaRowset::calc_local_file_crc", {
            return Status::Error<OS_ERROR>("fault_inject calc_local_file_crc error");
        });
        std::string file_md5sum;
        auto status = local_fs->md5sum(file_path, &file_md5sum);
        if (!status.ok()) {
            return status;
        }
        VLOG_CRITICAL << fmt::format("calc file_md5sum finished. file_path={}, md5sum={}",
                                     file_path.string(), file_md5sum);
        all_file_md5.emplace_back(std::move(file_md5sum));
    }
    std::sort(all_file_md5.begin(), all_file_md5.end());

    // 3. calculate the crc_value based on all_file_md5
    DCHECK(local_paths.size() == all_file_md5.size());
    *crc_value = 0;
    *file_count = local_paths.size();
    for (int i = 0; i < all_file_md5.size(); i++) {
        *crc_value = crc32c::Extend(*crc_value, all_file_md5[i].data(), all_file_md5[i].size());
    }

    return Status::OK();
}

Status BetaRowset::show_nested_index_file(rapidjson::Value* rowset_value,
                                          rapidjson::Document::AllocatorType& allocator) {
    const auto& fs = _rowset_meta->fs();
    auto storage_format = _schema->get_inverted_index_storage_format();
    const auto* format_str = storage_format == InvertedIndexStorageFormatPB::V1 ? "V1" : "V2";
    auto rs_id = rowset_id().to_string();
    rowset_value->AddMember("rowset_id", rapidjson::Value(rs_id.c_str(), allocator), allocator);
    rowset_value->AddMember("index_storage_format", rapidjson::Value(format_str, allocator),
                            allocator);
    rapidjson::Value segments(rapidjson::kArrayType);
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        rapidjson::Value segment(rapidjson::kObjectType);
        segment.AddMember("segment_id", rapidjson::Value(seg_id).Move(), allocator);

        auto seg_path = segment_file_path(seg_id);
        // std::string convert to path and get parent path
        auto seg_parent_path = std::filesystem::path(seg_path).parent_path();
        auto seg_file_name = std::filesystem::path(seg_path).filename().string();
        auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                fs, seg_parent_path, seg_file_name, storage_format);
        RETURN_IF_ERROR(inverted_index_file_reader->init());
        auto dirs = inverted_index_file_reader->get_all_directories();

        auto add_file_info_to_json = [&](const std::string& path,
                                         rapidjson::Value& json_value) -> Status {
            json_value.AddMember("idx_file_path", rapidjson::Value(path.c_str(), allocator),
                                 allocator);
            int64_t idx_file_size = 0;
            auto st = fs->file_size(path, &idx_file_size);
            if (st != Status::OK()) {
                LOG(WARNING) << "show nested index file get file size error, file: " << path
                             << ", error: " << st.msg();
                return st;
            }
            json_value.AddMember("idx_file_size", rapidjson::Value(idx_file_size).Move(),
                                 allocator);
            return Status::OK();
        };

        auto process_files = [&allocator, &inverted_index_file_reader](
                                     auto& index_meta, rapidjson::Value& indices,
                                     rapidjson::Value& index) -> Status {
            rapidjson::Value files_value(rapidjson::kArrayType);
            std::vector<std::string> files;
            auto ret = inverted_index_file_reader->open(&index_meta);
            if (!ret.has_value()) {
                LOG(INFO) << "InvertedIndexFileReader open error:" << ret.error();
                return Status::InternalError("InvertedIndexFileReader open error");
            }
            using T = std::decay_t<decltype(ret)>;
            auto reader = std::forward<T>(ret).value();
            reader->list(&files);
            for (auto& file : files) {
                rapidjson::Value file_value(rapidjson::kObjectType);
                auto size = reader->fileLength(file.c_str());
                file_value.AddMember("name", rapidjson::Value(file.c_str(), allocator), allocator);
                file_value.AddMember("size", rapidjson::Value(size).Move(), allocator);
                files_value.PushBack(file_value, allocator);
            }
            index.AddMember("files", files_value, allocator);
            indices.PushBack(index, allocator);
            return Status::OK();
        };

        if (storage_format != InvertedIndexStorageFormatPB::V1) {
            auto path = InvertedIndexDescriptor::get_index_file_name(seg_path);
            auto st = add_file_info_to_json(path, segment);
            if (!st.ok()) {
                return st;
            }
            rapidjson::Value indices(rapidjson::kArrayType);
            for (auto& dir : *dirs) {
                rapidjson::Value index(rapidjson::kObjectType);
                auto index_id = dir.first.first;
                auto index_suffix = dir.first.second;
                index.AddMember("index_id", rapidjson::Value(index_id).Move(), allocator);
                index.AddMember("index_suffix", rapidjson::Value(index_suffix.c_str(), allocator),
                                allocator);

                rapidjson::Value files_value(rapidjson::kArrayType);
                std::vector<std::string> files;
                doris::TabletIndexPB index_pb;
                index_pb.set_index_id(index_id);
                index_pb.set_index_suffix_name(index_suffix);
                TabletIndex index_meta;
                index_meta.init_from_pb(index_pb);

                auto status = process_files(index_meta, indices, index);
                if (!status.ok()) {
                    return status;
                }
            }
            segment.AddMember("indices", indices, allocator);
            segments.PushBack(segment, allocator);
        } else {
            rapidjson::Value indices(rapidjson::kArrayType);
            for (auto column : _rowset_meta->tablet_schema()->columns()) {
                const auto* index_meta = _rowset_meta->tablet_schema()->get_inverted_index(*column);
                if (index_meta == nullptr) {
                    continue;
                }
                rapidjson::Value index(rapidjson::kObjectType);
                auto index_id = index_meta->index_id();
                auto index_suffix = index_meta->get_index_suffix();
                index.AddMember("index_id", rapidjson::Value(index_id).Move(), allocator);
                index.AddMember("index_suffix", rapidjson::Value(index_suffix.c_str(), allocator),
                                allocator);
                auto path = InvertedIndexDescriptor::get_index_file_name(seg_path, index_id,
                                                                         index_suffix);
                auto st = add_file_info_to_json(path, index);
                if (!st.ok()) {
                    return st;
                }

                auto status = process_files(*index_meta, indices, index);
                if (!status.ok()) {
                    return status;
                }
            }
            segment.AddMember("indices", indices, allocator);
            segments.PushBack(segment, allocator);
        }
    }
    rowset_value->AddMember("segments", segments, allocator);
    return Status::OK();
}

} // namespace doris
