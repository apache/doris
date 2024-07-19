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
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/crc32c.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"

namespace doris {
using namespace ErrorCode;

std::string BetaRowset::local_segment_path_segcompacted(const std::string& tablet_path,
                                                        const RowsetId& rowset_id, int64_t begin,
                                                        int64_t end) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{begin_seg}-{end_seg}.dat
    return fmt::format("{}/{}_{}-{}.dat", tablet_path, rowset_id.to_string(), begin, end);
}

BetaRowset::BetaRowset(const TabletSchemaSPtr& schema, const RowsetMetaSharedPtr& rowset_meta,
                       std::string tablet_path)
        : Rowset(schema, rowset_meta, std::move(tablet_path)) {}

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
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed, resource_id={}",
                                          _rowset_meta->resource_id());
    }

    if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        auto indices = _schema->indexes();
        for (auto& index : indices) {
            for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
                auto seg_path = DORIS_TRY(segment_path(seg_id));
                int64_t file_size = 0;

                std::string inverted_index_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                index.index_id(), index.get_index_suffix());
                RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
                *index_size += file_size;
            }
        }
    } else {
        for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
            auto seg_path = DORIS_TRY(segment_path(seg_id));
            int64_t file_size = 0;

            std::string inverted_index_file_path = InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
            RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
            *index_size += file_size;
        }
    }
    return Status::OK();
}

void BetaRowset::clear_inverted_index_cache() {
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_path(i);
        if (!seg_path) {
            continue;
        }

        auto index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(*seg_path);
        for (const auto& column : tablet_schema()->columns()) {
            const TabletIndex* index_meta = tablet_schema()->get_inverted_index(*column);
            if (index_meta) {
                auto inverted_index_file_cache_key =
                        InvertedIndexDescriptor::get_index_file_cache_key(
                                index_path_prefix, index_meta->index_id(),
                                index_meta->get_index_suffix());
                (void)segment_v2::InvertedIndexSearcherCache::instance()->erase(
                        inverted_index_file_cache_key);
            }
        }
    }
}

Status BetaRowset::get_segments_size(std::vector<size_t>* segments_size) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed, resource_id={}",
                                          _rowset_meta->resource_id());
    }

    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));
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
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }

    DCHECK(seg_id >= 0);
    auto seg_path = DORIS_TRY(segment_path(seg_id));
    io::FileReaderOptions reader_options {
            .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                    : io::FileCachePolicy::NO_CACHE,
            .is_doris_table = true,
            .cache_base_path = "",
            .file_size = _rowset_meta->segment_file_size(seg_id),
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
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::OK();
    }

    // TODO should we close and remove all segment reader first?
    VLOG_NOTICE << "begin to remove files in rowset " << rowset_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    // If the rowset was removed, it need to remove the fds in segment cache directly
    clear_cache();

    bool success = true;
    Status st;
    const auto& fs = io::global_local_filesystem();
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        LOG(INFO) << "deleting " << seg_path;
        st = fs->delete_file(seg_path);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
            success = false;
        }

        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    st = fs->delete_file(inverted_index_file);
                    if (!st.ok()) {
                        LOG(WARNING) << st.to_string();
                        success = false;
                    }
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
                st = fs->delete_file(inverted_index_file);
                if (!st.ok()) {
                    LOG(WARNING) << st.to_string();
                    success = false;
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
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    const auto& local_fs = io::global_local_filesystem();
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

    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path =
                local_segment_path(dir, new_rowset_id.to_string(), i + new_rowset_start_seg_id);
        bool dst_path_exist = false;
        if (!local_fs->exists(dst_path, &dst_path_exist).ok() || dst_path_exist) {
            status = Status::Error<FILE_ALREADY_EXIST>(
                    "failed to create hard link, file already exist: {}", dst_path);
            return status;
        }
        auto src_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
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
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : _schema->indexes()) {
                if (index.index_type() != IndexType::INVERTED) {
                    continue;
                }
                auto index_id = index.index_id();
                if (without_index_uids != nullptr && without_index_uids->count(index_id)) {
                    continue;
                }
                std::string inverted_index_src_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path),
                                index_id, index.get_index_suffix());
                std::string inverted_index_dst_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path),
                                index_id, index.get_index_suffix());
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
        } else {
            if (_schema->has_inverted_index() &&
                (without_index_uids == nullptr || without_index_uids->empty())) {
                std::string inverted_index_file_src =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path));
                std::string inverted_index_file_dst =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path));
                bool index_dst_path_exist = false;

                if (!local_fs->exists(inverted_index_file_dst, &index_dst_path_exist).ok() ||
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
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    bool exists = false;
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = local_segment_path(dir, new_rowset_id.to_string(), i);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dst_path, &exists));
        if (exists) {
            return Status::Error<FILE_ALREADY_EXIST>("file already exist: {}", dst_path);
        }
        auto src_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(src_path, dst_path));
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_src_file_path =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(src_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    std::string inverted_index_dst_file_path =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(dst_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(
                            inverted_index_src_file_path, inverted_index_dst_file_path));
                    LOG(INFO) << "success to copy file. from=" << inverted_index_src_file_path
                              << ", "
                              << "to=" << inverted_index_dst_file_path;
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_src_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path));
                std::string inverted_index_dst_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path));
                RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(inverted_index_src_file,
                                                                         inverted_index_dst_file));
                LOG(INFO) << "success to copy file. from=" << inverted_index_src_file << ", "
                          << "to=" << inverted_index_dst_file;
            }
        }
    }
    return Status::OK();
}

Status BetaRowset::upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    if (num_segments() < 1) {
        return Status::OK();
    }
    std::vector<io::Path> local_paths;
    local_paths.reserve(num_segments());
    std::vector<io::Path> dest_paths;
    dest_paths.reserve(num_segments());
    for (int i = 0; i < num_segments(); ++i) {
        // Note: Here we use relative path for remote.
        auto remote_seg_path = dest_fs.remote_segment_path(_rowset_meta->tablet_id(),
                                                           new_rowset_id.to_string(), i);
        auto local_seg_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        dest_paths.emplace_back(remote_seg_path);
        local_paths.emplace_back(local_seg_path);
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string remote_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(
                                            remote_seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    std::string local_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(
                                            local_seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    dest_paths.emplace_back(remote_inverted_index_file);
                    local_paths.emplace_back(local_inverted_index_file);
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string remote_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        remote_seg_path));
                std::string local_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        local_seg_path));
                dest_paths.emplace_back(remote_inverted_index_file);
                local_paths.emplace_back(local_inverted_index_file);
            }
        }
    }
    auto st = dest_fs.fs->batch_upload(local_paths, dest_paths);
    if (st.ok()) {
        DorisMetrics::instance()->upload_rowset_count->increment(1);
        DorisMetrics::instance()->upload_total_byte->increment(data_disk_size());
    } else {
        DorisMetrics::instance()->upload_fail_count->increment(1);
    }
    return st;
}

Status BetaRowset::check_file_exist() {
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::InternalError("fs is not initialized, resource_id={}",
                                     _rowset_meta->resource_id());
    }

    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = DORIS_TRY(segment_path(i));
        bool seg_file_exist = false;
        RETURN_IF_ERROR(fs->exists(seg_path, &seg_file_exist));
        if (!seg_file_exist) {
            return Status::InternalError("data file not existed: {}, rowset_id={}", seg_path,
                                         rowset_id().to_string());
        }
    }

    return Status::OK();
}

Status BetaRowset::check_current_rowset_segment() {
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::InternalError("fs is not initialized, resource_id={}",
                                     _rowset_meta->resource_id());
    }

    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));

        std::shared_ptr<segment_v2::Segment> segment;
        io::FileReaderOptions reader_options {
                .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                        : io::FileCachePolicy::NO_CACHE,
                .is_doris_table = true,
                .cache_base_path {},
                .file_size = _rowset_meta->segment_file_size(seg_id),
        };
        auto s = segment_v2::Segment::open(fs, seg_path, seg_id, rowset_id(), _schema,
                                           reader_options, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "segment can not be opened. file=" << seg_path;
            return s;
        }
    }

    return Status::OK();
}

Status BetaRowset::add_to_binlog() {
    // FIXME(Drogon): not only local file system
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    const auto& fs = io::global_local_filesystem();

    // all segments are in the same directory, so cache binlog_dir without multi times check
    std::string binlog_dir;

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
            static_cast<void>(fs->batch_delete(paths));
            LOG(WARNING) << "done delete linked success files due to error " << status;
        }
    }};

    for (int i = 0; i < segments_num; ++i) {
        auto seg_file = local_segment_path(_tablet_path, rowset_id().to_string(), i);

        if (binlog_dir.empty()) {
            binlog_dir = std::filesystem::path(seg_file).parent_path().append("_binlog").string();

            bool exists = true;
            RETURN_IF_ERROR(fs->exists(binlog_dir, &exists));
            if (!exists) {
                RETURN_IF_ERROR(fs->create_directory(binlog_dir));
            }
        }

        auto binlog_file =
                (std::filesystem::path(binlog_dir) / std::filesystem::path(seg_file).filename())
                        .string();
        VLOG_DEBUG << "link " << seg_file << " to " << binlog_file;
        if (!fs->link_file(seg_file, binlog_file).ok()) {
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
                auto index_file = InvertedIndexDescriptor::get_index_file_path_v1(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_file), index_id,
                        index.get_index_suffix());
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                RETURN_IF_ERROR(fs->link_file(index_file, binlog_index_file));
                linked_success_files.push_back(binlog_index_file);
            }
        } else {
            if (_schema->has_inverted_index()) {
                auto index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_file));
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                RETURN_IF_ERROR(fs->link_file(index_file, binlog_index_file));
                linked_success_files.push_back(binlog_index_file);
            }
        }
    }

    return Status::OK();
}

Status BetaRowset::calc_file_crc(uint32_t* crc_value, int64_t* file_count) {
    const auto& fs = _rowset_meta->fs();
    DBUG_EXECUTE_IF("fault_inject::BetaRowset::calc_file_crc",
                    { return Status::Error<OS_ERROR>("fault_inject calc_file_crc error"); });
    if (num_segments() < 1) {
        *crc_value = 0x92a8fc17; // magic code from crc32c table
        return Status::OK();
    }

    // 1. pick up all the files including dat file and idx file
    std::vector<io::Path> file_paths;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));
        file_paths.emplace_back(seg_path);
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                const TabletIndex* index_meta = _schema->get_inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    file_paths.emplace_back(std::move(inverted_index_file));
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
                file_paths.emplace_back(std::move(inverted_index_file));
            }
        }
    }
    *crc_value = 0;
    *file_count = file_paths.size();
    if (!is_local()) {
        return Status::OK();
    }

    // 2. calculate the md5sum of each file
    const auto& local_fs = io::global_local_filesystem();
    DCHECK(!file_paths.empty());
    std::vector<std::string> all_file_md5;
    all_file_md5.reserve(file_paths.size());
    for (const auto& file_path : file_paths) {
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
    DCHECK(file_paths.size() == all_file_md5.size());
    for (auto& i : all_file_md5) {
        *crc_value = crc32c::Extend(*crc_value, i.data(), i.size());
    }

    return Status::OK();
}

} // namespace doris
