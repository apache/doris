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

#pragma once

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <functional>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "cloud/config.h"
#include "common/status.h"
#include "io/fs/encrypted_fs_factory.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/packed_file_system.h"
#include "olap/olap_define.h"
#include "olap/partial_update_info.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"

namespace doris {

class RowsetWriterContextBuilder;
using RowsetWriterContextBuilderSharedPtr = std::shared_ptr<RowsetWriterContextBuilder>;
class DataDir;
class Tablet;
class FileWriterCreator;
class SegmentCollector;

struct RowsetWriterContext {
    RowsetWriterContext() : schema_lock(new std::mutex) {
        load_id.set_hi(0);
        load_id.set_lo(0);
    }

    RowsetId rowset_id;
    int64_t tablet_id {0};
    int32_t tablet_schema_hash {0};
    int64_t index_id {0};
    int64_t partition_id {0};
    RowsetTypePB rowset_type {BETA_ROWSET};

    TabletSchemaSPtr tablet_schema;
    // PREPARED/COMMITTED for pending rowset
    // VISIBLE for non-pending rowset
    RowsetStatePB rowset_state {PREPARED};
    // properties for non-pending rowset
    Version version {0, 0};

    // properties for pending rowset
    int64_t txn_id {0};
    int64_t txn_expiration {0}; // For cloud mode
    PUniqueId load_id;
    TabletUid tablet_uid {0, 0};
    // indicate whether the data among segments is overlapping.
    // default is OVERLAP_UNKNOWN.
    SegmentsOverlapPB segments_overlap {OVERLAP_UNKNOWN};
    // segment file use uint32 to represent row number, therefore the maximum is UINT32_MAX.
    // the default is set to INT32_MAX to avoid overflow issue when casting from uint32_t to int.
    // test cases can change this value to control flush timing
    uint32_t max_rows_per_segment = INT32_MAX;
    // not owned, point to the data dir of this rowset
    // for checking disk capacity when write data to disk.
    // ATTN: not support for RowsetConvertor.
    // (because it hard to refactor, and RowsetConvertor will be deprecated in future)
    DataDir* data_dir = nullptr;

    int64_t newest_write_timestamp = -1;
    bool enable_unique_key_merge_on_write = false;
    // store column_unique_id to do index compaction
    std::set<int32_t> columns_to_do_index_compaction;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
    // need to figure out the sub type of compaction
    ReaderType compaction_type = ReaderType::UNKNOWN;
    BaseTabletSPtr tablet = nullptr;

    std::shared_ptr<MowContext> mow_context;
    std::shared_ptr<FileWriterCreator> file_writer_creator;
    std::shared_ptr<SegmentCollector> segment_collector;

    // memtable_on_sink_support_index_v2 = true, we will create SinkFileWriter to send inverted index file
    bool memtable_on_sink_support_index_v2 = false;

    /// begin file cache opts
    bool write_file_cache = false;
    bool is_hot_data = false;
    uint64_t file_cache_ttl_sec = 0;
    uint64_t approximate_bytes_to_write = 0;
    // If true, compaction output only writes index files to file cache, not data files
    bool compaction_output_write_index_only = false;
    /// end file cache opts

    // segcompaction for this RowsetWriter, only enabled when importing data
    bool enable_segcompaction = false;

    std::shared_ptr<PartialUpdateInfo> partial_update_info;

    bool is_transient_rowset_writer = false;

    // Intent flag: caller can actively turn merge-file feature on/off for this rowset.
    // This describes whether we *want* to try small-file merging.
    bool allow_packed_file = true;

    // Effective flag: whether this context actually ends up using MergeFileSystem for writes.
    // This is decided inside fs() based on enable_merge_file plus other conditions
    // (cloud mode, S3 filesystem, V1 inverted index, global config, etc.), and once
    // set to true it remains stable even if config::enable_merge_file changes later.
    mutable bool packed_file_active = false;

    // Cached FileSystem instance to ensure consistency across multiple fs() calls.
    // This prevents creating multiple MergeFileSystem instances and ensures
    // packed_file_active flag remains consistent.
    mutable io::FileSystemSPtr _cached_fs = nullptr;

    // For collect segment statistics for compaction
    std::vector<RowsetReaderSharedPtr> input_rs_readers;

    // TODO(lihangyu) remove this lock
    // In semi-structure senario tablet_schema will be updated concurrently,
    // this lock need to be held when update.Use shared_ptr to avoid delete copy contructor
    std::shared_ptr<std::mutex> schema_lock;

    int64_t compaction_level = 0;

    // For local rowset
    std::string tablet_path;

    // For remote rowset
    std::optional<StorageResource> storage_resource;

    std::optional<EncryptionAlgorithmPB> encrypt_algorithm;

    std::string job_id;

    bool is_local_rowset() const { return !storage_resource; }

    std::string segment_path(int seg_id) const {
        if (is_local_rowset()) {
            return local_segment_path(tablet_path, rowset_id.to_string(), seg_id);
        } else {
            return storage_resource->remote_segment_path(tablet_id, rowset_id.to_string(), seg_id);
        }
    }

    io::FileSystemSPtr fs() const {
        // Return cached instance if available to ensure consistency across multiple calls
        if (_cached_fs != nullptr) {
            return _cached_fs;
        }

        auto fs = [this]() -> io::FileSystemSPtr {
            if (is_local_rowset()) {
                return io::global_local_filesystem();
            } else {
                return storage_resource->fs;
            }
        }();

        bool is_s3_fs = fs->type() == io::FileSystemType::S3;

        auto algorithm = encrypt_algorithm;

        if (!algorithm.has_value()) {
#ifndef BE_TEST
            constexpr std::string_view msg =
                    "RowsetWriterContext::determine_encryption is not called when creating this "
                    "RowsetWriterContext, it will result in encrypted rowsets left unencrypted";
            auto st = Status::InternalError(msg);

            LOG(WARNING) << st;
            DCHECK(false) << st;
#else
            algorithm = EncryptionAlgorithmPB::PLAINTEXT;
#endif
        }

        // Apply encryption if needed
        if (algorithm.has_value()) {
            fs = io::make_file_system(fs, algorithm.value());
        }

        // Apply packed file system for write path if enabled
        // Create empty index_map for write path
        // Index information will be populated after write completes
        bool has_v1_inverted_index = tablet_schema != nullptr &&
                                     tablet_schema->has_inverted_index() &&
                                     tablet_schema->get_inverted_index_storage_format() ==
                                             InvertedIndexStorageFormatPB::V1;

        if (has_v1_inverted_index && allow_packed_file && config::enable_packed_file) {
            static constexpr std::string_view kMsg =
                    "Disable packed file for V1 inverted index tablet to avoid missing index "
                    "metadata (temporary workaround)";
            LOG(INFO) << kMsg << ", tablet_id=" << tablet_id << ", rowset_id=" << rowset_id;
        }

        // Only enable merge file for S3 file system, not for HDFS or other remote file systems
        packed_file_active = allow_packed_file && config::is_cloud_mode() &&
                             config::enable_packed_file && !has_v1_inverted_index && is_s3_fs;

        if (packed_file_active) {
            io::PackedAppendContext append_info;
            append_info.tablet_id = tablet_id;
            append_info.rowset_id = rowset_id.to_string();
            append_info.txn_id = txn_id;
            append_info.expiration_time = file_cache_ttl_sec > 0 && newest_write_timestamp > 0
                                                  ? newest_write_timestamp + file_cache_ttl_sec
                                                  : 0;
            fs = std::make_shared<io::PackedFileSystem>(fs, append_info);
        }

        // Cache the result to ensure consistency across multiple calls
        _cached_fs = fs;
        return fs;
    }

    io::FileSystem& fs_ref() const { return *fs(); }

    io::FileWriterOptions get_file_writer_options(bool is_index_file = false) {
        bool should_write_cache = write_file_cache;
        // If configured to only write index files to cache, skip cache for data files
        if (compaction_output_write_index_only && !is_index_file) {
            should_write_cache = false;
        }

        return io::FileWriterOptions {.write_file_cache = should_write_cache,
                                      .is_cold_data = is_hot_data,
                                      .file_cache_expiration_time = file_cache_ttl_sec,
                                      .approximate_bytes_to_write = approximate_bytes_to_write};
    }
};

} // namespace doris
