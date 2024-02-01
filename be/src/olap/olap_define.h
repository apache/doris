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

#include <stdint.h>

#include <cstdlib>
#include <sstream>
#include <string>

#include "common/status.h"

namespace doris {
// Here are some unified definitions
// The length of the Signature returned by the command
static const uint32_t OLAP_COMMAND_SIGNATURE_LEN = 4;
// Maximum path length
static const uint32_t OLAP_MAX_PATH_LEN = 512;
// Maximum length of each row block after compression
static const uint32_t OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE = 1024 * 1024 * 20;
// The maximum length of each row block before compression, which is the maximum length of the buf
static const uint32_t OLAP_DEFAULT_MAX_UNPACKED_ROW_BLOCK_SIZE = 1024 * 1024 * 100;
// The block size of the column storage file needs to be strictly controlled as it may be fully loaded into memory. Here, it is defined as 256MB
static const uint32_t OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE = 268435456;
// Scalability of column storage file size
static const double OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE = 0.9;
// In column storage files, data is compressed in blocks, with the default size of each block before compression
static const uint32_t OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE = 10 * 1024;
// The dictionary size threshold for using dictionary encoding for strings in column storage files
// This is a percentage, and dictionary encoding is enabled when the dictionary size/original data size is less than this percentage
static const uint32_t OLAP_DEFAULT_COLUMN_DICT_KEY_SIZE_THRESHOLD = 80; // 30%
// Size of LRU Cache Key
static const size_t OLAP_LRU_CACHE_MAX_KEY_LENGTH = OLAP_MAX_PATH_LEN * 2;

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;
// Default candidate size when executing be/ce
static constexpr uint32_t OLAP_COMPACTION_DEFAULT_CANDIDATE_SIZE = 10;

// the max length supported for varchar type
static const uint16_t OLAP_VARCHAR_MAX_LENGTH = 65535;

// the max length supported for string type 2GB
static const uint32_t OLAP_STRING_MAX_LENGTH = 2147483647;

// the max length supported for jsonb type 2G
static const uint32_t OLAP_JSONB_MAX_LENGTH = 2147483647;

// the max length supported for struct, but excluding the length of its subtypes.
static const uint16_t OLAP_STRUCT_MAX_LENGTH = 65535;

// the max length supported for array
static const uint16_t OLAP_ARRAY_MAX_LENGTH = 65535;

// the max length supported for map
static const uint16_t OLAP_MAP_MAX_LENGTH = 65535;

// the max bytes for stored string length
using StringOffsetType = uint32_t;
using StringLengthType = uint32_t;
using VarcharLengthType = uint16_t;
static const uint16_t OLAP_STRING_MAX_BYTES = sizeof(StringLengthType);
static const uint16_t OLAP_VARCHAR_MAX_BYTES = sizeof(VarcharLengthType);
// the max bytes for stored array length
static const uint16_t OLAP_ARRAY_MAX_BYTES = OLAP_ARRAY_MAX_LENGTH;

static constexpr uint16_t MAX_ZONE_MAP_INDEX_SIZE = 512;

enum OLAPDataVersion {
    OLAP_V1 = 0,
    DORIS_V1 = 1,
};

// Different types of folder names under storage_root_path
static const std::string MINI_PREFIX = "mini_download";
static const std::string CLUSTER_ID_PREFIX = "cluster_id";
static const std::string DATA_PREFIX = "data";
static const std::string DPP_PREFIX = "dpp_download";
static const std::string SNAPSHOT_PREFIX = "snapshot";
static const std::string TRASH_PREFIX = "trash";
static const std::string UNUSED_PREFIX = "unused";
static const std::string ERROR_LOG_PREFIX = "error_log";
static const std::string PENDING_DELTA_PREFIX = "pending_delta";
static const std::string INCREMENTAL_DELTA_PREFIX = "incremental_delta";
static const std::string CLONE_PREFIX = "clone";

// define paths
static inline std::string remote_tablet_path(int64_t tablet_id) {
    // data/{tablet_id}
    return fmt::format("{}/{}", DATA_PREFIX, tablet_id);
}
static inline std::string remote_tablet_meta_path(int64_t tablet_id, int64_t replica_id,
                                                  int64_t cooldown_term) {
    // data/{tablet_id}/{replica_id}.{cooldown_term}.meta
    return fmt::format("{}/{}.{}.meta", remote_tablet_path(tablet_id), replica_id, cooldown_term);
}

static const std::string TABLET_UID = "tablet_uid";
static const std::string STORAGE_NAME = "storage_name";

static const int32_t OLAP_DATA_VERSION_APPLIED = DORIS_V1;

static const uint32_t MAX_POSITION_SIZE = 16;

static const uint32_t MAX_STATISTIC_LENGTH = 34;

static const uint32_t MAX_OP_IN_FIELD_NUM = 100;

static const uint64_t GB_EXCHANGE_BYTE = 1024 * 1024 * 1024;

// bloom filter fpp
static const double BLOOM_FILTER_DEFAULT_FPP = 0.05;

enum ColumnFamilyIndex {
    DEFAULT_COLUMN_FAMILY_INDEX = 0,
    DORIS_COLUMN_FAMILY_INDEX,
    META_COLUMN_FAMILY_INDEX,
};

enum class DataWriteType {
    TYPE_DEFAULT = 0,
    TYPE_DIRECT,
    TYPE_SCHEMA_CHANGE,
    TYPE_COMPACTION,
};

static const char* const HINIS_KEY_SEPARATOR = ";";
static const char* const HINIS_KEY_PAIR_SEPARATOR = "|";
static const char* const HINIS_KEY_GROUP_SEPARATOR = "&";

static const std::string DEFAULT_COLUMN_FAMILY = "default";
static const std::string DORIS_COLUMN_FAMILY = "doris";
static const std::string META_COLUMN_FAMILY = "meta";
static const std::string END_ROWSET_ID = "end_rowset_id";
static const std::string CONVERTED_FLAG = "true";
static const std::string TABLET_CONVERT_FINISHED = "tablet_convert_finished";
const std::string TABLET_ID_KEY = "tablet_id";
const std::string TABLE_ID_KEY = "table_id";
const std::string ENABLE_BYTE_TO_BASE64 = "byte_to_base64";
const std::string TABLET_ID_PREFIX = "t_";
const std::string ROWSET_ID_PREFIX = "s_";
const std::string REMOTE_ROWSET_GC_PREFIX = "gc_";
const std::string REMOTE_TABLET_GC_PREFIX = "tgc_";

// Declare copy constructor and equal operator as private
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(type_t) \
    type_t& operator=(const type_t&);    \
    type_t(const type_t&);
#endif

#define SAFE_DELETE(ptr)      \
    do {                      \
        if (nullptr != ptr) { \
            delete ptr;       \
            ptr = nullptr;    \
        }                     \
    } while (0)

#define SAFE_DELETE_ARRAY(ptr) \
    do {                       \
        if (nullptr != ptr) {  \
            delete[] ptr;      \
            ptr = nullptr;     \
        }                      \
    } while (0)

#define SAFE_STOP(ptr)        \
    do {                      \
        if (nullptr != ptr) { \
            ptr->stop();      \
        }                     \
    } while (0)

#define SAFE_SHUTDOWN(ptr)    \
    do {                      \
        if (nullptr != ptr) { \
            ptr->shutdown();  \
        }                     \
    } while (0)

#ifndef BUILD_VERSION
#define BUILD_VERSION "Unknown"
#endif

} // namespace doris
