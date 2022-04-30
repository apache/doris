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
// 以下是一些统一的define
// 命令返回的Signature的长度
static const uint32_t OLAP_COMMAND_SIGNATURE_LEN = 4;
// 最大全路径长度
static const uint32_t OLAP_MAX_PATH_LEN = 512;
// 每个row block压缩之后的最大长度
static const uint32_t OLAP_DEFAULT_MAX_PACKED_ROW_BLOCK_SIZE = 1024 * 1024 * 20;
// 每个row block压缩前的最大长度，也就是buf的最大长度
static const uint32_t OLAP_DEFAULT_MAX_UNPACKED_ROW_BLOCK_SIZE = 1024 * 1024 * 100;
// 列存储文件的块大小,由于可能会被全部载入内存,所以需要严格控制大小, 这里定义为256MB
static const uint32_t OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE = 268435456;
// 列存储文件大小的伸缩性
static const double OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE = 0.9;
// 在列存储文件中, 数据分块压缩, 每个块的默认压缩前的大小
static const uint32_t OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE = 10 * 1024;
// 在列存储文件中, 对字符串使用字典编码的字典大小门限
// 此为百分比, 字典大小/原数据大小小于该百分比时, 启用字典编码
static const uint32_t OLAP_DEFAULT_COLUMN_DICT_KEY_SIZE_THRESHOLD = 80; // 30%
// LRU Cache Key的大小
static const size_t OLAP_LRU_CACHE_MAX_KEY_LENGTH = OLAP_MAX_PATH_LEN * 2;

static const uint64_t OLAP_FIX_HEADER_MAGIC_NUMBER = 0;
// 执行be/ce时默认的候选集大小
static constexpr uint32_t OLAP_COMPACTION_DEFAULT_CANDIDATE_SIZE = 10;

// the max length supported for varchar type
static const uint16_t OLAP_VARCHAR_MAX_LENGTH = 65535;

// the max length supported for string type 2GB
static const uint32_t OLAP_STRING_MAX_LENGTH = 2147483647;

// the max length supported for array
static const uint16_t OLAP_ARRAY_MAX_LENGTH = 65535;

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

// storage_root_path下不同类型文件夹名称
static const std::string MINI_PREFIX = "/mini_download";
static const std::string CLUSTER_ID_PREFIX = "/cluster_id";
static const std::string DATA_PREFIX = "/data";
static const std::string STORAGE_PARAM_PREFIX = "/storage_param";
static const std::string REMOTE_FILE_PARAM = "/remote_file_param";
static const std::string DPP_PREFIX = "/dpp_download";
static const std::string SNAPSHOT_PREFIX = "/snapshot";
static const std::string TRASH_PREFIX = "/trash";
static const std::string UNUSED_PREFIX = "/unused";
static const std::string ERROR_LOG_PREFIX = "/error_log";
static const std::string PENDING_DELTA_PREFIX = "/pending_delta";
static const std::string INCREMENTAL_DELTA_PREFIX = "/incremental_delta";
static const std::string CLONE_PREFIX = "/clone";

static const std::string TABLET_UID = "tablet_uid";
static const std::string STORAGE_NAME = "storage_name";

static const int32_t OLAP_DATA_VERSION_APPLIED = DORIS_V1;

static const uint32_t MAX_POSITION_SIZE = 16;

static const uint32_t MAX_STATISTIC_LENGTH = 34;

static const uint32_t MAX_OP_IN_FIELD_NUM = 100;

static const uint64_t GB_EXCHANGE_BYTE = 1024 * 1024 * 1024;

// bloom filter fpp
static const double BLOOM_FILTER_DEFAULT_FPP = 0.05;

#define OLAP_GOTO(label) goto label

enum ColumnFamilyIndex {
    DEFAULT_COLUMN_FAMILY_INDEX = 0,
    DORIS_COLUMN_FAMILY_INDEX,
    META_COLUMN_FAMILY_INDEX,
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
const std::string ENABLE_BYTE_TO_BASE64 = "byte_to_base64";
const std::string TABLET_ID_PREFIX = "t_";
const std::string ROWSET_ID_PREFIX = "s_";

#if defined(__GNUC__)
#define OLAP_LIKELY(x) __builtin_expect((x), 1)
#define OLAP_UNLIKELY(x) __builtin_expect((x), 0)
#else
#define OLAP_LIKELY(x)
#define OLAP_UNLIKELY(x)
#endif

#ifndef RETURN_NOT_OK
#define RETURN_NOT_OK(s)               \
    do {                               \
        Status _s = (s);               \
        if (OLAP_UNLIKELY(!_s.ok())) { \
            return _s;                 \
        }                              \
    } while (0);
#endif

#ifndef RETURN_NOT_OK_LOG
#define RETURN_NOT_OK_LOG(s, msg)                          \
    do {                                                   \
        Status _s = (s);                                   \
        if (OLAP_UNLIKELY(!_s)) {                          \
            LOG(WARNING) << (msg) << "[res=" << _s << "]"; \
            return _s;                                     \
        }                                                  \
    } while (0);
#endif

// Declare copy constructor and equal operator as private
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(type_t) \
    type_t& operator=(const type_t&);    \
    type_t(const type_t&);
#endif

// 没有使用的变量不报warning
#define OLAP_UNUSED_ARG(a) (void)(a)

// thread-safe(gcc only) method for obtaining singleton
#define DECLARE_SINGLETON(classname)     \
public:                                  \
    static classname* instance() {       \
        classname* p_instance = nullptr; \
        try {                            \
            static classname s_instance; \
            p_instance = &s_instance;    \
        } catch (...) {                  \
            p_instance = nullptr;        \
        }                                \
        return p_instance;               \
    }                                    \
                                         \
protected:                               \
    classname();                         \
                                         \
private:                                 \
    ~classname();

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

#ifndef BUILD_VERSION
#define BUILD_VERSION "Unknown"
#endif

} // namespace doris