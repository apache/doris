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

#ifndef DORIS_BE_SRC_OLAP_OLAP_DEFINE_H
#define DORIS_BE_SRC_OLAP_OLAP_DEFINE_H

#include <stdint.h>

#include <cstdlib>
#include <sstream>
#include <string>

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
static const uint16_t OLAP_STRING_MAX_LENGTH = 65535;

// the max bytes for stored string length
using StringOffsetType = uint32_t;
using StringLengthType = uint16_t;
static const uint16_t OLAP_STRING_MAX_BYTES = sizeof(StringLengthType);

enum OLAPDataVersion {
    OLAP_V1 = 0,
    DORIS_V1 = 1,
};

// storage_root_path下不同类型文件夹名称
static const std::string MINI_PREFIX = "/mini_download";
static const std::string CLUSTER_ID_PREFIX = "/cluster_id";
static const std::string DATA_PREFIX = "/data";
static const std::string DPP_PREFIX = "/dpp_download";
static const std::string SNAPSHOT_PREFIX = "/snapshot";
static const std::string TRASH_PREFIX = "/trash";
static const std::string UNUSED_PREFIX = "/unused";
static const std::string ERROR_LOG_PREFIX = "/error_log";
static const std::string PENDING_DELTA_PREFIX = "/pending_delta";
static const std::string INCREMENTAL_DELTA_PREFIX = "/incremental_delta";
static const std::string CLONE_PREFIX = "/clone";

static const int32_t OLAP_DATA_VERSION_APPLIED = DORIS_V1;

static const uint32_t MAX_POSITION_SIZE = 16;

static const uint32_t MAX_STATISTIC_LENGTH = 34;

static const uint32_t MAX_OP_IN_FIELD_NUM = 100;

static const uint64_t GB_EXCHANGE_BYTE = 1024 * 1024 * 1024;

// bloom filter fpp
static const double BLOOM_FILTER_DEFAULT_FPP = 0.05;

#define OLAP_GOTO(label) goto label

// OLAP的函数的返回值的定义. 当一个函数在调用下层函数错误时,
// 可以根据自己的需要选择直接返回下层函数的OLAPStatus,
// 或者返回自己的一个新OLAPStatus. 但是一个类只能返回自己对应的Status或者公共OLAPStatus,
// 例如_OLAP_ERR_FETCH*系列只能在FetchHandler中返回, 这样便于管理, 同时也方便查找最初的错误发生地.
enum OLAPStatus {
    OLAP_SUCCESS = 0,

    // other errors except following errors
    OLAP_ERR_OTHER_ERROR = -1,
    OLAP_REQUEST_FAILED = -2,

    // system error codessuch as file system memory and other system call failures
    // [-100, -200)
    OLAP_ERR_OS_ERROR = -100,
    OLAP_ERR_DIR_NOT_EXIST = -101,
    OLAP_ERR_FILE_NOT_EXIST = -102,
    OLAP_ERR_CREATE_FILE_ERROR = -103,
    OLAP_ERR_MALLOC_ERROR = -104,
    OLAP_ERR_STL_ERROR = -105,
    OLAP_ERR_IO_ERROR = -106,
    OLAP_ERR_MUTEX_ERROR = -107,
    OLAP_ERR_PTHREAD_ERROR = -108,
    OLAP_ERR_NETWORK_ERROR = -109,
    OLAP_ERR_UB_FUNC_ERROR = -110,
    OLAP_ERR_COMPRESS_ERROR = -111,
    OLAP_ERR_DECOMPRESS_ERROR = -112,
    OLAP_ERR_UNKNOWN_COMPRESSION_TYPE = -113,
    OLAP_ERR_MMAP_ERROR = -114,
    OLAP_ERR_RWLOCK_ERROR = -115,
    OLAP_ERR_READ_UNENOUGH = -116,
    OLAP_ERR_CANNOT_CREATE_DIR = -117,
    OLAP_ERR_UB_NETWORK_ERROR = -118,
    OLAP_ERR_FILE_FORMAT_ERROR = -119,
    OLAP_ERR_EVAL_CONJUNCTS_ERROR = -120,
    OLAP_ERR_COPY_FILE_ERROR = -121,
    OLAP_ERR_FILE_ALREADY_EXIST = -122,

    // common errors codes
    // [-200, -300)
    OLAP_ERR_NOT_INITED = -200,
    OLAP_ERR_FUNC_NOT_IMPLEMENTED = -201,
    OLAP_ERR_CALL_SEQUENCE_ERROR = -202,
    OLAP_ERR_INPUT_PARAMETER_ERROR = -203,
    OLAP_ERR_BUFFER_OVERFLOW = -204,
    OLAP_ERR_CONFIG_ERROR = -205,
    OLAP_ERR_INIT_FAILED = -206,
    OLAP_ERR_INVALID_SCHEMA = -207,
    OLAP_ERR_CHECKSUM_ERROR = -208,
    OLAP_ERR_SIGNATURE_ERROR = -209,
    OLAP_ERR_CATCH_EXCEPTION = -210,
    OLAP_ERR_PARSE_PROTOBUF_ERROR = -211,
    OLAP_ERR_SERIALIZE_PROTOBUF_ERROR = -212,
    OLAP_ERR_WRITE_PROTOBUF_ERROR = -213,
    OLAP_ERR_VERSION_NOT_EXIST = -214,
    OLAP_ERR_TABLE_NOT_FOUND = -215,
    OLAP_ERR_TRY_LOCK_FAILED = -216,
    OLAP_ERR_OUT_OF_BOUND = -218,
    OLAP_ERR_UNDERFLOW = -219,
    OLAP_ERR_FILE_DATA_ERROR = -220,
    OLAP_ERR_TEST_FILE_ERROR = -221,
    OLAP_ERR_INVALID_ROOT_PATH = -222,
    OLAP_ERR_NO_AVAILABLE_ROOT_PATH = -223,
    OLAP_ERR_CHECK_LINES_ERROR = -224,
    OLAP_ERR_INVALID_CLUSTER_INFO = -225,
    OLAP_ERR_TRANSACTION_NOT_EXIST = -226,
    OLAP_ERR_DISK_FAILURE = -227,
    OLAP_ERR_TRANSACTION_ALREADY_COMMITTED = -228,
    OLAP_ERR_TRANSACTION_ALREADY_VISIBLE = -229,
    OLAP_ERR_VERSION_ALREADY_MERGED = -230,
    OLAP_ERR_LZO_DISABLED = -231,
    OLAP_ERR_DISK_REACH_CAPACITY_LIMIT = -232,
    OLAP_ERR_TOO_MANY_TRANSACTIONS = -233,
    OLAP_ERR_INVALID_SNAPSHOT_VERSION = -234,
    OLAP_ERR_TOO_MANY_VERSION = -235,
    OLAP_ERR_NOT_INITIALIZED = -236,
    OLAP_ERR_ALREADY_CANCELLED = -237,

    // CommandExecutor
    // [-300, -400)
    OLAP_ERR_CE_CMD_PARAMS_ERROR = -300,
    OLAP_ERR_CE_BUFFER_TOO_SMALL = -301,
    OLAP_ERR_CE_CMD_NOT_VALID = -302,
    OLAP_ERR_CE_LOAD_TABLE_ERROR = -303,
    OLAP_ERR_CE_NOT_FINISHED = -304,
    OLAP_ERR_CE_TABLET_ID_EXIST = -305,
    OLAP_ERR_CE_TRY_CE_LOCK_ERROR = -306,

    // Tablet
    // [-400, -500)
    OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR = -400,
    OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR = -401,
    OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR = -402,
    OLAP_ERR_TABLE_INDEX_FIND_ERROR = -403,
    OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR = -404,
    OLAP_ERR_TABLE_CREATE_META_ERROR = -405,
    OLAP_ERR_TABLE_ALREADY_DELETED_ERROR = -406,

    // StorageEngine
    // [-500, -600)
    OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE = -500,
    OLAP_ERR_ENGINE_DROP_NOEXISTS_TABLE = -501,
    OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR = -502,
    OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR = -503,
    OLAP_ERR_DELETE_VERSION_ERROR = -504,
    OLAP_ERR_GC_SCAN_PATH_ERROR = -505,
    OLAP_ERR_ENGINE_INSERT_OLD_TABLET = -506,

    // FetchHandler
    // [-600, -700)
    OLAP_ERR_FETCH_OTHER_ERROR = -600,
    OLAP_ERR_FETCH_TABLE_NOT_EXIST = -601,
    OLAP_ERR_FETCH_VERSION_ERROR = -602,
    OLAP_ERR_FETCH_SCHEMA_ERROR = -603,
    OLAP_ERR_FETCH_COMPRESSION_ERROR = -604,
    OLAP_ERR_FETCH_CONTEXT_NOT_EXIST = -605,
    OLAP_ERR_FETCH_GET_READER_PARAMS_ERR = -606,
    OLAP_ERR_FETCH_SAVE_SESSION_ERR = -607,
    OLAP_ERR_FETCH_MEMORY_EXCEEDED = -608,

    // Reader
    // [-700, -800)
    OLAP_ERR_READER_IS_UNINITIALIZED = -700,
    OLAP_ERR_READER_GET_ITERATOR_ERROR = -701,
    OLAP_ERR_CAPTURE_ROWSET_READER_ERROR = -702,
    OLAP_ERR_READER_READING_ERROR = -703,
    OLAP_ERR_READER_INITIALIZE_ERROR = -704,

    // BaseCompaction
    // [-800, -900)
    OLAP_ERR_BE_VERSION_NOT_MATCH = -800,
    OLAP_ERR_BE_REPLACE_VERSIONS_ERROR = -801,
    OLAP_ERR_BE_MERGE_ERROR = -802,
    OLAP_ERR_BE_COMPUTE_VERSION_HASH_ERROR = -803,
    OLAP_ERR_CAPTURE_ROWSET_ERROR = -804,
    OLAP_ERR_BE_SAVE_HEADER_ERROR = -805,
    OLAP_ERR_BE_INIT_OLAP_DATA = -806,
    OLAP_ERR_BE_TRY_OBTAIN_VERSION_LOCKS = -807,
    OLAP_ERR_BE_NO_SUITABLE_VERSION = -808,
    OLAP_ERR_BE_TRY_BE_LOCK_ERROR = -809,
    OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS = -810,
    OLAP_ERR_BE_ERROR_DELETE_ACTION = -811,
    OLAP_ERR_BE_SEGMENTS_OVERLAPPING = -812,
    OLAP_ERR_BE_CLONE_OCCURRED = -813,

    // PUSH
    // [-900, -1000)
    OLAP_ERR_PUSH_INIT_ERROR = -900,
    OLAP_ERR_PUSH_DELTA_FILE_EOF = -901,
    OLAP_ERR_PUSH_VERSION_INCORRECT = -902,
    OLAP_ERR_PUSH_SCHEMA_MISMATCH = -903,
    OLAP_ERR_PUSH_CHECKSUM_ERROR = -904,
    OLAP_ERR_PUSH_ACQUIRE_DATASOURCE_ERROR = -905,
    OLAP_ERR_PUSH_CREAT_CUMULATIVE_ERROR = -906,
    OLAP_ERR_PUSH_BUILD_DELTA_ERROR = -907,
    OLAP_ERR_PUSH_VERSION_ALREADY_EXIST = -908,
    OLAP_ERR_PUSH_TABLE_NOT_EXIST = -909,
    OLAP_ERR_PUSH_INPUT_DATA_ERROR = -910,
    OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST = -911,
    // only support realtime push api, batch process is deprecated and is removed
    OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED = -912,
    OLAP_ERR_PUSH_COMMIT_ROWSET = -913,
    OLAP_ERR_PUSH_ROWSET_NOT_FOUND = -914,

    // SegmentGroup
    // [-1000, -1100)
    OLAP_ERR_INDEX_LOAD_ERROR = -1000,
    OLAP_ERR_INDEX_EOF = -1001,
    OLAP_ERR_INDEX_CHECKSUM_ERROR = -1002,
    OLAP_ERR_INDEX_DELTA_PRUNING = -1003,

    // OLAPData
    // [-1100, -1200)
    OLAP_ERR_DATA_ROW_BLOCK_ERROR = -1100,
    OLAP_ERR_DATA_FILE_TYPE_ERROR = -1101,
    OLAP_ERR_DATA_EOF = -1102,

    // OLAPDataWriter
    // [-1200, -1300)
    OLAP_ERR_WRITER_INDEX_WRITE_ERROR = -1200,
    OLAP_ERR_WRITER_DATA_WRITE_ERROR = -1201,
    OLAP_ERR_WRITER_ROW_BLOCK_ERROR = -1202,
    OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED = -1203,

    // RowBlock
    // [-1300, -1400)
    OLAP_ERR_ROWBLOCK_DECOMPRESS_ERROR = -1300,
    OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION = -1301,
    OLAP_ERR_ROWBLOCK_READ_INFO_ERROR = -1302,

    // TabletMeta
    // [-1400, -1500)
    OLAP_ERR_HEADER_ADD_VERSION = -1400,
    OLAP_ERR_HEADER_DELETE_VERSION = -1401,
    OLAP_ERR_HEADER_ADD_PENDING_DELTA = -1402,
    OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION = -1403,
    OLAP_ERR_HEADER_INVALID_FLAG = -1404,
    OLAP_ERR_HEADER_PUT = -1405,
    OLAP_ERR_HEADER_DELETE = -1406,
    OLAP_ERR_HEADER_GET = -1407,
    OLAP_ERR_HEADER_LOAD_INVALID_KEY = -1408,
    OLAP_ERR_HEADER_FLAG_PUT = -1409,
    OLAP_ERR_HEADER_LOAD_JSON_HEADER = -1410,
    OLAP_ERR_HEADER_INIT_FAILED = -1411,
    OLAP_ERR_HEADER_PB_PARSE_FAILED = -1412,
    OLAP_ERR_HEADER_HAS_PENDING_DATA = -1413,

    // TabletSchema
    // [-1500, -1600)
    OLAP_ERR_SCHEMA_SCHEMA_INVALID = -1500,
    OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID = -1501,

    // SchemaHandler
    // [-1600, -1606)
    OLAP_ERR_ALTER_MULTI_TABLE_ERR = -1600,
    OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS = -1601,
    OLAP_ERR_ALTER_STATUS_ERR = -1602,
    OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED = -1603,
    OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID = -1604,
    OLAP_ERR_QUERY_SPLIT_KEY_ERR = -1605,
    //Error caused by a data quality issue during schema change/materialized view
    OLAP_ERR_DATA_QUALITY_ERR = -1606,

    // Column File
    // [-1700, -1800)
    OLAP_ERR_COLUMN_DATA_LOAD_BLOCK = -1700,
    OLAP_ERR_COLUMN_DATA_RECORD_INDEX = -1701,
    OLAP_ERR_COLUMN_DATA_MAKE_FILE_HEADER = -1702,
    OLAP_ERR_COLUMN_DATA_READ_VAR_INT = -1703,
    OLAP_ERR_COLUMN_DATA_PATCH_LIST_NUM = -1704,
    OLAP_ERR_COLUMN_STREAM_EOF = -1705,
    OLAP_ERR_COLUMN_READ_STREAM = -1706,
    OLAP_ERR_COLUMN_STREAM_NOT_EXIST = -1716,
    OLAP_ERR_COLUMN_VALUE_NULL = -1717,
    OLAP_ERR_COLUMN_SEEK_ERROR = -1719,

    // DeleteHandler
    // [-1900, -2000)
    OLAP_ERR_DELETE_INVALID_CONDITION = -1900,
    OLAP_ERR_DELETE_UPDATE_HEADER_FAILED = -1901,
    OLAP_ERR_DELETE_SAVE_HEADER_FAILED = -1902,
    OLAP_ERR_DELETE_INVALID_PARAMETERS = -1903,
    OLAP_ERR_DELETE_INVALID_VERSION = -1904,

    // Cumulative Handler
    // [-2000, -3000)
    OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS = -2000,
    OLAP_ERR_CUMULATIVE_REPEAT_INIT = -2001,
    OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS = -2002,
    OLAP_ERR_CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE = -2003,
    OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS = -2004,
    OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION = -2005,
    OLAP_ERR_CUMULATIVE_MISS_VERSION = -2006,
    OLAP_ERR_CUMULATIVE_CLONE_OCCURRED = -2007,

    // OLAPMeta
    // [-3000, -3100)
    OLAP_ERR_META_INVALID_ARGUMENT = -3000,
    OLAP_ERR_META_OPEN_DB = -3001,
    OLAP_ERR_META_KEY_NOT_FOUND = -3002,
    OLAP_ERR_META_GET = -3003,
    OLAP_ERR_META_PUT = -3004,
    OLAP_ERR_META_ITERATOR = -3005,
    OLAP_ERR_META_DELETE = -3006,
    OLAP_ERR_META_ALREADY_EXIST = -3007,

    // Rowset
    // [-3100, -3200)
    OLAP_ERR_ROWSET_WRITER_INIT = -3100,
    OLAP_ERR_ROWSET_SAVE_FAILED = -3101,
    OLAP_ERR_ROWSET_GENERATE_ID_FAILED = -3102,
    OLAP_ERR_ROWSET_DELETE_FILE_FAILED = -3103,
    OLAP_ERR_ROWSET_BUILDER_INIT = -3104,
    OLAP_ERR_ROWSET_TYPE_NOT_FOUND = -3105,
    OLAP_ERR_ROWSET_ALREADY_EXIST = -3106,
    OLAP_ERR_ROWSET_CREATE_READER = -3107,
    OLAP_ERR_ROWSET_INVALID = -3108,
    OLAP_ERR_ROWSET_LOAD_FAILED = -3109,
    OLAP_ERR_ROWSET_READER_INIT = -3110,
    OLAP_ERR_ROWSET_READ_FAILED = -3111,
    OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION = -3112
};

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
const std::string TABLET_SCHEMA_HASH_KEY = "schema_hash";
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
#define RETURN_NOT_OK(s)                         \
    do {                                         \
        OLAPStatus _s = (s);                     \
        if (OLAP_UNLIKELY(_s != OLAP_SUCCESS)) { \
            return _s;                           \
        }                                        \
    } while (0);
#endif

#ifndef RETURN_NOT_OK_LOG
#define RETURN_NOT_OK_LOG(s, msg)                          \
    do {                                                   \
        OLAPStatus _s = (s);                               \
        if (OLAP_UNLIKELY(_s != OLAP_SUCCESS)) {           \
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
        classname* p_instance = NULL;    \
        try {                            \
            static classname s_instance; \
            p_instance = &s_instance;    \
        } catch (...) {                  \
            p_instance = NULL;           \
        }                                \
        return p_instance;               \
    }                                    \
                                         \
protected:                               \
    classname();                         \
                                         \
private:                                 \
    ~classname();

#define SAFE_DELETE(ptr)   \
    do {                   \
        if (NULL != ptr) { \
            delete ptr;    \
            ptr = NULL;    \
        }                  \
    } while (0)

#define SAFE_DELETE_ARRAY(ptr) \
    do {                       \
        if (NULL != ptr) {     \
            delete[] ptr;      \
            ptr = NULL;        \
        }                      \
    } while (0)

#ifndef BUILD_VERSION
#define BUILD_VERSION "Unknown"
#endif

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_DEFINE_H
