// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <iostream>
#include <string>
#include <string_view>

#include "common/compiler_util.h"
#include "gen_cpp/Status_types.h" // for TStatus
#include "util/stack_util.h"

namespace doris {

class PStatus;

enum ErrorCode : int16_t {
    E_OK = 0,
#define E(NAME) E_##NAME = TStatusCode::NAME
    // Errors defined in TStatus
    E(PUBLISH_TIMEOUT),
    E(MEM_ALLOC_FAILED),
    E(BUFFER_ALLOCATION_FAILED),
    E(INVALID_ARGUMENT),
    E(MINIMUM_RESERVATION_UNAVAILABLE),
    E(CORRUPTION),
    E(IO_ERROR),
    E(NOT_FOUND),
    E(ALREADY_EXIST),
    E(NOT_IMPLEMENTED_ERROR),
    E(END_OF_FILE),
    E(INTERNAL_ERROR),
    E(RUNTIME_ERROR),
    E(CANCELLED),
    E(MEM_LIMIT_EXCEEDED),
    E(THRIFT_RPC_ERROR),
    E(TIMEOUT),
    E(TOO_MANY_TASKS),
    E(SERVICE_UNAVAILABLE),
    E(UNINITIALIZED),
    E(ABORTED),
    E(DATA_QUALITY_ERROR),
    E(LABEL_ALREADY_EXISTS),
#undef E
    // BE internal errors
    OS_ERROR = -100,
    DIR_NOT_EXIST = -101,
    FILE_NOT_EXIST = -102,
    CREATE_FILE_ERROR = -103,
    STL_ERROR = -105,
    MUTEX_ERROR = -107,
    PTHREAD_ERROR = -108,
    NETWORK_ERROR = -109,
    UB_FUNC_ERROR = -110,
    COMPRESS_ERROR = -111,
    DECOMPRESS_ERROR = -112,
    UNKNOWN_COMPRESSION_TYPE = -113,
    MMAP_ERROR = -114,
    READ_UNENOUGH = -116,
    CANNOT_CREATE_DIR = -117,
    UB_NETWORK_ERROR = -118,
    FILE_FORMAT_ERROR = -119,
    EVAL_CONJUNCTS_ERROR = -120,
    COPY_FILE_ERROR = -121,
    FILE_ALREADY_EXIST = -122,
    CALL_SEQUENCE_ERROR = -202,
    BUFFER_OVERFLOW = -204,
    CONFIG_ERROR = -205,
    INIT_FAILED = -206,
    INVALID_SCHEMA = -207,
    CHECKSUM_ERROR = -208,
    SIGNATURE_ERROR = -209,
    CATCH_EXCEPTION = -210,
    PARSE_PROTOBUF_ERROR = -211,
    SERIALIZE_PROTOBUF_ERROR = -212,
    WRITE_PROTOBUF_ERROR = -213,
    VERSION_NOT_EXIST = -214,
    TABLE_NOT_FOUND = -215,
    TRY_LOCK_FAILED = -216,
    OUT_OF_BOUND = -218,
    UNDERFLOW = -219,
    FILE_DATA_ERROR = -220,
    TEST_FILE_ERROR = -221,
    INVALID_ROOT_PATH = -222,
    NO_AVAILABLE_ROOT_PATH = -223,
    CHECK_LINES_ERROR = -224,
    INVALID_CLUSTER_INFO = -225,
    TRANSACTION_NOT_EXIST = -226,
    DISK_FAILURE = -227,
    TRANSACTION_ALREADY_COMMITTED = -228,
    TRANSACTION_ALREADY_VISIBLE = -229,
    VERSION_ALREADY_MERGED = -230,
    LZO_DISABLED = -231,
    DISK_REACH_CAPACITY_LIMIT = -232,
    TOO_MANY_TRANSACTIONS = -233,
    INVALID_SNAPSHOT_VERSION = -234,
    TOO_MANY_VERSION = -235,
    NOT_INITIALIZED = -236,
    ALREADY_CANCELLED = -237,
    TOO_MANY_SEGMENTS = -238,
    CE_CMD_PARAMS_ERROR = -300,
    CE_BUFFER_TOO_SMALL = -301,
    CE_CMD_NOT_VALID = -302,
    CE_LOAD_TABLE_ERROR = -303,
    CE_NOT_FINISHED = -304,
    CE_TABLET_ID_EXIST = -305,
    TABLE_VERSION_DUPLICATE_ERROR = -400,
    TABLE_VERSION_INDEX_MISMATCH_ERROR = -401,
    TABLE_INDEX_VALIDATE_ERROR = -402,
    TABLE_INDEX_FIND_ERROR = -403,
    TABLE_CREATE_FROM_HEADER_ERROR = -404,
    TABLE_CREATE_META_ERROR = -405,
    TABLE_ALREADY_DELETED_ERROR = -406,
    ENGINE_INSERT_EXISTS_TABLE = -500,
    ENGINE_DROP_NOEXISTS_TABLE = -501,
    ENGINE_LOAD_INDEX_TABLE_ERROR = -502,
    TABLE_INSERT_DUPLICATION_ERROR = -503,
    DELETE_VERSION_ERROR = -504,
    GC_SCAN_PATH_ERROR = -505,
    ENGINE_INSERT_OLD_TABLET = -506,
    FETCH_OTHER_ERROR = -600,
    FETCH_TABLE_NOT_EXIST = -601,
    FETCH_VERSION_ERROR = -602,
    FETCH_SCHEMA_ERROR = -603,
    FETCH_COMPRESSION_ERROR = -604,
    FETCH_CONTEXT_NOT_EXIST = -605,
    FETCH_GET_READER_PARAMS_ERR = -606,
    FETCH_SAVE_SESSION_ERR = -607,
    FETCH_MEMORY_EXCEEDED = -608,
    READER_IS_UNINITIALIZED = -700,
    READER_GET_ITERATOR_ERROR = -701,
    CAPTURE_ROWSET_READER_ERROR = -702,
    READER_READING_ERROR = -703,
    READER_INITIALIZE_ERROR = -704,
    BE_VERSION_NOT_MATCH = -800,
    BE_REPLACE_VERSIONS_ERROR = -801,
    BE_MERGE_ERROR = -802,
    CAPTURE_ROWSET_ERROR = -804,
    BE_SAVE_HEADER_ERROR = -805,
    BE_INIT_OLAP_DATA = -806,
    BE_TRY_OBTAIN_VERSION_LOCKS = -807,
    BE_NO_SUITABLE_VERSION = -808,
    BE_INVALID_NEED_MERGED_VERSIONS = -810,
    BE_ERROR_DELETE_ACTION = -811,
    BE_SEGMENTS_OVERLAPPING = -812,
    BE_CLONE_OCCURRED = -813,
    PUSH_INIT_ERROR = -900,
    PUSH_VERSION_INCORRECT = -902,
    PUSH_SCHEMA_MISMATCH = -903,
    PUSH_CHECKSUM_ERROR = -904,
    PUSH_ACQUIRE_DATASOURCE_ERROR = -905,
    PUSH_CREAT_CUMULATIVE_ERROR = -906,
    PUSH_BUILD_DELTA_ERROR = -907,
    PUSH_VERSION_ALREADY_EXIST = -908,
    PUSH_TABLE_NOT_EXIST = -909,
    PUSH_INPUT_DATA_ERROR = -910,
    PUSH_TRANSACTION_ALREADY_EXIST = -911,
    PUSH_BATCH_PROCESS_REMOVED = -912,
    PUSH_COMMIT_ROWSET = -913,
    PUSH_ROWSET_NOT_FOUND = -914,
    INDEX_LOAD_ERROR = -1000,
    INDEX_CHECKSUM_ERROR = -1002,
    INDEX_DELTA_PRUNING = -1003,
    DATA_ROW_BLOCK_ERROR = -1100,
    DATA_FILE_TYPE_ERROR = -1101,
    WRITER_INDEX_WRITE_ERROR = -1200,
    WRITER_DATA_WRITE_ERROR = -1201,
    WRITER_ROW_BLOCK_ERROR = -1202,
    WRITER_SEGMENT_NOT_FINALIZED = -1203,
    ROWBLOCK_DECOMPRESS_ERROR = -1300,
    ROWBLOCK_FIND_ROW_EXCEPTION = -1301,
    ROWBLOCK_READ_INFO_ERROR = -1302,
    HEADER_ADD_VERSION = -1400,
    HEADER_DELETE_VERSION = -1401,
    HEADER_ADD_PENDING_DELTA = -1402,
    HEADER_ADD_INCREMENTAL_VERSION = -1403,
    HEADER_INVALID_FLAG = -1404,
    HEADER_LOAD_INVALID_KEY = -1408,
    HEADER_LOAD_JSON_HEADER = -1410,
    HEADER_INIT_FAILED = -1411,
    HEADER_PB_PARSE_FAILED = -1412,
    HEADER_HAS_PENDING_DATA = -1413,
    SCHEMA_SCHEMA_INVALID = -1500,
    SCHEMA_SCHEMA_FIELD_INVALID = -1501,
    ALTER_MULTI_TABLE_ERR = -1600,
    ALTER_DELTA_DOES_NOT_EXISTS = -1601,
    ALTER_STATUS_ERR = -1602,
    PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED = -1603,
    SCHEMA_CHANGE_INFO_INVALID = -1604,
    QUERY_SPLIT_KEY_ERR = -1605,
    DATA_QUALITY_ERR = -1606,
    COLUMN_DATA_LOAD_BLOCK = -1700,
    COLUMN_DATA_RECORD_INDEX = -1701,
    COLUMN_DATA_MAKE_FILE_HEADER = -1702,
    COLUMN_DATA_READ_VAR_INT = -1703,
    COLUMN_DATA_PATCH_LIST_NUM = -1704,
    COLUMN_READ_STREAM = -1706,
    COLUMN_STREAM_NOT_EXIST = -1716,
    COLUMN_VALUE_NULL = -1717,
    COLUMN_SEEK_ERROR = -1719,
    DELETE_INVALID_CONDITION = -1900,
    DELETE_UPDATE_HEADER_FAILED = -1901,
    DELETE_SAVE_HEADER_FAILED = -1902,
    DELETE_INVALID_PARAMETERS = -1903,
    DELETE_INVALID_VERSION = -1904,
    CUMULATIVE_NO_SUITABLE_VERSION = -2000,
    CUMULATIVE_REPEAT_INIT = -2001,
    CUMULATIVE_INVALID_PARAMETERS = -2002,
    CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE = -2003,
    CUMULATIVE_INVALID_NEED_MERGED_VERSIONS = -2004,
    CUMULATIVE_ERROR_DELETE_ACTION = -2005,
    CUMULATIVE_MISS_VERSION = -2006,
    CUMULATIVE_CLONE_OCCURRED = -2007,
    META_INVALID_ARGUMENT = -3000,
    META_OPEN_DB_ERROR = -3001,
    META_KEY_NOT_FOUND = -3002,
    META_GET_ERROR = -3003,
    META_PUT_ERROR = -3004,
    META_ITERATOR_ERROR = -3005,
    META_DELETE_ERROR = -3006,
    META_ALREADY_EXIST = -3007,
    ROWSET_WRITER_INIT = -3100,
    ROWSET_SAVE_FAILED = -3101,
    ROWSET_GENERATE_ID_FAILED = -3102,
    ROWSET_DELETE_FILE_FAILED = -3103,
    ROWSET_BUILDER_INIT = -3104,
    ROWSET_TYPE_NOT_FOUND = -3105,
    ROWSET_ALREADY_EXIST = -3106,
    ROWSET_CREATE_READER = -3107,
    ROWSET_INVALID = -3108,
    ROWSET_LOAD_FAILED = -3109,
    ROWSET_READER_INIT = -3110,
    ROWSET_READ_FAILED = -3111,
    ROWSET_INVALID_STATE_TRANSITION = -3112,
    STRING_OVERFLOW_IN_VEC_ENGINE = -3113,
    ROWSET_ADD_MIGRATION_V2 = -3114,
    PUBLISH_VERSION_NOT_CONTINUOUS = -3115,
    ROWSET_RENAME_FILE_FAILED = -3116,
    SEGCOMPACTION_INIT_READER = -3117,
    SEGCOMPACTION_INIT_WRITER = -3118,
    SEGCOMPACTION_FAILED = -3119,
};

// clang-format off
// whether to capture stacktrace
template <ErrorCode code>
static constexpr bool capture_stacktrace() {
    return code != E_OK
        && code != E_END_OF_FILE
        && code != E_MEM_LIMIT_EXCEEDED
        && code != TRY_LOCK_FAILED
        && code != TOO_MANY_SEGMENTS
        && code != TOO_MANY_VERSION
        && code != ALREADY_CANCELLED
        && code != PUSH_TRANSACTION_ALREADY_EXIST
        && code != BE_NO_SUITABLE_VERSION
        && code != CUMULATIVE_NO_SUITABLE_VERSION
        && code != PUBLISH_VERSION_NOT_CONTINUOUS
        && code != ROWSET_RENAME_FILE_FAILED
        && code != SEGCOMPACTION_INIT_READER
        && code != SEGCOMPACTION_INIT_WRITER
        && code != SEGCOMPACTION_FAILED;
}
// clang-format on

class Status {
public:
    Status() : _code(ErrorCode::E_OK) {}

    // copy c'tor makes copy of error detail so Status can be returned by value
    Status(const Status& rhs) { *this = rhs; }

    // move c'tor
    Status(Status&& rhs) noexcept = default;

    // same as copy c'tor
    Status& operator=(const Status& rhs) {
        _code = rhs._code;
        if (rhs._err_msg) {
            _err_msg = std::make_unique<ErrMsg>(*rhs._err_msg);
        }
        return *this;
    }

    // move assign
    Status& operator=(Status&& rhs) noexcept = default;

    // "Copy" c'tor from TStatus.
    Status(const TStatus& status);

    Status(const PStatus& pstatus);

    template <ErrorCode code, bool stacktrace = true, typename... Args>
    Status static Error(std::string_view msg, Args&&... args) {
        Status status;
        status._code = code;
        status._err_msg = std::make_unique<ErrMsg>();
        if constexpr (sizeof...(args) == 0) {
            status._err_msg->_msg = msg;
        } else {
            status._err_msg->_msg = fmt::format(msg, std::forward<Args>(args)...);
        }
#ifdef ENABLE_STACKTRACE
        if constexpr (stacktrace && capture_stacktrace<code>()) {
            status._err_msg->_stack = get_stack_trace();
        }
#endif
        return status;
    }

    template <ErrorCode code, bool stacktrace = true>
    Status static Error() {
        Status status;
        status._code = code;
#ifdef ENABLE_STACKTRACE
        if constexpr (stacktrace && capture_stacktrace<code>()) {
            status._err_msg = std::make_unique<ErrMsg>();
            status._err_msg->_stack = get_stack_trace();
        }
#endif
        return status;
    }

    template <typename... Args>
    Status static Error(ErrorCode code, std::string_view msg, Args&&... args) {
        Status status;
        status._code = code;
        status._err_msg = std::make_unique<ErrMsg>();
        if constexpr (sizeof...(args) == 0) {
            status._err_msg->_msg = msg;
        } else {
            status._err_msg->_msg = fmt::format(msg, std::forward<Args>(args)...);
        }
        return status;
    }

    Status static Error(ErrorCode code) {
        Status status;
        status._code = code;
        return status;
    }

    static Status OK() { return Status(); }

#define ERROR_CTOR(name, code)                                                  \
    template <typename... Args>                                                 \
    static Status name(std::string_view msg, Args&&... args) {                  \
        return Error<ErrorCode::code, false>(msg, std::forward<Args>(args)...); \
    }
    ERROR_CTOR(PublishTimeout, E_PUBLISH_TIMEOUT)
    ERROR_CTOR(MemoryAllocFailed, E_MEM_ALLOC_FAILED)
    ERROR_CTOR(BufferAllocFailed, E_BUFFER_ALLOCATION_FAILED)
    ERROR_CTOR(InvalidArgument, E_INVALID_ARGUMENT)
    ERROR_CTOR(MinimumReservationUnavailable, E_MINIMUM_RESERVATION_UNAVAILABLE)
    ERROR_CTOR(Corruption, E_CORRUPTION)
    ERROR_CTOR(IOError, E_IO_ERROR)
    ERROR_CTOR(NotFound, E_NOT_FOUND)
    ERROR_CTOR(AlreadyExist, E_ALREADY_EXIST)
    ERROR_CTOR(NotSupported, E_NOT_IMPLEMENTED_ERROR)
    ERROR_CTOR(EndOfFile, E_END_OF_FILE)
    ERROR_CTOR(InternalError, E_INTERNAL_ERROR)
    ERROR_CTOR(RuntimeError, E_RUNTIME_ERROR)
    ERROR_CTOR(Cancelled, E_CANCELLED)
    ERROR_CTOR(MemoryLimitExceeded, E_MEM_LIMIT_EXCEEDED)
    ERROR_CTOR(RpcError, E_THRIFT_RPC_ERROR)
    ERROR_CTOR(TimedOut, E_TIMEOUT)
    ERROR_CTOR(TooManyTasks, E_TOO_MANY_TASKS)
    ERROR_CTOR(ServiceUnavailable, E_SERVICE_UNAVAILABLE)
    ERROR_CTOR(Uninitialized, E_UNINITIALIZED)
    ERROR_CTOR(Aborted, E_ABORTED)
    ERROR_CTOR(DataQualityError, E_DATA_QUALITY_ERROR)
#undef ERROR_CTOR

    template <ErrorCode code>
    bool is() const {
        return code == _code;
    }

    bool ok() const { return _code == ErrorCode::E_OK; }

    bool is_io_error() const {
        return ErrorCode::E_IO_ERROR == _code || ErrorCode::READ_UNENOUGH == _code ||
               ErrorCode::CHECKSUM_ERROR == _code || ErrorCode::FILE_DATA_ERROR == _code ||
               ErrorCode::TEST_FILE_ERROR == _code || ErrorCode::ROWBLOCK_READ_INFO_ERROR == _code;
    }

    // Convert into TStatus. Call this if 'status_container' contains an optional
    // TStatus field named 'status'. This also sets __isset.status.
    template <typename T>
    void set_t_status(T* status_container) const {
        to_thrift(&status_container->status);
        status_container->__isset.status = true;
    }

    // Convert into TStatus.
    void to_thrift(TStatus* status) const;
    TStatus to_thrift() const;
    void to_protobuf(PStatus* status) const;

    std::string code_as_string() const {
        return (int)_code >= 0 ? doris::to_string(static_cast<TStatusCode::type>(_code))
                               : fmt::format("E{}", (int16_t)_code);
    }

    std::string to_string() const;

    /// @return A json representation of this status.
    std::string to_json() const;

    ErrorCode code() const { return _code; }

    /// Clone this status and add the specified prefix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to prepend.
    /// @return A ref to Status object
    Status& prepend(std::string_view msg);

    /// Add the specified suffix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to append.
    /// @return A ref to Status object
    Status& append(std::string_view msg);

    // if(!status) or if (status) will use this operator
    operator bool() const { return this->ok(); }

    // Used like if (res == Status::OK())
    // if the state is ok, then both code and precise code is not initialized properly, so that should check ok state
    // ignore error messages during comparison
    bool operator==(const Status& st) const { return _code == st._code; }

    // Used like if (res != Status::OK())
    bool operator!=(const Status& st) const { return _code != st._code; }

    friend std::ostream& operator<<(std::ostream& ostr, const Status& status);

private:
    ErrorCode _code;
    struct ErrMsg {
        std::string _msg;
#ifdef ENABLE_STACKTRACE
        std::string _stack;
#endif
    };
    std::unique_ptr<ErrMsg> _err_msg;
};

inline std::ostream& operator<<(std::ostream& ostr, const Status& status) {
    ostr << '[' << status.code_as_string() << ']' << (status._err_msg ? status._err_msg->_msg : "");
#ifdef ENABLE_STACKTRACE
    if (status->_err_msg && !status->_err_msg._stack.empty()) {
        ostr << '\n' << status->_err_msg._stack;
    }
#endif
    return ostr;
}

inline std::string Status::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

// some generally useful macros
#define RETURN_IF_ERROR(stmt)           \
    do {                                \
        Status _status_ = (stmt);       \
        if (UNLIKELY(!_status_.ok())) { \
            return _status_;            \
        }                               \
    } while (false)

// End _get_next_span after last call to get_next method
#define RETURN_IF_ERROR_AND_CHECK_SPAN(stmt, get_next_span, done) \
    do {                                                          \
        Status _status_ = (stmt);                                 \
        auto _span = (get_next_span);                             \
        if (UNLIKELY(_span && (!_status_.ok() || done))) {        \
            _span->End();                                         \
        }                                                         \
        if (UNLIKELY(!_status_.ok())) {                           \
            return _status_;                                      \
        }                                                         \
    } while (false)

#define RETURN_IF_STATUS_ERROR(status, stmt) \
    do {                                     \
        status = (stmt);                     \
        if (UNLIKELY(!status.ok())) {        \
            return;                          \
        }                                    \
    } while (false)

#define EXIT_IF_ERROR(stmt)             \
    do {                                \
        Status _status_ = (stmt);       \
        if (UNLIKELY(!_status_.ok())) { \
            LOG(ERROR) << _status_;     \
            exit(1);                    \
        }                               \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)              \
    do {                                                    \
        Status _s = (to_call);                              \
        if (UNLIKELY(!_s.ok())) {                           \
            LOG(WARNING) << (warning_prefix) << ": " << _s; \
        }                                                   \
    } while (false);

#define RETURN_WITH_WARN_IF_ERROR(stmt, ret_code, warning_prefix)  \
    do {                                                           \
        Status _s = (stmt);                                        \
        if (UNLIKELY(!_s.ok())) {                                  \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s; \
            return ret_code;                                       \
        }                                                          \
    } while (false);

#define RETURN_NOT_OK_STATUS_WITH_WARN(stmt, warning_prefix)       \
    do {                                                           \
        Status _s = (stmt);                                        \
        if (UNLIKELY(!_s.ok())) {                                  \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s; \
            return _s;                                             \
        }                                                          \
    } while (false);
} // namespace doris
#ifdef WARN_UNUSED_RESULT
#undef WARN_UNUSED_RESULT
#endif

#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
