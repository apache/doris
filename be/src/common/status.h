// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <fmt/format.h>
#include <gen_cpp/Status_types.h> // for TStatus
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#ifdef ENABLE_STACKTRACE
#include "util/stack_util.h"
#endif

#include "common/expected.h"

namespace doris {

class PStatus;

namespace ErrorCode {
#define E(name, code) static constexpr int name = code
E(OK, 0);
#define TStatusError(name) E(name, TStatusCode::name)
// Errors defined in TStatus
TStatusError(PUBLISH_TIMEOUT);
TStatusError(MEM_ALLOC_FAILED);
TStatusError(BUFFER_ALLOCATION_FAILED);
TStatusError(INVALID_ARGUMENT);
TStatusError(MINIMUM_RESERVATION_UNAVAILABLE);
TStatusError(CORRUPTION);
TStatusError(IO_ERROR);
TStatusError(NOT_FOUND);
TStatusError(ALREADY_EXIST);
TStatusError(NOT_IMPLEMENTED_ERROR);
TStatusError(END_OF_FILE);
TStatusError(INTERNAL_ERROR);
TStatusError(RUNTIME_ERROR);
TStatusError(CANCELLED);
TStatusError(MEM_LIMIT_EXCEEDED);
TStatusError(THRIFT_RPC_ERROR);
TStatusError(TIMEOUT);
TStatusError(TOO_MANY_TASKS);
TStatusError(UNINITIALIZED);
TStatusError(ABORTED);
TStatusError(DATA_QUALITY_ERROR);
TStatusError(LABEL_ALREADY_EXISTS);
TStatusError(NOT_AUTHORIZED);
TStatusError(HTTP_ERROR);
#undef TStatusError
// BE internal errors
E(OS_ERROR, -100);
E(DIR_NOT_EXIST, -101);
E(FILE_NOT_EXIST, -102);
E(CREATE_FILE_ERROR, -103);
E(STL_ERROR, -105);
E(MUTEX_ERROR, -107);
E(PTHREAD_ERROR, -108);
E(NETWORK_ERROR, -109);
E(UB_FUNC_ERROR, -110);
E(COMPRESS_ERROR, -111);
E(DECOMPRESS_ERROR, -112);
E(UNKNOWN_COMPRESSION_TYPE, -113);
E(MMAP_ERROR, -114);
E(CANNOT_CREATE_DIR, -117);
E(UB_NETWORK_ERROR, -118);
E(FILE_FORMAT_ERROR, -119);
E(EVAL_CONJUNCTS_ERROR, -120);
E(COPY_FILE_ERROR, -121);
E(FILE_ALREADY_EXIST, -122);
E(BAD_CAST, -123);
E(CALL_SEQUENCE_ERROR, -202);
E(BUFFER_OVERFLOW, -204);
E(CONFIG_ERROR, -205);
E(INIT_FAILED, -206);
E(INVALID_SCHEMA, -207);
E(CHECKSUM_ERROR, -208);
E(SIGNATURE_ERROR, -209);
E(CATCH_EXCEPTION, -210);
E(PARSE_PROTOBUF_ERROR, -211);
E(SERIALIZE_PROTOBUF_ERROR, -212);
E(WRITE_PROTOBUF_ERROR, -213);
E(VERSION_NOT_EXIST, -214);
E(TABLE_NOT_FOUND, -215);
E(TRY_LOCK_FAILED, -216);
E(OUT_OF_BOUND, -218);
E(INVALID_ROOT_PATH, -222);
E(NO_AVAILABLE_ROOT_PATH, -223);
E(CHECK_LINES_ERROR, -224);
E(INVALID_CLUSTER_INFO, -225);
E(TRANSACTION_NOT_EXIST, -226);
E(DISK_FAILURE, -227);
E(TRANSACTION_ALREADY_COMMITTED, -228);
E(TRANSACTION_ALREADY_VISIBLE, -229);
E(VERSION_ALREADY_MERGED, -230);
E(LZO_DISABLED, -231);
E(DISK_REACH_CAPACITY_LIMIT, -232);
E(TOO_MANY_TRANSACTIONS, -233);
E(INVALID_SNAPSHOT_VERSION, -234);
E(TOO_MANY_VERSION, -235);
E(NOT_INITIALIZED, -236);
E(ALREADY_CANCELLED, -237);
E(TOO_MANY_SEGMENTS, -238);
E(ALREADY_CLOSED, -239);
E(SERVICE_UNAVAILABLE, -240);
E(NEED_SEND_AGAIN, -241);
E(CE_CMD_PARAMS_ERROR, -300);
E(CE_BUFFER_TOO_SMALL, -301);
E(CE_CMD_NOT_VALID, -302);
E(CE_LOAD_TABLE_ERROR, -303);
E(CE_NOT_FINISHED, -304);
E(CE_TABLET_ID_EXIST, -305);
E(TABLE_VERSION_DUPLICATE_ERROR, -400);
E(TABLE_VERSION_INDEX_MISMATCH_ERROR, -401);
E(TABLE_INDEX_VALIDATE_ERROR, -402);
E(TABLE_INDEX_FIND_ERROR, -403);
E(TABLE_CREATE_FROM_HEADER_ERROR, -404);
E(TABLE_CREATE_META_ERROR, -405);
E(TABLE_ALREADY_DELETED_ERROR, -406);
E(ENGINE_INSERT_EXISTS_TABLE, -500);
E(ENGINE_DROP_NOEXISTS_TABLE, -501);
E(ENGINE_LOAD_INDEX_TABLE_ERROR, -502);
E(TABLE_INSERT_DUPLICATION_ERROR, -503);
E(DELETE_VERSION_ERROR, -504);
E(GC_SCAN_PATH_ERROR, -505);
E(ENGINE_INSERT_OLD_TABLET, -506);
E(FETCH_OTHER_ERROR, -600);
E(FETCH_TABLE_NOT_EXIST, -601);
E(FETCH_VERSION_ERROR, -602);
E(FETCH_SCHEMA_ERROR, -603);
E(FETCH_COMPRESSION_ERROR, -604);
E(FETCH_CONTEXT_NOT_EXIST, -605);
E(FETCH_GET_READER_PARAMS_ERR, -606);
E(FETCH_SAVE_SESSION_ERR, -607);
E(FETCH_MEMORY_EXCEEDED, -608);
E(READER_IS_UNINITIALIZED, -700);
E(READER_GET_ITERATOR_ERROR, -701);
E(CAPTURE_ROWSET_READER_ERROR, -702);
E(READER_READING_ERROR, -703);
E(READER_INITIALIZE_ERROR, -704);
E(BE_VERSION_NOT_MATCH, -800);
E(BE_REPLACE_VERSIONS_ERROR, -801);
E(BE_MERGE_ERROR, -802);
E(CAPTURE_ROWSET_ERROR, -804);
E(BE_SAVE_HEADER_ERROR, -805);
E(BE_INIT_OLAP_DATA, -806);
E(BE_TRY_OBTAIN_VERSION_LOCKS, -807);
E(BE_NO_SUITABLE_VERSION, -808);
E(BE_INVALID_NEED_MERGED_VERSIONS, -810);
E(BE_ERROR_DELETE_ACTION, -811);
E(BE_SEGMENTS_OVERLAPPING, -812);
E(BE_CLONE_OCCURRED, -813);
E(PUSH_INIT_ERROR, -900);
E(PUSH_VERSION_INCORRECT, -902);
E(PUSH_SCHEMA_MISMATCH, -903);
E(PUSH_CHECKSUM_ERROR, -904);
E(PUSH_ACQUIRE_DATASOURCE_ERROR, -905);
E(PUSH_CREAT_CUMULATIVE_ERROR, -906);
E(PUSH_BUILD_DELTA_ERROR, -907);
E(PUSH_VERSION_ALREADY_EXIST, -908);
E(PUSH_TABLE_NOT_EXIST, -909);
E(PUSH_INPUT_DATA_ERROR, -910);
E(PUSH_TRANSACTION_ALREADY_EXIST, -911);
E(PUSH_BATCH_PROCESS_REMOVED, -912);
E(PUSH_COMMIT_ROWSET, -913);
E(PUSH_ROWSET_NOT_FOUND, -914);
E(INDEX_LOAD_ERROR, -1000);
E(INDEX_CHECKSUM_ERROR, -1002);
E(INDEX_DELTA_PRUNING, -1003);
E(DATA_ROW_BLOCK_ERROR, -1100);
E(DATA_FILE_TYPE_ERROR, -1101);
E(WRITER_INDEX_WRITE_ERROR, -1200);
E(WRITER_DATA_WRITE_ERROR, -1201);
E(WRITER_ROW_BLOCK_ERROR, -1202);
E(WRITER_SEGMENT_NOT_FINALIZED, -1203);
E(ROWBLOCK_DECOMPRESS_ERROR, -1300);
E(ROWBLOCK_FIND_ROW_EXCEPTION, -1301);
E(HEADER_ADD_VERSION, -1400);
E(HEADER_DELETE_VERSION, -1401);
E(HEADER_ADD_PENDING_DELTA, -1402);
E(HEADER_ADD_INCREMENTAL_VERSION, -1403);
E(HEADER_INVALID_FLAG, -1404);
E(HEADER_LOAD_INVALID_KEY, -1408);
E(HEADER_LOAD_JSON_HEADER, -1410);
E(HEADER_INIT_FAILED, -1411);
E(HEADER_PB_PARSE_FAILED, -1412);
E(HEADER_HAS_PENDING_DATA, -1413);
E(SCHEMA_SCHEMA_INVALID, -1500);
E(SCHEMA_SCHEMA_FIELD_INVALID, -1501);
E(ALTER_MULTI_TABLE_ERR, -1600);
E(ALTER_DELTA_DOES_NOT_EXISTS, -1601);
E(ALTER_STATUS_ERR, -1602);
E(PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED, -1603);
E(SCHEMA_CHANGE_INFO_INVALID, -1604);
E(QUERY_SPLIT_KEY_ERR, -1605);
E(DATA_QUALITY_ERR, -1606);
E(COLUMN_DATA_LOAD_BLOCK, -1700);
E(COLUMN_DATA_RECORD_INDEX, -1701);
E(COLUMN_DATA_MAKE_FILE_HEADER, -1702);
E(COLUMN_DATA_READ_VAR_INT, -1703);
E(COLUMN_DATA_PATCH_LIST_NUM, -1704);
E(COLUMN_READ_STREAM, -1706);
E(COLUMN_STREAM_NOT_EXIST, -1716);
E(COLUMN_VALUE_NULL, -1717);
E(COLUMN_SEEK_ERROR, -1719);
E(COLUMN_NO_MATCH_OFFSETS_SIZE, -1720);
E(COLUMN_NO_MATCH_FILTER_SIZE, -1721);
E(DELETE_INVALID_CONDITION, -1900);
E(DELETE_UPDATE_HEADER_FAILED, -1901);
E(DELETE_SAVE_HEADER_FAILED, -1902);
E(DELETE_INVALID_PARAMETERS, -1903);
E(DELETE_INVALID_VERSION, -1904);
E(CUMULATIVE_NO_SUITABLE_VERSION, -2000);
E(CUMULATIVE_REPEAT_INIT, -2001);
E(CUMULATIVE_INVALID_PARAMETERS, -2002);
E(CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE, -2003);
E(CUMULATIVE_INVALID_NEED_MERGED_VERSIONS, -2004);
E(CUMULATIVE_ERROR_DELETE_ACTION, -2005);
E(CUMULATIVE_MISS_VERSION, -2006);
E(CUMULATIVE_CLONE_OCCURRED, -2007);
E(FULL_NO_SUITABLE_VERSION, -2008);
E(FULL_MISS_VERSION, -2009);
E(META_INVALID_ARGUMENT, -3000);
E(META_OPEN_DB_ERROR, -3001);
E(META_KEY_NOT_FOUND, -3002);
E(META_GET_ERROR, -3003);
E(META_PUT_ERROR, -3004);
E(META_ITERATOR_ERROR, -3005);
E(META_DELETE_ERROR, -3006);
E(META_ALREADY_EXIST, -3007);
E(ROWSET_WRITER_INIT, -3100);
E(ROWSET_SAVE_FAILED, -3101);
E(ROWSET_GENERATE_ID_FAILED, -3102);
E(ROWSET_DELETE_FILE_FAILED, -3103);
E(ROWSET_BUILDER_INIT, -3104);
E(ROWSET_TYPE_NOT_FOUND, -3105);
E(ROWSET_ALREADY_EXIST, -3106);
E(ROWSET_CREATE_READER, -3107);
E(ROWSET_INVALID, -3108);
E(ROWSET_READER_INIT, -3110);
E(ROWSET_INVALID_STATE_TRANSITION, -3112);
E(STRING_OVERFLOW_IN_VEC_ENGINE, -3113);
E(ROWSET_ADD_MIGRATION_V2, -3114);
E(PUBLISH_VERSION_NOT_CONTINUOUS, -3115);
E(ROWSET_RENAME_FILE_FAILED, -3116);
E(SEGCOMPACTION_INIT_READER, -3117);
E(SEGCOMPACTION_INIT_WRITER, -3118);
E(SEGCOMPACTION_FAILED, -3119);
E(PIP_WAIT_FOR_RF, -3120);
E(PIP_WAIT_FOR_SC, -3121);
E(ROWSET_ADD_TO_BINLOG_FAILED, -3122);
E(ROWSET_BINLOG_NOT_ONLY_ONE_VERSION, -3123);
E(INVERTED_INDEX_INVALID_PARAMETERS, -6000);
E(INVERTED_INDEX_NOT_SUPPORTED, -6001);
E(INVERTED_INDEX_CLUCENE_ERROR, -6002);
E(INVERTED_INDEX_FILE_NOT_FOUND, -6003);
E(INVERTED_INDEX_BYPASS, -6004);
E(INVERTED_INDEX_NO_TERMS, -6005);
E(INVERTED_INDEX_RENAME_FILE_FAILED, -6006);
E(INVERTED_INDEX_EVALUATE_SKIPPED, -6007);
E(INVERTED_INDEX_BUILD_WAITTING, -6008);
E(KEY_NOT_FOUND, -6009);
E(KEY_ALREADY_EXISTS, -6010);
E(ENTRY_NOT_FOUND, -6011);
#undef E
} // namespace ErrorCode

// clang-format off
// whether to capture stacktrace
constexpr bool capture_stacktrace(int code) {
    return code != ErrorCode::OK
        && code != ErrorCode::END_OF_FILE
        && code != ErrorCode::MEM_LIMIT_EXCEEDED
        && code != ErrorCode::TRY_LOCK_FAILED
        && code != ErrorCode::TOO_MANY_SEGMENTS
        && code != ErrorCode::TOO_MANY_VERSION
        && code != ErrorCode::ALREADY_CANCELLED
        && code != ErrorCode::ALREADY_CLOSED
        && code != ErrorCode::NEED_SEND_AGAIN
        && code != ErrorCode::PUSH_TRANSACTION_ALREADY_EXIST
        && code != ErrorCode::BE_NO_SUITABLE_VERSION
        && code != ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION
        && code != ErrorCode::FULL_NO_SUITABLE_VERSION
        && code != ErrorCode::PUBLISH_VERSION_NOT_CONTINUOUS
        && code != ErrorCode::PUBLISH_TIMEOUT
        && code != ErrorCode::ROWSET_RENAME_FILE_FAILED
        && code != ErrorCode::SEGCOMPACTION_INIT_READER
        && code != ErrorCode::SEGCOMPACTION_INIT_WRITER
        && code != ErrorCode::SEGCOMPACTION_FAILED
        && code != ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS
        && code != ErrorCode::INVERTED_INDEX_NOT_SUPPORTED
        && code != ErrorCode::INVERTED_INDEX_CLUCENE_ERROR
        && code != ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND
        && code != ErrorCode::INVERTED_INDEX_BYPASS
        && code != ErrorCode::INVERTED_INDEX_NO_TERMS
        && code != ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED
        && code != ErrorCode::INVERTED_INDEX_BUILD_WAITTING
        && code != ErrorCode::META_KEY_NOT_FOUND
        && code != ErrorCode::PUSH_VERSION_ALREADY_EXIST
        && code != ErrorCode::VERSION_NOT_EXIST
        && code != ErrorCode::TABLE_ALREADY_DELETED_ERROR
        && code != ErrorCode::TRANSACTION_NOT_EXIST
        && code != ErrorCode::TRANSACTION_ALREADY_VISIBLE
        && code != ErrorCode::TOO_MANY_TRANSACTIONS
        && code != ErrorCode::TRANSACTION_ALREADY_COMMITTED
        && code != ErrorCode::ENTRY_NOT_FOUND
        && code != ErrorCode::KEY_NOT_FOUND
        && code != ErrorCode::KEY_ALREADY_EXISTS
        && code != ErrorCode::CANCELLED
        && code != ErrorCode::UNINITIALIZED
        && code != ErrorCode::PIP_WAIT_FOR_RF
        && code != ErrorCode::PIP_WAIT_FOR_SC;
}
// clang-format on

class [[nodiscard]] Status {
public:
    Status() : _code(ErrorCode::OK), _err_msg(nullptr) {}

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
    Status& operator=(Status&& rhs) noexcept {
        _code = rhs._code;
        if (rhs._err_msg) {
            _err_msg = std::move(rhs._err_msg);
        }
        return *this;
    }

    template <bool stacktrace = true>
    Status static create(const TStatus& status) {
        return Error<stacktrace>(
                status.status_code,
                "TStatus: " + (status.error_msgs.empty() ? "" : status.error_msgs[0]));
    }

    template <bool stacktrace = true>
    Status static create(const PStatus& pstatus) {
        return Error<stacktrace>(
                pstatus.status_code(),
                "PStatus: " + (pstatus.error_msgs_size() == 0 ? "" : pstatus.error_msgs(0)));
    }

    template <int code, bool stacktrace = true, typename... Args>
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
        if constexpr (stacktrace && capture_stacktrace(code)) {
            // Delete the first one frame pointers, which are inside the status.h
            status._err_msg->_stack = get_stack_trace(1);
            LOG(WARNING) << "meet error status: " << status; // may print too many stacks.
        }
#endif
        return status;
    }

    template <bool stacktrace = true, typename... Args>
    Status static Error(int code, std::string_view msg, Args&&... args) {
        Status status;
        status._code = code;
        status._err_msg = std::make_unique<ErrMsg>();
        if constexpr (sizeof...(args) == 0) {
            status._err_msg->_msg = msg;
        } else {
            status._err_msg->_msg = fmt::format(msg, std::forward<Args>(args)...);
        }
#ifdef ENABLE_STACKTRACE
        if (stacktrace && capture_stacktrace(code)) {
            status._err_msg->_stack = get_stack_trace(1);
            LOG(WARNING) << "meet error status: " << status; // may print too many stacks.
        }
#endif
        return status;
    }

    static Status OK() { return Status(); }

#define ERROR_CTOR(name, code)                                                 \
    template <typename... Args>                                                \
    static Status name(std::string_view msg, Args&&... args) {                 \
        return Error<ErrorCode::code, true>(msg, std::forward<Args>(args)...); \
    }

    ERROR_CTOR(PublishTimeout, PUBLISH_TIMEOUT)
    ERROR_CTOR(MemoryAllocFailed, MEM_ALLOC_FAILED)
    ERROR_CTOR(BufferAllocFailed, BUFFER_ALLOCATION_FAILED)
    ERROR_CTOR(InvalidArgument, INVALID_ARGUMENT)
    ERROR_CTOR(MinimumReservationUnavailable, MINIMUM_RESERVATION_UNAVAILABLE)
    ERROR_CTOR(Corruption, CORRUPTION)
    ERROR_CTOR(IOError, IO_ERROR)
    ERROR_CTOR(NotFound, NOT_FOUND)
    ERROR_CTOR(AlreadyExist, ALREADY_EXIST)
    ERROR_CTOR(NotSupported, NOT_IMPLEMENTED_ERROR)
    ERROR_CTOR(EndOfFile, END_OF_FILE)
    ERROR_CTOR(InternalError, INTERNAL_ERROR)
    ERROR_CTOR(WaitForRf, PIP_WAIT_FOR_RF)
    ERROR_CTOR(WaitForScannerContext, PIP_WAIT_FOR_SC)
    ERROR_CTOR(RuntimeError, RUNTIME_ERROR)
    ERROR_CTOR(Cancelled, CANCELLED)
    ERROR_CTOR(MemoryLimitExceeded, MEM_LIMIT_EXCEEDED)
    ERROR_CTOR(RpcError, THRIFT_RPC_ERROR)
    ERROR_CTOR(TimedOut, TIMEOUT)
    ERROR_CTOR(TooManyTasks, TOO_MANY_TASKS)
    ERROR_CTOR(Uninitialized, UNINITIALIZED)
    ERROR_CTOR(Aborted, ABORTED)
    ERROR_CTOR(DataQualityError, DATA_QUALITY_ERROR)
    ERROR_CTOR(NotAuthorized, NOT_AUTHORIZED)
    ERROR_CTOR(HttpError, HTTP_ERROR)
    ERROR_CTOR(NeedSendAgain, NEED_SEND_AGAIN)
#undef ERROR_CTOR

    template <int code>
    bool is() const {
        return code == _code;
    }

    void set_code(int code) { _code = code; }

    bool ok() const { return _code == ErrorCode::OK; }

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

    std::string to_string() const;

    /// @return A json representation of this status.
    std::string to_json() const;

    int code() const { return _code; }

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

    // Used like if ASSERT_EQ(res, Status::OK())
    // if the state is ok, then both code and precise code is not initialized properly, so that should check ok state
    // ignore error messages during comparison
    bool operator==(const Status& st) const { return _code == st._code; }

    // Used like if ASSERT_NE(res, Status::OK())
    bool operator!=(const Status& st) const { return _code != st._code; }

    friend std::ostream& operator<<(std::ostream& ostr, const Status& status);

private:
    int _code;
    struct ErrMsg {
        std::string _msg;
#ifdef ENABLE_STACKTRACE
        std::string _stack;
#endif
    };
    std::unique_ptr<ErrMsg> _err_msg;

    std::string code_as_string() const {
        return (int)_code >= 0 ? doris::to_string(static_cast<TStatusCode::type>(_code))
                               : fmt::format("E{}", (int16_t)_code);
    }
};

inline std::ostream& operator<<(std::ostream& ostr, const Status& status) {
    ostr << '[' << status.code_as_string() << ']';
    ostr << (status._err_msg ? status._err_msg->_msg : "");
#ifdef ENABLE_STACKTRACE
    if (status._err_msg && !status._err_msg->_stack.empty()) {
        ostr << '\n' << status._err_msg->_stack;
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

#define RETURN_ERROR_IF_NON_VEC \
    return Status::NotSupported("Non-vectorized engine is not supported since Doris 2.0.");

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

template <typename T>
using Result = expected<T, Status>;

#define RETURN_IF_ERROR_RESULT(stmt)                \
    do {                                            \
        Status _status_ = (stmt);                   \
        if (UNLIKELY(!_status_.ok())) {             \
            return unexpected(std::move(_status_)); \
        }                                           \
    } while (false)

} // namespace doris
