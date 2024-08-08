// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <fmt/format.h>
#include <gen_cpp/Status_types.h> // for TStatus
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/expected.h"
#include "util/stack_util.h"

namespace doris {

namespace io {
struct ObjectStorageStatus;
}

class Status;

extern io::ObjectStorageStatus convert_to_obj_response(Status st);

class PStatus;

namespace ErrorCode {

// E thrift_error_name, print_stacktrace
#define APPLY_FOR_THRIFT_ERROR_CODES(TStatusError)        \
    TStatusError(PUBLISH_TIMEOUT, false);                 \
    TStatusError(MEM_ALLOC_FAILED, true);                 \
    TStatusError(BUFFER_ALLOCATION_FAILED, true);         \
    TStatusError(INVALID_ARGUMENT, false);                \
    TStatusError(INVALID_JSON_PATH, false);               \
    TStatusError(MINIMUM_RESERVATION_UNAVAILABLE, true);  \
    TStatusError(CORRUPTION, true);                       \
    TStatusError(IO_ERROR, true);                         \
    TStatusError(NOT_FOUND, true);                        \
    TStatusError(ALREADY_EXIST, true);                    \
    TStatusError(NOT_IMPLEMENTED_ERROR, false);           \
    TStatusError(END_OF_FILE, false);                     \
    TStatusError(INTERNAL_ERROR, true);                   \
    TStatusError(RUNTIME_ERROR, true);                    \
    TStatusError(CANCELLED, false);                       \
    TStatusError(ANALYSIS_ERROR, false);                  \
    TStatusError(MEM_LIMIT_EXCEEDED, false);              \
    TStatusError(THRIFT_RPC_ERROR, true);                 \
    TStatusError(TIMEOUT, true);                          \
    TStatusError(LIMIT_REACH, false);                     \
    TStatusError(TOO_MANY_TASKS, true);                   \
    TStatusError(UNINITIALIZED, false);                   \
    TStatusError(INCOMPLETE, false);                      \
    TStatusError(OLAP_ERR_VERSION_ALREADY_MERGED, false); \
    TStatusError(ABORTED, false);                         \
    TStatusError(DATA_QUALITY_ERROR, false);              \
    TStatusError(LABEL_ALREADY_EXISTS, true);             \
    TStatusError(NOT_AUTHORIZED, true);                   \
    TStatusError(BINLOG_DISABLE, false);                  \
    TStatusError(BINLOG_TOO_OLD_COMMIT_SEQ, false);       \
    TStatusError(BINLOG_TOO_NEW_COMMIT_SEQ, false);       \
    TStatusError(BINLOG_NOT_FOUND_DB, false);             \
    TStatusError(BINLOG_NOT_FOUND_TABLE, false);          \
    TStatusError(NETWORK_ERROR, false);                   \
    TStatusError(ILLEGAL_STATE, false);                   \
    TStatusError(SNAPSHOT_NOT_EXIST, true);               \
    TStatusError(HTTP_ERROR, true);                       \
    TStatusError(TABLET_MISSING, true);                   \
    TStatusError(NOT_MASTER, true);                       \
    TStatusError(DELETE_BITMAP_LOCK_ERROR, false);
// E error_name, error_code, print_stacktrace
#define APPLY_FOR_OLAP_ERROR_CODES(E)                        \
    E(OK, 0, false);                                         \
    E(CALL_SEQUENCE_ERROR, -202, true);                      \
    E(BUFFER_OVERFLOW, -204, true);                          \
    E(CONFIG_ERROR, -205, true);                             \
    E(INIT_FAILED, -206, true);                              \
    E(INVALID_SCHEMA, -207, true);                           \
    E(CHECKSUM_ERROR, -208, true);                           \
    E(SIGNATURE_ERROR, -209, true);                          \
    E(CATCH_EXCEPTION, -210, true);                          \
    E(PARSE_PROTOBUF_ERROR, -211, true);                     \
    E(SERIALIZE_PROTOBUF_ERROR, -212, true);                 \
    E(WRITE_PROTOBUF_ERROR, -213, true);                     \
    E(VERSION_NOT_EXIST, -214, false);                       \
    E(TABLE_NOT_FOUND, -215, true);                          \
    E(TRY_LOCK_FAILED, -216, false);                         \
    E(EXCEEDED_LIMIT, -217, false);                          \
    E(OUT_OF_BOUND, -218, false);                            \
    E(INVALID_ROOT_PATH, -222, true);                        \
    E(NO_AVAILABLE_ROOT_PATH, -223, true);                   \
    E(CHECK_LINES_ERROR, -224, true);                        \
    E(INVALID_CLUSTER_INFO, -225, true);                     \
    E(TRANSACTION_NOT_EXIST, -226, false);                   \
    E(DISK_FAILURE, -227, true);                             \
    E(TRANSACTION_ALREADY_COMMITTED, -228, false);           \
    E(TRANSACTION_ALREADY_VISIBLE, -229, false);             \
    E(VERSION_ALREADY_MERGED, -230, true);                   \
    E(LZO_DISABLED, -231, true);                             \
    E(DISK_REACH_CAPACITY_LIMIT, -232, true);                \
    E(TOO_MANY_TRANSACTIONS, -233, false);                   \
    E(INVALID_SNAPSHOT_VERSION, -234, true);                 \
    E(TOO_MANY_VERSION, -235, false);                        \
    E(NOT_INITIALIZED, -236, true);                          \
    E(ALREADY_CANCELLED, -237, false);                       \
    E(TOO_MANY_SEGMENTS, -238, false);                       \
    E(ALREADY_CLOSED, -239, false);                          \
    E(SERVICE_UNAVAILABLE, -240, true);                      \
    E(NEED_SEND_AGAIN, -241, false);                         \
    E(OS_ERROR, -242, true);                                 \
    E(DIR_NOT_EXIST, -243, true);                            \
    E(FILE_NOT_EXIST, -244, true);                           \
    E(CREATE_FILE_ERROR, -245, true);                        \
    E(STL_ERROR, -246, true);                                \
    E(MUTEX_ERROR, -247, true);                              \
    E(PTHREAD_ERROR, -248, true);                            \
    E(UB_FUNC_ERROR, -250, true);                            \
    E(COMPRESS_ERROR, -251, true);                           \
    E(DECOMPRESS_ERROR, -252, true);                         \
    E(FILE_ALREADY_EXIST, -253, true);                       \
    E(BAD_CAST, -254, true);                                 \
    E(ARITHMETIC_OVERFLOW_ERRROR, -255, false);              \
    E(PERMISSION_DENIED, -256, false);                       \
    E(CE_CMD_PARAMS_ERROR, -300, true);                      \
    E(CE_BUFFER_TOO_SMALL, -301, true);                      \
    E(CE_CMD_NOT_VALID, -302, true);                         \
    E(CE_LOAD_TABLE_ERROR, -303, true);                      \
    E(CE_NOT_FINISHED, -304, true);                          \
    E(CE_TABLET_ID_EXIST, -305, true);                       \
    E(TABLE_VERSION_DUPLICATE_ERROR, -400, true);            \
    E(TABLE_VERSION_INDEX_MISMATCH_ERROR, -401, true);       \
    E(TABLE_INDEX_VALIDATE_ERROR, -402, true);               \
    E(TABLE_INDEX_FIND_ERROR, -403, true);                   \
    E(TABLE_CREATE_FROM_HEADER_ERROR, -404, true);           \
    E(TABLE_CREATE_META_ERROR, -405, true);                  \
    E(TABLE_ALREADY_DELETED_ERROR, -406, false);             \
    E(ENGINE_INSERT_EXISTS_TABLE, -500, true);               \
    E(ENGINE_DROP_NOEXISTS_TABLE, -501, true);               \
    E(ENGINE_LOAD_INDEX_TABLE_ERROR, -502, true);            \
    E(TABLE_INSERT_DUPLICATION_ERROR, -503, true);           \
    E(DELETE_VERSION_ERROR, -504, true);                     \
    E(GC_SCAN_PATH_ERROR, -505, true);                       \
    E(ENGINE_INSERT_OLD_TABLET, -506, true);                 \
    E(FETCH_OTHER_ERROR, -600, true);                        \
    E(FETCH_TABLE_NOT_EXIST, -601, true);                    \
    E(FETCH_VERSION_ERROR, -602, true);                      \
    E(FETCH_SCHEMA_ERROR, -603, true);                       \
    E(FETCH_COMPRESSION_ERROR, -604, true);                  \
    E(FETCH_CONTEXT_NOT_EXIST, -605, true);                  \
    E(FETCH_GET_READER_PARAMS_ERR, -606, true);              \
    E(FETCH_SAVE_SESSION_ERR, -607, true);                   \
    E(FETCH_MEMORY_EXCEEDED, -608, true);                    \
    E(READER_IS_UNINITIALIZED, -700, true);                  \
    E(READER_GET_ITERATOR_ERROR, -701, true);                \
    E(CAPTURE_ROWSET_READER_ERROR, -702, true);              \
    E(READER_READING_ERROR, -703, true);                     \
    E(READER_INITIALIZE_ERROR, -704, true);                  \
    E(BE_VERSION_NOT_MATCH, -800, true);                     \
    E(BE_REPLACE_VERSIONS_ERROR, -801, true);                \
    E(BE_MERGE_ERROR, -802, true);                           \
    E(CAPTURE_ROWSET_ERROR, -804, true);                     \
    E(BE_SAVE_HEADER_ERROR, -805, true);                     \
    E(BE_INIT_OLAP_DATA, -806, true);                        \
    E(BE_TRY_OBTAIN_VERSION_LOCKS, -807, true);              \
    E(BE_NO_SUITABLE_VERSION, -808, false);                  \
    E(BE_INVALID_NEED_MERGED_VERSIONS, -810, true);          \
    E(BE_ERROR_DELETE_ACTION, -811, true);                   \
    E(BE_SEGMENTS_OVERLAPPING, -812, true);                  \
    E(PUSH_INIT_ERROR, -900, true);                          \
    E(PUSH_VERSION_INCORRECT, -902, true);                   \
    E(PUSH_SCHEMA_MISMATCH, -903, true);                     \
    E(PUSH_CHECKSUM_ERROR, -904, true);                      \
    E(PUSH_ACQUIRE_DATASOURCE_ERROR, -905, true);            \
    E(PUSH_CREAT_CUMULATIVE_ERROR, -906, true);              \
    E(PUSH_BUILD_DELTA_ERROR, -907, true);                   \
    E(PUSH_VERSION_ALREADY_EXIST, -908, false);              \
    E(PUSH_TABLE_NOT_EXIST, -909, true);                     \
    E(PUSH_INPUT_DATA_ERROR, -910, true);                    \
    E(PUSH_TRANSACTION_ALREADY_EXIST, -911, false);          \
    E(PUSH_BATCH_PROCESS_REMOVED, -912, true);               \
    E(PUSH_COMMIT_ROWSET, -913, true);                       \
    E(PUSH_ROWSET_NOT_FOUND, -914, true);                    \
    E(INDEX_LOAD_ERROR, -1000, true);                        \
    E(INDEX_CHECKSUM_ERROR, -1002, true);                    \
    E(INDEX_DELTA_PRUNING, -1003, true);                     \
    E(DATA_ROW_BLOCK_ERROR, -1100, true);                    \
    E(DATA_FILE_TYPE_ERROR, -1101, true);                    \
    E(WRITER_INDEX_WRITE_ERROR, -1200, true);                \
    E(WRITER_DATA_WRITE_ERROR, -1201, true);                 \
    E(WRITER_ROW_BLOCK_ERROR, -1202, true);                  \
    E(WRITER_SEGMENT_NOT_FINALIZED, -1203, true);            \
    E(ROWBLOCK_DECOMPRESS_ERROR, -1300, true);               \
    E(ROWBLOCK_FIND_ROW_EXCEPTION, -1301, true);             \
    E(HEADER_ADD_VERSION, -1400, true);                      \
    E(HEADER_DELETE_VERSION, -1401, true);                   \
    E(HEADER_ADD_PENDING_DELTA, -1402, true);                \
    E(HEADER_ADD_INCREMENTAL_VERSION, -1403, true);          \
    E(HEADER_INVALID_FLAG, -1404, true);                     \
    E(HEADER_LOAD_INVALID_KEY, -1408, true);                 \
    E(HEADER_LOAD_JSON_HEADER, -1410, true);                 \
    E(HEADER_INIT_FAILED, -1411, true);                      \
    E(HEADER_PB_PARSE_FAILED, -1412, true);                  \
    E(HEADER_HAS_PENDING_DATA, -1413, true);                 \
    E(SCHEMA_SCHEMA_INVALID, -1500, true);                   \
    E(SCHEMA_SCHEMA_FIELD_INVALID, -1501, true);             \
    E(ALTER_MULTI_TABLE_ERR, -1600, true);                   \
    E(ALTER_DELTA_DOES_NOT_EXISTS, -1601, true);             \
    E(ALTER_STATUS_ERR, -1602, true);                        \
    E(PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED, -1603, true);     \
    E(SCHEMA_CHANGE_INFO_INVALID, -1604, true);              \
    E(QUERY_SPLIT_KEY_ERR, -1605, true);                     \
    E(DATA_QUALITY_ERR, -1606, false);                       \
    E(COLUMN_DATA_LOAD_BLOCK, -1700, true);                  \
    E(COLUMN_DATA_RECORD_INDEX, -1701, true);                \
    E(COLUMN_DATA_MAKE_FILE_HEADER, -1702, true);            \
    E(COLUMN_DATA_READ_VAR_INT, -1703, true);                \
    E(COLUMN_DATA_PATCH_LIST_NUM, -1704, true);              \
    E(COLUMN_READ_STREAM, -1706, true);                      \
    E(COLUMN_STREAM_NOT_EXIST, -1716, true);                 \
    E(COLUMN_VALUE_NULL, -1717, true);                       \
    E(COLUMN_SEEK_ERROR, -1719, true);                       \
    E(COLUMN_NO_MATCH_OFFSETS_SIZE, -1720, true);            \
    E(COLUMN_NO_MATCH_FILTER_SIZE, -1721, true);             \
    E(DELETE_INVALID_CONDITION, -1900, true);                \
    E(DELETE_UPDATE_HEADER_FAILED, -1901, true);             \
    E(DELETE_SAVE_HEADER_FAILED, -1902, true);               \
    E(DELETE_INVALID_PARAMETERS, -1903, true);               \
    E(DELETE_INVALID_VERSION, -1904, true);                  \
    E(CUMULATIVE_NO_SUITABLE_VERSION, -2000, false);         \
    E(CUMULATIVE_REPEAT_INIT, -2001, true);                  \
    E(CUMULATIVE_INVALID_PARAMETERS, -2002, true);           \
    E(CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE, -2003, true);   \
    E(CUMULATIVE_INVALID_NEED_MERGED_VERSIONS, -2004, true); \
    E(CUMULATIVE_ERROR_DELETE_ACTION, -2005, true);          \
    E(CUMULATIVE_MISS_VERSION, -2006, true);                 \
    E(FULL_NO_SUITABLE_VERSION, -2008, false);               \
    E(FULL_MISS_VERSION, -2009, true);                       \
    E(META_INVALID_ARGUMENT, -3000, true);                   \
    E(META_OPEN_DB_ERROR, -3001, true);                      \
    E(META_KEY_NOT_FOUND, -3002, false);                     \
    E(META_GET_ERROR, -3003, true);                          \
    E(META_PUT_ERROR, -3004, true);                          \
    E(META_ITERATOR_ERROR, -3005, true);                     \
    E(META_DELETE_ERROR, -3006, true);                       \
    E(META_ALREADY_EXIST, -3007, true);                      \
    E(ROWSET_WRITER_INIT, -3100, true);                      \
    E(ROWSET_SAVE_FAILED, -3101, true);                      \
    E(ROWSET_GENERATE_ID_FAILED, -3102, true);               \
    E(ROWSET_DELETE_FILE_FAILED, -3103, true);               \
    E(ROWSET_BUILDER_INIT, -3104, true);                     \
    E(ROWSET_TYPE_NOT_FOUND, -3105, true);                   \
    E(ROWSET_ALREADY_EXIST, -3106, true);                    \
    E(ROWSET_CREATE_READER, -3107, true);                    \
    E(ROWSET_INVALID, -3108, true);                          \
    E(ROWSET_READER_INIT, -3110, true);                      \
    E(ROWSET_INVALID_STATE_TRANSITION, -3112, true);         \
    E(STRING_OVERFLOW_IN_VEC_ENGINE, -3113, true);           \
    E(ROWSET_ADD_MIGRATION_V2, -3114, true);                 \
    E(PUBLISH_VERSION_NOT_CONTINUOUS, -3115, false);         \
    E(ROWSET_RENAME_FILE_FAILED, -3116, false);              \
    E(SEGCOMPACTION_INIT_READER, -3117, false);              \
    E(SEGCOMPACTION_INIT_WRITER, -3118, false);              \
    E(SEGCOMPACTION_FAILED, -3119, false);                   \
    E(PIP_WAIT_FOR_RF, -3120, false);                        \
    E(PIP_WAIT_FOR_SC, -3121, false);                        \
    E(ROWSET_ADD_TO_BINLOG_FAILED, -3122, true);             \
    E(ROWSET_BINLOG_NOT_ONLY_ONE_VERSION, -3123, true);      \
    E(INVERTED_INDEX_INVALID_PARAMETERS, -6000, false);      \
    E(INVERTED_INDEX_NOT_SUPPORTED, -6001, false);           \
    E(INVERTED_INDEX_CLUCENE_ERROR, -6002, false);           \
    E(INVERTED_INDEX_FILE_NOT_FOUND, -6003, false);          \
    E(INVERTED_INDEX_BYPASS, -6004, false);                  \
    E(INVERTED_INDEX_NO_TERMS, -6005, false);                \
    E(INVERTED_INDEX_RENAME_FILE_FAILED, -6006, true);       \
    E(INVERTED_INDEX_EVALUATE_SKIPPED, -6007, false);        \
    E(INVERTED_INDEX_BUILD_WAITTING, -6008, false);          \
    E(INVERTED_INDEX_NOT_IMPLEMENTED, -6009, false);         \
    E(INVERTED_INDEX_COMPACTION_ERROR, -6010, false);        \
    E(INVERTED_INDEX_ANALYZER_ERROR, -6011, false);          \
    E(KEY_NOT_FOUND, -7000, false);                          \
    E(KEY_ALREADY_EXISTS, -7001, false);                     \
    E(ENTRY_NOT_FOUND, -7002, false);                        \
    E(INVALID_TABLET_STATE, -7211, false);                   \
    E(ROWSETS_EXPIRED, -7311, false);

// Define constexpr int error_code_name = error_code_value
#define M(NAME, ERRORCODE, ENABLESTACKTRACE) constexpr int NAME = ERRORCODE;
APPLY_FOR_OLAP_ERROR_CODES(M)
#undef M

#define MM(name, ENABLESTACKTRACE) constexpr int name = TStatusCode::name;
APPLY_FOR_THRIFT_ERROR_CODES(MM)
#undef MM

constexpr int MAX_ERROR_CODE_DEFINE_NUM = 65536;
struct ErrorCodeState {
    int16_t error_code = 0;
    bool stacktrace = true;
    std::string description;
    size_t count = 0; // Used for count the number of error happens
    std::mutex mutex; // lock guard for count state
};
extern ErrorCodeState error_states[MAX_ERROR_CODE_DEFINE_NUM];

class ErrorCodeInitializer {
public:
    ErrorCodeInitializer(int temp) : signal_value(temp) {
        for (int i = 0; i < MAX_ERROR_CODE_DEFINE_NUM; ++i) {
            error_states[i].error_code = 0;
        }
#define M(NAME, ENABLESTACKTRACE)                                  \
    error_states[TStatusCode::NAME].stacktrace = ENABLESTACKTRACE; \
    error_states[TStatusCode::NAME].description = #NAME;           \
    error_states[TStatusCode::NAME].error_code = TStatusCode::NAME;
        APPLY_FOR_THRIFT_ERROR_CODES(M)
#undef M
// In status.h, if error code > 0, then it means it will be used in TStatusCode and will
// also be used in FE.
// Other error codes that with error code < 0, will only be used in BE.
// We use abs(error code) as the index in error_states, so that these two kinds of error
// codes MUST not have overlap.
// Add an assert here to make sure the code in TStatusCode and other error code are not
// overlapped.
#define M(NAME, ERRORCODE, ENABLESTACKTRACE)                    \
    assert(error_states[abs(ERRORCODE)].error_code == 0);       \
    error_states[abs(ERRORCODE)].stacktrace = ENABLESTACKTRACE; \
    error_states[abs(ERRORCODE)].error_code = ERRORCODE;
        APPLY_FOR_OLAP_ERROR_CODES(M)
#undef M
    }

    void check_init() {
        //the signal value is 0, it means the global error states not inited, it's logical error
        // DO NOT use dcheck here, because dcheck depend on glog, and glog maybe not inited at this time.
        if (signal_value == 0) {
            exit(-1);
        }
    }

private:
    int signal_value = 0;
};

extern ErrorCodeInitializer error_code_init;
} // namespace ErrorCode

class [[nodiscard]] Status {
public:
    Status() : _code(ErrorCode::OK), _err_msg(nullptr) {}

    // used to convert Exception to Status
    Status(int code, std::string msg, std::string stack = "") : _code(code) {
        _err_msg = std::make_unique<ErrMsg>();
        _err_msg->_msg = std::move(msg);
        if (config::enable_stacktrace) {
            _err_msg->_stack = std::move(stack);
        }
    }

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
        if (stacktrace && ErrorCode::error_states[abs(code)].stacktrace &&
            config::enable_stacktrace) {
            // Delete the first one frame pointers, which are inside the status.h
            status._err_msg->_stack = get_stack_trace(1);
            LOG(WARNING) << "meet error status: " << status; // may print too many stacks.
        }
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
        if (stacktrace && ErrorCode::error_states[abs(code)].stacktrace &&
            config::enable_stacktrace) {
            status._err_msg->_stack = get_stack_trace(1);
            LOG(WARNING) << "meet error status: " << status; // may print too many stacks.
        }
        return status;
    }

    static Status OK() { return Status(); }

#define ERROR_CTOR(name, code)                                                       \
    template <bool stacktrace = true, typename... Args>                              \
    static Status name(std::string_view msg, Args&&... args) {                       \
        return Error<ErrorCode::code, stacktrace>(msg, std::forward<Args>(args)...); \
    }

    ERROR_CTOR(PublishTimeout, PUBLISH_TIMEOUT)
    ERROR_CTOR(MemoryAllocFailed, MEM_ALLOC_FAILED)
    ERROR_CTOR(BufferAllocFailed, BUFFER_ALLOCATION_FAILED)
    ERROR_CTOR(InvalidArgument, INVALID_ARGUMENT)
    ERROR_CTOR(InvalidJsonPath, INVALID_JSON_PATH)
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

    // Convert into TStatus.
    void to_thrift(TStatus* status) const;
    TStatus to_thrift() const;
    void to_protobuf(PStatus* status) const;

    std::string to_string() const;
    std::string to_string_no_stack() const;

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

    std::string_view msg() const { return _err_msg ? _err_msg->_msg : std::string_view(""); }

    std::pair<int, std::string> retrieve_error_msg() { return {_code, std::move(_err_msg->_msg)}; }

    friend io::ObjectStorageStatus convert_to_obj_response(Status st);

private:
    int _code;
    struct ErrMsg {
        std::string _msg;
        std::string _stack;
    };
    std::unique_ptr<ErrMsg> _err_msg;

    std::string code_as_string() const {
        return (int)_code >= 0 ? doris::to_string(static_cast<TStatusCode::type>(_code))
                               : fmt::format("E{}", (int16_t)_code);
    }
};

// There are many thread using status to indicate the cancel state, one thread may update it and
// the other thread will read it. Status is not thread safe, for example, if one thread is update it
// and another thread is call to_string method, it may core, because the _err_msg is an unique ptr and
// it is deconstructed during copy method.
// And also we could not use lock, because we need get status frequently to check if it is cancelled.
// The defaule value is ok.
class AtomicStatus {
public:
    AtomicStatus() : error_st_(Status::OK()) {}

    bool ok() const { return error_code_.load(std::memory_order_acquire) == 0; }

    bool update(const Status& new_status) {
        // If new status is normal, or the old status is abnormal, then not need update
        if (new_status.ok() || error_code_.load(std::memory_order_acquire) != 0) {
            return false;
        }
        std::lock_guard l(mutex_);
        if (error_code_.load(std::memory_order_acquire) != 0) {
            return false;
        }
        error_st_ = new_status;
        error_code_.store(new_status.code(), std::memory_order_release);
        return true;
    }

    // will copy a new status object to avoid concurrency
    // This stauts could only be called when ok==false
    Status status() const {
        std::lock_guard l(mutex_);
        return error_st_;
    }

private:
    std::atomic_int16_t error_code_ = 0;
    Status error_st_;
    // mutex's lock is not a const method, but we will use this mutex in
    // some const method, so that it should be mutable.
    mutable std::mutex mutex_;

    AtomicStatus(const AtomicStatus&) = delete;
    void operator=(const AtomicStatus&) = delete;
};

inline std::ostream& operator<<(std::ostream& ostr, const Status& status) {
    ostr << '[' << status.code_as_string() << ']';
    ostr << status.msg();
    if (status._err_msg && !status._err_msg->_stack.empty() && config::enable_stacktrace) {
        ostr << '\n' << status._err_msg->_stack;
    }
    return ostr;
}

inline std::string Status::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

inline std::string Status::to_string_no_stack() const {
    return fmt::format("[{}]{}", code_as_string(), msg());
}

// some generally useful macros
#define RETURN_IF_ERROR(stmt)           \
    do {                                \
        Status _status_ = (stmt);       \
        if (UNLIKELY(!_status_.ok())) { \
            return _status_;            \
        }                               \
    } while (false)

#define PROPAGATE_FALSE(stmt)                     \
    do {                                          \
        if (UNLIKELY(!static_cast<bool>(stmt))) { \
            return false;                         \
        }                                         \
    } while (false)

#define THROW_IF_ERROR(stmt)            \
    do {                                \
        Status _status_ = (stmt);       \
        if (UNLIKELY(!_status_.ok())) { \
            throw Exception(_status_);  \
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

#define RETURN_FALSE_IF_ERROR(stmt)   \
    do {                              \
        Status status = (stmt);       \
        if (UNLIKELY(!status.ok())) { \
            return false;             \
        }                             \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)              \
    do {                                                    \
        Status _s = (to_call);                              \
        if (UNLIKELY(!_s.ok())) {                           \
            LOG(WARNING) << (warning_prefix) << ": " << _s; \
        }                                                   \
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

using ResultError = unexpected<Status>;

#define RETURN_IF_ERROR_RESULT(stmt)                \
    do {                                            \
        Status _status_ = (stmt);                   \
        if (UNLIKELY(!_status_.ok())) {             \
            return unexpected(std::move(_status_)); \
        }                                           \
    } while (false)

#define DORIS_TRY(stmt)                          \
    ({                                           \
        auto&& res = (stmt);                     \
        using T = std::decay_t<decltype(res)>;   \
        if (!res.has_value()) [[unlikely]] {     \
            return std::forward<T>(res).error(); \
        }                                        \
        std::forward<T>(res).value();            \
    });

} // namespace doris

// specify formatter for Status
template <>
struct fmt::formatter<doris::Status> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(doris::Status const& status, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}", status.to_string());
    }
};
