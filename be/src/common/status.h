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
#include "common/logging.h"
#include "gen_cpp/Status_types.h" // for TStatus

namespace doris {

class PStatus;

// ErrorName, ErrorCode, String Description, Should print stacktrace
#define APPLY_FOR_ERROR_CODES(M)                                         \
    M(OLAP_SUCCESS, 0, "", false)                                        \
    M(OLAP_ERR_OTHER_ERROR, -1, "", true)                                \
    M(OLAP_REQUEST_FAILED, -2, "", false)                                \
    M(OLAP_ERR_OS_ERROR, -100, "", true)                                 \
    M(OLAP_ERR_DIR_NOT_EXIST, -101, "", true)                            \
    M(OLAP_ERR_FILE_NOT_EXIST, -102, "", true)                           \
    M(OLAP_ERR_CREATE_FILE_ERROR, -103, "", true)                        \
    M(OLAP_ERR_MALLOC_ERROR, -104, "", true)                             \
    M(OLAP_ERR_STL_ERROR, -105, "", true)                                \
    M(OLAP_ERR_IO_ERROR, -106, "", true)                                 \
    M(OLAP_ERR_MUTEX_ERROR, -107, "", true)                              \
    M(OLAP_ERR_PTHREAD_ERROR, -108, "", true)                            \
    M(OLAP_ERR_NETWORK_ERROR, -109, "", true)                            \
    M(OLAP_ERR_UB_FUNC_ERROR, -110, "", true)                            \
    M(OLAP_ERR_COMPRESS_ERROR, -111, "", true)                           \
    M(OLAP_ERR_DECOMPRESS_ERROR, -112, "", true)                         \
    M(OLAP_ERR_UNKNOWN_COMPRESSION_TYPE, -113, "", true)                 \
    M(OLAP_ERR_MMAP_ERROR, -114, "", true)                               \
    M(OLAP_ERR_RWLOCK_ERROR, -115, "", true)                             \
    M(OLAP_ERR_READ_UNENOUGH, -116, "", true)                            \
    M(OLAP_ERR_CANNOT_CREATE_DIR, -117, "", true)                        \
    M(OLAP_ERR_UB_NETWORK_ERROR, -118, "", true)                         \
    M(OLAP_ERR_FILE_FORMAT_ERROR, -119, "", true)                        \
    M(OLAP_ERR_EVAL_CONJUNCTS_ERROR, -120, "", true)                     \
    M(OLAP_ERR_COPY_FILE_ERROR, -121, "", true)                          \
    M(OLAP_ERR_FILE_ALREADY_EXIST, -122, "", true)                       \
    M(OLAP_ERR_NOT_INITED, -200, "", true)                               \
    M(OLAP_ERR_FUNC_NOT_IMPLEMENTED, -201, "", true)                     \
    M(OLAP_ERR_CALL_SEQUENCE_ERROR, -202, "", true)                      \
    M(OLAP_ERR_INPUT_PARAMETER_ERROR, -203, "", true)                    \
    M(OLAP_ERR_BUFFER_OVERFLOW, -204, "", true)                          \
    M(OLAP_ERR_CONFIG_ERROR, -205, "", true)                             \
    M(OLAP_ERR_INIT_FAILED, -206, "", true)                              \
    M(OLAP_ERR_INVALID_SCHEMA, -207, "", true)                           \
    M(OLAP_ERR_CHECKSUM_ERROR, -208, "", true)                           \
    M(OLAP_ERR_SIGNATURE_ERROR, -209, "", true)                          \
    M(OLAP_ERR_CATCH_EXCEPTION, -210, "", true)                          \
    M(OLAP_ERR_PARSE_PROTOBUF_ERROR, -211, "", true)                     \
    M(OLAP_ERR_SERIALIZE_PROTOBUF_ERROR, -212, "", true)                 \
    M(OLAP_ERR_WRITE_PROTOBUF_ERROR, -213, "", true)                     \
    M(OLAP_ERR_VERSION_NOT_EXIST, -214, "", true)                        \
    M(OLAP_ERR_TABLE_NOT_FOUND, -215, "", true)                          \
    M(OLAP_ERR_TRY_LOCK_FAILED, -216, "", true)                          \
    M(OLAP_ERR_OUT_OF_BOUND, -218, "", true)                             \
    M(OLAP_ERR_UNDERFLOW, -219, "", true)                                \
    M(OLAP_ERR_FILE_DATA_ERROR, -220, "", true)                          \
    M(OLAP_ERR_TEST_FILE_ERROR, -221, "", true)                          \
    M(OLAP_ERR_INVALID_ROOT_PATH, -222, "", true)                        \
    M(OLAP_ERR_NO_AVAILABLE_ROOT_PATH, -223, "", true)                   \
    M(OLAP_ERR_CHECK_LINES_ERROR, -224, "", true)                        \
    M(OLAP_ERR_INVALID_CLUSTER_INFO, -225, "", true)                     \
    M(OLAP_ERR_TRANSACTION_NOT_EXIST, -226, "", true)                    \
    M(OLAP_ERR_DISK_FAILURE, -227, "", true)                             \
    M(OLAP_ERR_TRANSACTION_ALREADY_COMMITTED, -228, "", true)            \
    M(OLAP_ERR_TRANSACTION_ALREADY_VISIBLE, -229, "", true)              \
    M(OLAP_ERR_VERSION_ALREADY_MERGED, -230, "", true)                   \
    M(OLAP_ERR_LZO_DISABLED, -231, "", true)                             \
    M(OLAP_ERR_DISK_REACH_CAPACITY_LIMIT, -232, "", true)                \
    M(OLAP_ERR_TOO_MANY_TRANSACTIONS, -233, "", true)                    \
    M(OLAP_ERR_INVALID_SNAPSHOT_VERSION, -234, "", true)                 \
    M(OLAP_ERR_TOO_MANY_VERSION, -235, "", true)                         \
    M(OLAP_ERR_NOT_INITIALIZED, -236, "", true)                          \
    M(OLAP_ERR_ALREADY_CANCELLED, -237, "", true)                        \
    M(OLAP_ERR_TOO_MANY_SEGMENTS, -238, "", true)                        \
    M(OLAP_ERR_CE_CMD_PARAMS_ERROR, -300, "", true)                      \
    M(OLAP_ERR_CE_BUFFER_TOO_SMALL, -301, "", true)                      \
    M(OLAP_ERR_CE_CMD_NOT_VALID, -302, "", true)                         \
    M(OLAP_ERR_CE_LOAD_TABLE_ERROR, -303, "", true)                      \
    M(OLAP_ERR_CE_NOT_FINISHED, -304, "", true)                          \
    M(OLAP_ERR_CE_TABLET_ID_EXIST, -305, "", true)                       \
    M(OLAP_ERR_CE_TRY_CE_LOCK_ERROR, -306, "", false)                    \
    M(OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR, -400, "", true)            \
    M(OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR, -401, "", true)       \
    M(OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR, -402, "", true)               \
    M(OLAP_ERR_TABLE_INDEX_FIND_ERROR, -403, "", true)                   \
    M(OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR, -404, "", true)           \
    M(OLAP_ERR_TABLE_CREATE_META_ERROR, -405, "", true)                  \
    M(OLAP_ERR_TABLE_ALREADY_DELETED_ERROR, -406, "", false)             \
    M(OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE, -500, "", true)               \
    M(OLAP_ERR_ENGINE_DROP_NOEXISTS_TABLE, -501, "", true)               \
    M(OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR, -502, "", true)            \
    M(OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, -503, "", true)           \
    M(OLAP_ERR_DELETE_VERSION_ERROR, -504, "", true)                     \
    M(OLAP_ERR_GC_SCAN_PATH_ERROR, -505, "", true)                       \
    M(OLAP_ERR_ENGINE_INSERT_OLD_TABLET, -506, "", true)                 \
    M(OLAP_ERR_FETCH_OTHER_ERROR, -600, "", true)                        \
    M(OLAP_ERR_FETCH_TABLE_NOT_EXIST, -601, "", true)                    \
    M(OLAP_ERR_FETCH_VERSION_ERROR, -602, "", true)                      \
    M(OLAP_ERR_FETCH_SCHEMA_ERROR, -603, "", true)                       \
    M(OLAP_ERR_FETCH_COMPRESSION_ERROR, -604, "", true)                  \
    M(OLAP_ERR_FETCH_CONTEXT_NOT_EXIST, -605, "", true)                  \
    M(OLAP_ERR_FETCH_GET_READER_PARAMS_ERR, -606, "", true)              \
    M(OLAP_ERR_FETCH_SAVE_SESSION_ERR, -607, "", true)                   \
    M(OLAP_ERR_FETCH_MEMORY_EXCEEDED, -608, "", true)                    \
    M(OLAP_ERR_READER_IS_UNINITIALIZED, -700, "", true)                  \
    M(OLAP_ERR_READER_GET_ITERATOR_ERROR, -701, "", true)                \
    M(OLAP_ERR_CAPTURE_ROWSET_READER_ERROR, -702, "", true)              \
    M(OLAP_ERR_READER_READING_ERROR, -703, "", true)                     \
    M(OLAP_ERR_READER_INITIALIZE_ERROR, -704, "", true)                  \
    M(OLAP_ERR_BE_VERSION_NOT_MATCH, -800, "", true)                     \
    M(OLAP_ERR_BE_REPLACE_VERSIONS_ERROR, -801, "", true)                \
    M(OLAP_ERR_BE_MERGE_ERROR, -802, "", true)                           \
    M(OLAP_ERR_CAPTURE_ROWSET_ERROR, -804, "", true)                     \
    M(OLAP_ERR_BE_SAVE_HEADER_ERROR, -805, "", true)                     \
    M(OLAP_ERR_BE_INIT_OLAP_DATA, -806, "", true)                        \
    M(OLAP_ERR_BE_TRY_OBTAIN_VERSION_LOCKS, -807, "", true)              \
    M(OLAP_ERR_BE_NO_SUITABLE_VERSION, -808, "", false)                  \
    M(OLAP_ERR_BE_TRY_BE_LOCK_ERROR, -809, "", true)                     \
    M(OLAP_ERR_BE_INVALID_NEED_MERGED_VERSIONS, -810, "", true)          \
    M(OLAP_ERR_BE_ERROR_DELETE_ACTION, -811, "", true)                   \
    M(OLAP_ERR_BE_SEGMENTS_OVERLAPPING, -812, "", true)                  \
    M(OLAP_ERR_BE_CLONE_OCCURRED, -813, "", true)                        \
    M(OLAP_ERR_PUSH_INIT_ERROR, -900, "", true)                          \
    M(OLAP_ERR_PUSH_DELTA_FILE_EOF, -901, "", false)                     \
    M(OLAP_ERR_PUSH_VERSION_INCORRECT, -902, "", true)                   \
    M(OLAP_ERR_PUSH_SCHEMA_MISMATCH, -903, "", true)                     \
    M(OLAP_ERR_PUSH_CHECKSUM_ERROR, -904, "", true)                      \
    M(OLAP_ERR_PUSH_ACQUIRE_DATASOURCE_ERROR, -905, "", true)            \
    M(OLAP_ERR_PUSH_CREAT_CUMULATIVE_ERROR, -906, "", true)              \
    M(OLAP_ERR_PUSH_BUILD_DELTA_ERROR, -907, "", true)                   \
    M(OLAP_ERR_PUSH_VERSION_ALREADY_EXIST, -908, "", true)               \
    M(OLAP_ERR_PUSH_TABLE_NOT_EXIST, -909, "", true)                     \
    M(OLAP_ERR_PUSH_INPUT_DATA_ERROR, -910, "", true)                    \
    M(OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST, -911, "", true)           \
    M(OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED, -912, "", true)               \
    M(OLAP_ERR_PUSH_COMMIT_ROWSET, -913, "", true)                       \
    M(OLAP_ERR_PUSH_ROWSET_NOT_FOUND, -914, "", true)                    \
    M(OLAP_ERR_INDEX_LOAD_ERROR, -1000, "", true)                        \
    M(OLAP_ERR_INDEX_EOF, -1001, "", false)                              \
    M(OLAP_ERR_INDEX_CHECKSUM_ERROR, -1002, "", true)                    \
    M(OLAP_ERR_INDEX_DELTA_PRUNING, -1003, "", true)                     \
    M(OLAP_ERR_DATA_ROW_BLOCK_ERROR, -1100, "", true)                    \
    M(OLAP_ERR_DATA_FILE_TYPE_ERROR, -1101, "", true)                    \
    M(OLAP_ERR_DATA_EOF, -1102, "", false)                               \
    M(OLAP_ERR_WRITER_INDEX_WRITE_ERROR, -1200, "", true)                \
    M(OLAP_ERR_WRITER_DATA_WRITE_ERROR, -1201, "", true)                 \
    M(OLAP_ERR_WRITER_ROW_BLOCK_ERROR, -1202, "", true)                  \
    M(OLAP_ERR_WRITER_SEGMENT_NOT_FINALIZED, -1203, "", true)            \
    M(OLAP_ERR_ROWBLOCK_DECOMPRESS_ERROR, -1300, "", true)               \
    M(OLAP_ERR_ROWBLOCK_FIND_ROW_EXCEPTION, -1301, "", true)             \
    M(OLAP_ERR_ROWBLOCK_READ_INFO_ERROR, -1302, "", true)                \
    M(OLAP_ERR_HEADER_ADD_VERSION, -1400, "", true)                      \
    M(OLAP_ERR_HEADER_DELETE_VERSION, -1401, "", true)                   \
    M(OLAP_ERR_HEADER_ADD_PENDING_DELTA, -1402, "", true)                \
    M(OLAP_ERR_HEADER_ADD_INCREMENTAL_VERSION, -1403, "", true)          \
    M(OLAP_ERR_HEADER_INVALID_FLAG, -1404, "", true)                     \
    M(OLAP_ERR_HEADER_PUT, -1405, "", true)                              \
    M(OLAP_ERR_HEADER_DELETE, -1406, "", true)                           \
    M(OLAP_ERR_HEADER_GET, -1407, "", true)                              \
    M(OLAP_ERR_HEADER_LOAD_INVALID_KEY, -1408, "", true)                 \
    M(OLAP_ERR_HEADER_FLAG_PUT, -1409, "", true)                         \
    M(OLAP_ERR_HEADER_LOAD_JSON_HEADER, -1410, "", true)                 \
    M(OLAP_ERR_HEADER_INIT_FAILED, -1411, "", true)                      \
    M(OLAP_ERR_HEADER_PB_PARSE_FAILED, -1412, "", true)                  \
    M(OLAP_ERR_HEADER_HAS_PENDING_DATA, -1413, "", false)                \
    M(OLAP_ERR_SCHEMA_SCHEMA_INVALID, -1500, "", false)                  \
    M(OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID, -1501, "", true)             \
    M(OLAP_ERR_ALTER_MULTI_TABLE_ERR, -1600, "", true)                   \
    M(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS, -1601, "", true)             \
    M(OLAP_ERR_ALTER_STATUS_ERR, -1602, "", true)                        \
    M(OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED, -1603, "", true)     \
    M(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID, -1604, "", true)              \
    M(OLAP_ERR_QUERY_SPLIT_KEY_ERR, -1605, "", true)                     \
    M(OLAP_ERR_DATA_QUALITY_ERR, -1606, "", true)                        \
    M(OLAP_ERR_COLUMN_DATA_LOAD_BLOCK, -1700, "", true)                  \
    M(OLAP_ERR_COLUMN_DATA_RECORD_INDEX, -1701, "", true)                \
    M(OLAP_ERR_COLUMN_DATA_MAKE_FILE_HEADER, -1702, "", true)            \
    M(OLAP_ERR_COLUMN_DATA_READ_VAR_INT, -1703, "", true)                \
    M(OLAP_ERR_COLUMN_DATA_PATCH_LIST_NUM, -1704, "", true)              \
    M(OLAP_ERR_COLUMN_STREAM_EOF, -1705, "", false)                      \
    M(OLAP_ERR_COLUMN_READ_STREAM, -1706, "", true)                      \
    M(OLAP_ERR_COLUMN_STREAM_NOT_EXIST, -1716, "", true)                 \
    M(OLAP_ERR_COLUMN_VALUE_NULL, -1717, "", true)                       \
    M(OLAP_ERR_COLUMN_SEEK_ERROR, -1719, "", true)                       \
    M(OLAP_ERR_DELETE_INVALID_CONDITION, -1900, "", true)                \
    M(OLAP_ERR_DELETE_UPDATE_HEADER_FAILED, -1901, "", true)             \
    M(OLAP_ERR_DELETE_SAVE_HEADER_FAILED, -1902, "", true)               \
    M(OLAP_ERR_DELETE_INVALID_PARAMETERS, -1903, "", true)               \
    M(OLAP_ERR_DELETE_INVALID_VERSION, -1904, "", true)                  \
    M(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION, -2000, "", false)         \
    M(OLAP_ERR_CUMULATIVE_REPEAT_INIT, -2001, "", true)                  \
    M(OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS, -2002, "", true)           \
    M(OLAP_ERR_CUMULATIVE_FAILED_ACQUIRE_DATA_SOURCE, -2003, "", true)   \
    M(OLAP_ERR_CUMULATIVE_INVALID_NEED_MERGED_VERSIONS, -2004, "", true) \
    M(OLAP_ERR_CUMULATIVE_ERROR_DELETE_ACTION, -2005, "", true)          \
    M(OLAP_ERR_CUMULATIVE_MISS_VERSION, -2006, "", true)                 \
    M(OLAP_ERR_CUMULATIVE_CLONE_OCCURRED, -2007, "", true)               \
    M(OLAP_ERR_META_INVALID_ARGUMENT, -3000, "", true)                   \
    M(OLAP_ERR_META_OPEN_DB, -3001, "", true)                            \
    M(OLAP_ERR_META_KEY_NOT_FOUND, -3002, "", true)                      \
    M(OLAP_ERR_META_GET, -3003, "", true)                                \
    M(OLAP_ERR_META_PUT, -3004, "", true)                                \
    M(OLAP_ERR_META_ITERATOR, -3005, "", true)                           \
    M(OLAP_ERR_META_DELETE, -3006, "", true)                             \
    M(OLAP_ERR_META_ALREADY_EXIST, -3007, "", true)                      \
    M(OLAP_ERR_ROWSET_WRITER_INIT, -3100, "", true)                      \
    M(OLAP_ERR_ROWSET_SAVE_FAILED, -3101, "", true)                      \
    M(OLAP_ERR_ROWSET_GENERATE_ID_FAILED, -3102, "", true)               \
    M(OLAP_ERR_ROWSET_DELETE_FILE_FAILED, -3103, "", true)               \
    M(OLAP_ERR_ROWSET_BUILDER_INIT, -3104, "", true)                     \
    M(OLAP_ERR_ROWSET_TYPE_NOT_FOUND, -3105, "", true)                   \
    M(OLAP_ERR_ROWSET_ALREADY_EXIST, -3106, "", true)                    \
    M(OLAP_ERR_ROWSET_CREATE_READER, -3107, "", true)                    \
    M(OLAP_ERR_ROWSET_INVALID, -3108, "", true)                          \
    M(OLAP_ERR_ROWSET_LOAD_FAILED, -3109, "", true)                      \
    M(OLAP_ERR_ROWSET_READER_INIT, -3110, "", true)                      \
    M(OLAP_ERR_ROWSET_READ_FAILED, -3111, "", true)                      \
    M(OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION, -3112, "", true)         \
    M(OLAP_ERR_STRING_OVERFLOW_IN_VEC_ENGINE, -3113, "", true)           \
    M(OLAP_ERR_ROWSET_ADD_MIGRATION_V2, -3114, "", true)                 \
    M(OLAP_ERR_PUBLISH_VERSION_NOT_CONTINUOUS, -3115, "", false)         \
    M(OLAP_ERR_ROWSET_RENAME_FILE_FAILED, -3116, "", false)              \
    M(OLAP_ERR_SEGCOMPACTION_INIT_READER, -3117, "", false)              \
    M(OLAP_ERR_SEGCOMPACTION_INIT_WRITER, -3118, "", false)              \
    M(OLAP_ERR_SEGCOMPACTION_FAILED, -3119, "", false)

enum ErrorCode {
#define M(NAME, ERRORCODE, DESC, STACKTRACEENABLED) NAME = ERRORCODE,
    APPLY_FOR_ERROR_CODES(M)
#undef M
};

class Status {
public:
    Status() : _code(TStatusCode::OK), _precise_code(0) {}

    // copy c'tor makes copy of error detail so Status can be returned by value
    Status(const Status& rhs) = default;

    // move c'tor
    Status(Status&& rhs) noexcept = default;

    // same as copy c'tor
    Status& operator=(const Status& rhs) = default;

    // move assign
    Status& operator=(Status&& rhs) noexcept = default;

    // "Copy" c'tor from TStatus.
    Status(const TStatus& status);

    Status(const PStatus& pstatus);

    // Not allow user create status using constructors, could only use util methods
private:
    Status(TStatusCode::type code, std::string_view msg, int16_t precise_code = 1)
            : _code(code), _precise_code(precise_code), _err_msg(msg) {}

    Status(TStatusCode::type code, std::string&& msg, int16_t precise_code = 1)
            : _code(code), _precise_code(precise_code), _err_msg(std::move(msg)) {}

    template <typename... Args>
    static Status ErrorFmt(TStatusCode::type code, std::string_view fmt, Args&&... args) {
        // In some cases, fmt contains '{}' but there are no args.
        if constexpr (sizeof...(args) == 0) {
            return Status(code, fmt);
        } else {
            return Status(code, fmt::format(fmt, std::forward<Args>(args)...));
        }
    }

    template <typename... Args>
    static Status ErrorFmtWithStackTrace(TStatusCode::type code, std::string_view fmt,
                                         Args&&... args) {
        // In some cases, fmt contains '{}' but there are no args.
        if constexpr (sizeof...(args) == 0) {
            return ConstructErrorStatus(code, -1, fmt);
        } else {
            return ConstructErrorStatus(code, -1, fmt::format(fmt, std::forward<Args>(args)...));
        }
    }

public:
    static Status OK() { return Status(); }

    template <typename... Args>
    static Status PublishTimeout(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::PUBLISH_TIMEOUT, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status MemoryAllocFailed(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::MEM_ALLOC_FAILED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status BufferAllocFailed(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::BUFFER_ALLOCATION_FAILED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status InvalidArgument(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::INVALID_ARGUMENT, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    static Status MinimumReservationUnavailable(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE, fmt,
                        std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status Corruption(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::CORRUPTION, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status IOError(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::IO_ERROR, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status NotFound(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::NOT_FOUND, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    static Status AlreadyExist(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::ALREADY_EXIST, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    static Status NotSupported(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::NOT_IMPLEMENTED_ERROR, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    static Status EndOfFile(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::END_OF_FILE, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status InternalError(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::INTERNAL_ERROR, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status RuntimeError(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::RUNTIME_ERROR, fmt, std::forward<Args>(args)...);
    }
    template <typename... Args>
    static Status Cancelled(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::CANCELLED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status MemoryLimitExceeded(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::MEM_LIMIT_EXCEEDED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status RpcError(std::string_view fmt, Args&&... args) {
        return ErrorFmtWithStackTrace(TStatusCode::THRIFT_RPC_ERROR, fmt,
                                      std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status TimedOut(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::TIMEOUT, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status TooManyTasks(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::TOO_MANY_TASKS, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status ServiceUnavailable(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::SERVICE_UNAVAILABLE, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status Uninitialized(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::UNINITIALIZED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status Aborted(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::ABORTED, fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    static Status DataQualityError(std::string_view fmt, Args&&... args) {
        return ErrorFmt(TStatusCode::DATA_QUALITY_ERROR, fmt, std::forward<Args>(args)...);
    }

    // A wrapper for ErrorCode
    //      Precise code is for ErrorCode's enum value
    //      All Status Error is treated as Internal Error
    static Status OLAPInternalError(int16_t precise_code, std::string_view msg = "");

    static Status ConstructErrorStatus(TStatusCode::type tcode, int16_t precise_code,
                                       std::string_view msg);

    bool ok() const { return _code == TStatusCode::OK; }

    bool is_cancelled() const { return code() == TStatusCode::CANCELLED; }
    bool is_mem_limit_exceeded() const { return code() == TStatusCode::MEM_LIMIT_EXCEEDED; }
    bool is_end_of_file() const { return code() == TStatusCode::END_OF_FILE; }
    bool is_not_found() const { return code() == TStatusCode::NOT_FOUND; }
    bool is_already_exist() const { return code() == TStatusCode::ALREADY_EXIST; }
    bool is_io_error() const {
        auto p_code = precise_code();
        return code() == TStatusCode::IO_ERROR ||
               ((OLAP_ERR_IO_ERROR == p_code || OLAP_ERR_READ_UNENOUGH == p_code) &&
                errno == EIO) ||
               OLAP_ERR_CHECKSUM_ERROR == p_code || OLAP_ERR_FILE_DATA_ERROR == p_code ||
               OLAP_ERR_TEST_FILE_ERROR == p_code || OLAP_ERR_ROWBLOCK_READ_INFO_ERROR == p_code;
    }

    /// @return @c true if the status indicates Uninitialized.
    bool is_uninitialized() const { return code() == TStatusCode::UNINITIALIZED; }

    // @return @c true if the status indicates an Aborted error.
    bool is_aborted() const { return code() == TStatusCode::ABORTED; }

    /// @return @c true if the status indicates an InvalidArgument error.
    bool is_invalid_argument() const { return code() == TStatusCode::INVALID_ARGUMENT; }

    // @return @c true if the status indicates ServiceUnavailable.
    bool is_service_unavailable() const { return code() == TStatusCode::SERVICE_UNAVAILABLE; }

    bool is_data_quality_error() const { return code() == TStatusCode::DATA_QUALITY_ERROR; }

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

    const std::string& get_error_msg() const { return _err_msg; }

    /// @return A string representation of this status suitable for printing.
    ///   Returns the string "OK" for success.
    std::string to_string() const;

    /// @return A json representation of this status.
    std::string to_json() const;

    /// @return A string representation of the status code, without the message
    ///   text or sub code information.
    const char* code_as_string() const;

    TStatusCode::type code() const {
        return ok() ? TStatusCode::OK : static_cast<TStatusCode::type>(_code);
    }

    int16_t precise_code() const { return ok() ? 0 : _precise_code; }

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
    bool operator==(const Status& st) const {
        return ok() ? st.ok() : code() == st.code() && precise_code() == st.precise_code();
    }

    // Used like if (res != Status::OK())
    bool operator!=(const Status& st) const {
        return ok() ? !st.ok() : code() != st.code() || precise_code() != st.precise_code();
    }

private:
    TStatusCode::type _code;
    int16_t _precise_code;
    std::string _err_msg;
};

// Override the << operator, it is used during LOG(INFO) << "xxxx" << status;
// Add inline here to dedup many includes
inline std::ostream& operator<<(std::ostream& ostr, const Status& param) {
    return ostr << param.to_string();
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

#define EXIT_IF_ERROR(stmt)                         \
    do {                                            \
        Status _status_ = (stmt);                   \
        if (UNLIKELY(!_status_.ok())) {             \
            LOG(ERROR) << _status_.get_error_msg(); \
            exit(1);                                \
        }                                           \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)                          \
    do {                                                                \
        Status _s = (to_call);                                          \
        if (UNLIKELY(!_s.ok())) {                                       \
            LOG(WARNING) << (warning_prefix) << ": " << _s.to_string(); \
        }                                                               \
    } while (false);

#define RETURN_WITH_WARN_IF_ERROR(stmt, ret_code, warning_prefix)              \
    do {                                                                       \
        Status _s = (stmt);                                                    \
        if (UNLIKELY(!_s.ok())) {                                              \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s.to_string(); \
            return ret_code;                                                   \
        }                                                                      \
    } while (false);

#define RETURN_NOT_OK_STATUS_WITH_WARN(stmt, warning_prefix)                   \
    do {                                                                       \
        Status _s = (stmt);                                                    \
        if (UNLIKELY(!_s.ok())) {                                              \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s.to_string(); \
            return _s;                                                         \
        }                                                                      \
    } while (false);
} // namespace doris
#ifdef WARN_UNUSED_RESULT
#undef WARN_UNUSED_RESULT
#endif

#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
