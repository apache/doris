// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <fmt/format.h>
#include <iostream>
#include <string>
#include <vector>

#include <boost/stacktrace.hpp>
#include <glog/logging.h>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gen_cpp/Status_types.h" // for TStatus
#include "gen_cpp/types.pb.h"     // for PStatus
#include "util/slice.h"           // for Slice

namespace doris {

// ErrorName, ErrorCode, String Description, Should print stacktrace
#define APPLY_FOR_ERROR_CODES(M)                                         \
    M(OLAP_SUCCESS, 0, "", false)                                        \
    M(OLAP_ERR_OTHER_ERROR, -1, "", true)                                \
    M(OLAP_REQUEST_FAILED, -2, "", true)                                 \
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
    M(OLAP_ERR_CE_TRY_CE_LOCK_ERROR, -306, "", true)                     \
    M(OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR, -400, "", true)            \
    M(OLAP_ERR_TABLE_VERSION_INDEX_MISMATCH_ERROR, -401, "", true)       \
    M(OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR, -402, "", true)               \
    M(OLAP_ERR_TABLE_INDEX_FIND_ERROR, -403, "", true)                   \
    M(OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR, -404, "", true)           \
    M(OLAP_ERR_TABLE_CREATE_META_ERROR, -405, "", true)                  \
    M(OLAP_ERR_TABLE_ALREADY_DELETED_ERROR, -406, "", true)              \
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
    M(OLAP_ERR_HEADER_HAS_PENDING_DATA, -1413, "", true)                 \
    M(OLAP_ERR_SCHEMA_SCHEMA_INVALID, -1500, "", true)                   \
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
    M(OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION, -2000, "", true)          \
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
    M(OLAP_ERR_ROWSET_ADD_MIGRATION_V2, -3114, "", true)

enum ErrorCode {
#define M(NAME, ERRORCODE, DESC, STACKTRACEENABLED) NAME = ERRORCODE,
    APPLY_FOR_ERROR_CODES(M)
#undef M
};

class Status {
    enum {
        // If the error and log returned by the query are truncated, the status to string may be too long.
        STATE_CAPACITY = 2048,
        HEADER_LEN = 7,
        MESSAGE_LEN = STATE_CAPACITY - HEADER_LEN
    };

public:
    Status() : _length(0) {}

    // copy c'tor makes copy of error detail so Status can be returned by value
    Status(const Status& rhs) { *this = rhs; }

    // move c'tor
    Status(Status&& rhs) { *this = rhs; }

    // same as copy c'tor
    Status& operator=(const Status& rhs) {
        if (rhs._length) {
            memcpy(_state, rhs._state, rhs._length);
        } else {
            _length = 0;
        }
        return *this;
    }

    // move assign
    Status& operator=(Status&& rhs) {
        this->operator=(rhs);
        return *this;
    }

    // "Copy" c'tor from TStatus.
    Status(const TStatus& status);

    Status(const PStatus& pstatus);

    static Status OK() { return Status(); }

    static Status PublishTimeout(const Slice& msg, int16_t precise_code = 1,
                                 const Slice& msg2 = Slice()) {
        return Status(TStatusCode::PUBLISH_TIMEOUT, msg, precise_code, msg2);
    }
    static Status MemoryAllocFailed(const Slice& msg, int16_t precise_code = 1,
                                    const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_ALLOC_FAILED, msg, precise_code, msg2);
    }
    static Status BufferAllocFailed(const Slice& msg, int16_t precise_code = 1,
                                    const Slice& msg2 = Slice()) {
        return Status(TStatusCode::BUFFER_ALLOCATION_FAILED, msg, precise_code, msg2);
    }
    static Status InvalidArgument(const Slice& msg, int16_t precise_code = 1,
                                  const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INVALID_ARGUMENT, msg, precise_code, msg2);
    }
    static Status MinimumReservationUnavailable(const Slice& msg, int16_t precise_code = 1,
                                                const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE, msg, precise_code, msg2);
    }
    static Status Corruption(const Slice& msg, int16_t precise_code = 1,
                             const Slice& msg2 = Slice()) {
        return Status(TStatusCode::CORRUPTION, msg, precise_code, msg2);
    }
    static Status IOError(const Slice& msg, int16_t precise_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::IO_ERROR, msg, precise_code, msg2);
    }
    static Status NotFound(const Slice& msg, int16_t precise_code = 1,
                           const Slice& msg2 = Slice()) {
        return Status(TStatusCode::NOT_FOUND, msg, precise_code, msg2);
    }
    static Status AlreadyExist(const Slice& msg, int16_t precise_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::ALREADY_EXIST, msg, precise_code, msg2);
    }
    static Status NotSupported(const Slice& msg, int16_t precise_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::NOT_IMPLEMENTED_ERROR, msg, precise_code, msg2);
    }
    static Status EndOfFile(const Slice& msg, int16_t precise_code = 1,
                            const Slice& msg2 = Slice()) {
        return Status(TStatusCode::END_OF_FILE, msg, precise_code, msg2);
    }
    static Status InternalError(const Slice& msg, int16_t precise_code = 1,
                                const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INTERNAL_ERROR, msg, precise_code, msg2);
    }
    static Status RuntimeError(const Slice& msg, int16_t precise_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::RUNTIME_ERROR, msg, precise_code, msg2);
    }
    static Status Cancelled(const Slice& msg, int16_t precise_code = 1,
                            const Slice& msg2 = Slice()) {
        return Status(TStatusCode::CANCELLED, msg, precise_code, msg2);
    }

    static Status MemoryLimitExceeded(const Slice& msg, int16_t precise_code = 1,
                                      const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_LIMIT_EXCEEDED, msg, precise_code, msg2);
    }

    static Status ThriftRpcError(const Slice& msg, int16_t precise_code = 1,
                                 const Slice& msg2 = Slice()) {
        return Status(TStatusCode::THRIFT_RPC_ERROR, msg, precise_code, msg2);
    }

    static Status TimedOut(const Slice& msg, int16_t precise_code = 1,
                           const Slice& msg2 = Slice()) {
        return Status(TStatusCode::TIMEOUT, msg, precise_code, msg2);
    }

    static Status TooManyTasks(const Slice& msg, int16_t precise_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::TOO_MANY_TASKS, msg, precise_code, msg2);
    }
    static Status ServiceUnavailable(const Slice& msg, int16_t precise_code = -1,
                                     const Slice& msg2 = Slice()) {
        return Status(TStatusCode::SERVICE_UNAVAILABLE, msg, precise_code, msg2);
    }
    static Status Uninitialized(const Slice& msg, int16_t precise_code = -1,
                                const Slice& msg2 = Slice()) {
        return Status(TStatusCode::UNINITIALIZED, msg, precise_code, msg2);
    }
    static Status Aborted(const Slice& msg, int16_t precise_code = -1,
                          const Slice& msg2 = Slice()) {
        return Status(TStatusCode::ABORTED, msg, precise_code, msg2);
    }

    static Status DataQualityError(const Slice& msg, int16_t precise_code = -1,
                                   const Slice& msg2 = Slice()) {
        return Status(TStatusCode::DATA_QUALITY_ERROR, msg, precise_code, msg2);
    }

    template <typename... Args>
    static Status OLAPInternalError(int16_t precise_code, const std::string& fmt, Args&&... args) {
        return ConstructErrorStatus(precise_code, fmt::format(fmt, std::forward<Args>(args)...));
    }

    // A wrapper for ErrorCode
    //      Precise code is for ErrorCode's enum value
    //      All Status Error is treated as Internal Error
    static Status OLAPInternalError(int16_t precise_code) {
        return ConstructErrorStatus(precise_code, Slice());
    }

    static Status ConstructErrorStatus(int16_t precise_code, const Slice& msg);

    bool ok() const { return _length == 0; }

    bool is_cancelled() const { return code() == TStatusCode::CANCELLED; }
    bool is_mem_limit_exceeded() const { return code() == TStatusCode::MEM_LIMIT_EXCEEDED; }
    bool is_thrift_rpc_error() const { return code() == TStatusCode::THRIFT_RPC_ERROR; }
    bool is_end_of_file() const { return code() == TStatusCode::END_OF_FILE; }
    bool is_not_found() const { return code() == TStatusCode::NOT_FOUND; }
    bool is_already_exist() const { return code() == TStatusCode::ALREADY_EXIST; }
    bool is_io_error() const { return code() == TStatusCode::IO_ERROR; }

    /// @return @c true iff the status indicates Uninitialized.
    bool is_uninitialized() const { return code() == TStatusCode::UNINITIALIZED; }

    // @return @c true iff the status indicates an Aborted error.
    bool is_aborted() const { return code() == TStatusCode::ABORTED; }

    /// @return @c true iff the status indicates an InvalidArgument error.
    bool is_invalid_argument() const { return code() == TStatusCode::INVALID_ARGUMENT; }

    // @return @c true iff the status indicates ServiceUnavailable.
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

    std::string get_error_msg() const {
        auto msg = message();
        return std::string(msg.data, msg.size);
    }

    /// @return A string representation of this status suitable for printing.
    ///   Returns the string "OK" for success.
    std::string to_string() const;

    /// @return A string representation of the status code, without the message
    ///   text or sub code information.
    std::string code_as_string() const;

    // This is similar to to_string, except that it does not include
    // the stringified error code or sub code.
    //
    // @note The returned Slice is only valid as long as this Status object
    //   remains live and unchanged.
    //
    // @return The message portion of the Status. For @c OK statuses,
    //   this returns an empty string.
    Slice message() const;

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
    /// @return A new Status object with the same state plus an additional
    ///   leading message.
    Status clone_and_prepend(const Slice& msg) const;

    /// Clone this status and add the specified suffix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to append.
    /// @return A new Status object with the same state plus an additional
    ///   trailing message.
    Status clone_and_append(const Slice& msg) const;

    // if(!status) or if (status) will use this operator
    operator bool() const { return this->ok(); }

    // Used like if (res == Status::OK())
    // if the state is ok, then both code and precise code is not initialized properly, so that should check ok state
    // ignore error messages during comparison
    bool operator==(const Status& st) {
        return ok() ? st.ok() : code() == st.code() && precise_code() == st.precise_code();
    }

    // Used like if (res != Status::OK())
    bool operator!=(const Status& st) {
        return ok() ? !st.ok() : code() != st.code() || precise_code() != st.precise_code();
    }

private:
    void assemble_state(TStatusCode::type code, const Slice& msg, int16_t precise_code,
                        const Slice& msg2) {
        DCHECK(code != TStatusCode::OK);
        uint32_t len1 = msg.size;
        uint32_t len2 = msg2.size;
        uint32_t size = len1 + ((len2 > 0) ? (2 + len2) : 0);

        // limited to MESSAGE_LEN
        if (UNLIKELY(size > MESSAGE_LEN)) {
            std::string str = code_as_string();
            str.append(": ");
            str.append(msg.data, msg.size);
            char buf[64] = {};
            int n = snprintf(buf, sizeof(buf), " precise_code:%d ", precise_code);
            str.append(buf, n);
            str.append(msg2.data, msg2.size);
            LOG(WARNING) << "warning: Status msg truncated, " << str;
            size = MESSAGE_LEN;
        }

        _length = size + HEADER_LEN;
        _code = (char)code;
        _precise_code = precise_code;

        // copy msg
        char* result = _state + HEADER_LEN;
        uint32_t len = std::min<uint32_t>(len1, MESSAGE_LEN);
        memcpy(result, msg.data, len);

        // copy msg2
        if (len2 > 0 && len < MESSAGE_LEN - 2) {
            result[len++] = ':';
            result[len++] = ' ';
            memcpy(&result[len], msg2.data, std::min<uint32_t>(len2, MESSAGE_LEN - len));
        }
    }

    Status(TStatusCode::type code, const Slice& msg, int16_t precise_code, const Slice& msg2) {
        assemble_state(code, msg, precise_code, msg2);
    }

private:
    // OK status has a zero _length.  Otherwise, _state is a static array
    // of the following form:
    //    _state[0..3] == length of message
    //    _state[4]    == code
    //    _state[5..6] == precise_code
    //    _state[7..]  == message
    union {
        char _state[STATE_CAPACITY];

        struct {
            // Message length == HEADER(7 bytes) + message size
            // Sometimes error message is empty, so that we could not use length==0 to indicate
            // whether there is error happens
            int64_t _length : 32;
            int64_t _code : 8;
            int64_t _precise_code : 16;
            int64_t _message : 8; // save message since here
        };
    };
};

// Override the << operator, it is used during LOG(INFO) << "xxxx" << status;
// Add inline here to dedup many includes
inline std::ostream& operator<<(std::ostream& ostr, const Status& param) {
    return ostr << param.to_string();
}

// some generally useful macros
#define RETURN_IF_ERROR(stmt)            \
    do {                                 \
        const Status& _status_ = (stmt); \
        if (UNLIKELY(!_status_.ok())) {  \
            return _status_;             \
        }                                \
    } while (false)

#define RETURN_IF_STATUS_ERROR(status, stmt) \
    do {                                     \
        status = (stmt);                     \
        if (UNLIKELY(!status.ok())) {        \
            return;                          \
        }                                    \
    } while (false)

#define EXIT_IF_ERROR(stmt)                        \
    do {                                           \
        const Status& _status_ = (stmt);           \
        if (UNLIKELY(!_status_.ok())) {            \
            string msg = _status_.get_error_msg(); \
            LOG(ERROR) << msg;                     \
            exit(1);                               \
        }                                          \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)                          \
    do {                                                                \
        const Status& _s = (to_call);                                   \
        if (UNLIKELY(!_s.ok())) {                                       \
            LOG(WARNING) << (warning_prefix) << ": " << _s.to_string(); \
        }                                                               \
    } while (false);

#define RETURN_WITH_WARN_IF_ERROR(stmt, ret_code, warning_prefix)              \
    do {                                                                       \
        const Status& _s = (stmt);                                             \
        if (UNLIKELY(!_s.ok())) {                                              \
            LOG(WARNING) << (warning_prefix) << ", error: " << _s.to_string(); \
            return ret_code;                                                   \
        }                                                                      \
    } while (false);

#define RETURN_NOT_OK_STATUS_WITH_WARN(stmt, warning_prefix)                   \
    do {                                                                       \
        const Status& _s = (stmt);                                             \
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