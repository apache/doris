// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gen_cpp/Status_types.h" // for TStatus
#include "gen_cpp/types.pb.h"     // for PStatus
#include "util/slice.h"           // for Slice

namespace doris {

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
    Status(const Status& rhs) {
        *this = rhs;
    }

    // move c'tor
    Status(Status&& rhs) {
        *this = rhs;
    }

    // same as copy c'tor
    Status& operator=(const Status& rhs) {
        if (rhs._length) {
            memcpy(_state, rhs._state, rhs._length + Status::HEADER_LEN);
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

    bool ok() const { return _length == 0; }
    void set_ok() { _length = 0; }

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

    int16_t precise_code() const {
        return ok() ? 0 : _precise_code;
    }

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

    operator bool() const { return this->ok(); }

private:
    void assemble_state(TStatusCode::type code, const Slice& msg, int16_t precise_code, const Slice& msg2) {
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

        _length = size;
        _code = (char)code;
        _precise_code = precise_code;

        // copy msg
        char* result =  _state + HEADER_LEN;
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
            int64_t _length : 32;       // message length
            int64_t _code : 8;          
            int64_t _precise_code : 16;
            int64_t _message : 8;       // save message since here
        };
    };
};

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
