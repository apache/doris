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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/compiler_util.h"
#include "gen_cpp/Status_types.h"  // for TStatus
#include "gen_cpp/status.pb.h" // for PStatus
#include "util/slice.h" // for Slice

namespace doris {

class Status {
public:
    Status(): _state(nullptr) {}
    ~Status() noexcept { delete[] _state; }

    // copy c'tor makes copy of error detail so Status can be returned by value
    Status(const Status& s)
            : _state(s._state == nullptr ? nullptr : copy_state(s._state))  {
    }

    // same as copy c'tor
    Status& operator=(const Status& s) {
        // The following condition catches both aliasing (when this == &s),
        // and the common case where both s and *this are OK.
        if (_state != s._state) {
            delete[] _state;
            _state = (s._state == nullptr) ? nullptr : copy_state(s._state);
        }
        return *this;
    }

    // move c'tor
    Status(Status&& s) noexcept : _state(s._state) {
        s._state = nullptr;
    }

    // move assign
    Status& operator=(Status&& s) noexcept {
        std::swap(_state, s._state);
        return *this;
    }

    // "Copy" c'tor from TStatus.
    Status(const TStatus& status);

    Status(const PStatus& pstatus);

    static Status OK() { return Status(); }

    static Status PublishTimeout(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::PUBLISH_TIMEOUT, msg, sub_code, msg2);
    }
    static Status MemoryAllocFailed(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_ALLOC_FAILED, msg, sub_code, msg2);
    }
    static Status BufferAllocFailed(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::BUFFER_ALLOCATION_FAILED, msg, sub_code, msg2);
    }
    static Status InvalidArgument(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INVALID_ARGUMENT, msg, sub_code, msg2);
    }
    static Status MinimumReservationUnavailable(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INVALID_ARGUMENT, msg, sub_code, msg2);
    }
    static Status IoError(const Slice& msg,
                               int16_t sub_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::IO_ERROR, msg, sub_code, msg2);
    }
    static Status EndOfFile(const Slice& msg,
                            int16_t sub_code = 1,
                            const Slice& msg2 = Slice()) {
        return Status(TStatusCode::END_OF_FILE, msg, sub_code, msg2);
    }
    static Status InternalError(const Slice& msg,
                               int16_t sub_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::INTERNAL_ERROR, msg, sub_code, msg2);
    }
    static Status RuntimeError(const Slice& msg,
                               int16_t sub_code = 1,
                               const Slice& msg2 = Slice()) {
        return Status(TStatusCode::RUNTIME_ERROR, msg, sub_code, msg2);
    }
    static Status Cancelled(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::CANCELLED, msg, sub_code, msg2);
    }

    static Status MemoryLimitExceeded(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::MEM_LIMIT_EXCEEDED, msg, sub_code, msg2);
    }

    static Status ThriftRpcError(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::THRIFT_RPC_ERROR, msg, sub_code, msg2);
    }

    static Status TimedOut(const Slice& msg, int16_t sub_code = 1, const Slice& msg2 = Slice()) {
        return Status(TStatusCode::TIMEOUT, msg, sub_code, msg2);
    }

    bool ok() const { return _state == nullptr; }
    bool is_cancelled() const { return code() == TStatusCode::CANCELLED; }
    bool is_mem_limit_exceeded() const { return code() == TStatusCode::MEM_LIMIT_EXCEEDED; }
    bool is_thrift_rpc_error() const { return code() == TStatusCode::THRIFT_RPC_ERROR; }

    // Convert into TStatus. Call this if 'status_container' contains an optional
    // TStatus field named 'status'. This also sets __isset.status.
    template <typename T>
    void set_t_status(T* status_container) const {
        to_thrift(&status_container->status);
        status_container->__isset.status = true;
    }

    // Convert into TStatus.
    void to_thrift(TStatus* status) const;
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
        return _state == nullptr ? TStatusCode::OK : static_cast<TStatusCode::type>(_state[4]);
    }

    int16_t precise_code() const {
        if (_state == nullptr) {
            return 0;
        }
        int16_t precise_code;
        memcpy(&precise_code, _state + 5, sizeof(precise_code));
        return precise_code;
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

private:
    const char* copy_state(const char* state);

    Status(TStatusCode::type code, const Slice& msg, int16_t sub_code, const Slice& msg2);

private:
    // OK status has a nullptr _state.  Otherwise, _state is a new[] array
    // of the following form:
    //    _state[0..3] == length of message
    //    _state[4]    == code
    //    _state[5..6] == precise_code
    //    _state[7..]  == message
    const char* _state;
};

// some generally useful macros
#define RETURN_IF_ERROR(stmt) \
    do { \
        Status _status_ = (stmt); \
        if (UNLIKELY(!_status_.ok())) { \
            return _status_; \
        } \
    } while (false)

#define RETURN_IF_STATUS_ERROR(status, stmt) \
    do { \
        status = (stmt); \
        if (UNLIKELY(!status.ok())) { \
            return; \
        } \
    } while (false)

#define EXIT_IF_ERROR(stmt) \
    do { \
        Status _status_ = (stmt); \
        if (UNLIKELY(!_status_.ok())) { \
            string msg = _status_.get_error_msg(); \
            LOG(ERROR) << msg;            \
            exit(1); \
        } \
    } while (false)

}

#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
