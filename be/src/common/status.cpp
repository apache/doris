// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/status.h"

#include <boost/stacktrace.hpp>
#include <glog/logging.h>

#include "gutil/strings/fastmem.h" // for memcpy_inlined
namespace doris {

constexpr int MAX_ERROR_NUM = 65536;
struct ErrorCodeState {
    int16_t error_code = 0;
    bool stacktrace = true;
    std::string description;
    size_t count = 0; // Used for count the number of error happens
    std::mutex mutex; // lock guard for count state
};
ErrorCodeState error_states[MAX_ERROR_NUM];
class Initializer {
public:
    Initializer() {
#define M(NAME, ERRORCODE, DESC, STACKTRACEENABLED) \
    error_states[abs(ERRORCODE)].stacktrace = STACKTRACEENABLED;
        APPLY_FOR_ERROR_CODES(M)
#undef M
// Currently, most of description is empty, so that we use NAME as description
#define M(NAME, ERRORCODE, DESC, STACKTRACEENABLED) \
    error_states[abs(ERRORCODE)].description = #NAME;
        APPLY_FOR_ERROR_CODES(M)
#undef M
    }
};
Initializer init; // Used to init the error_states array

Status::Status(const TStatus& s) {
    if (s.status_code != TStatusCode::OK) {
        // It is ok to set precise code == 1 here, because we do not know the precise code
        // just from thrift's TStatus
        if (s.error_msgs.empty()) {
            assemble_state(s.status_code, Slice(), 1, Slice());
        } else {
            assemble_state(s.status_code, s.error_msgs[0], 1, Slice());
        }
    } else {
        _length = 0;
    }
}

// TODO yiguolei, maybe should init PStatus's precise code because OLAPInternal Error may
// tranfer precise code during BRPC
Status::Status(const PStatus& s) {
    TStatusCode::type code = (TStatusCode::type)s.status_code();
    if (code != TStatusCode::OK) {
        // It is ok to set precise code == 1 here, because we do not know the precise code
        // just from thrift's TStatus
        if (s.error_msgs_size() == 0) {
            assemble_state(code, Slice(), 1, Slice());
        } else {
            assemble_state(code, s.error_msgs(0), 1, Slice());
        }
    } else {
        _length = 0;
    }
}

// Implement it here to remove the boost header file from status.h to reduce precompile time
Status Status::ConstructErrorStatus(int16_t precise_code, const Slice& msg) {
// This will print all error status's stack, it maybe too many, but it is just used for debug
#ifdef PRINT_ALL_ERR_STATUS_STACKTRACE
    LOG(WARNING) << "Error occurred, error code = " << precise_code << ", with message: " << msg
                 << "\n caused by:" << boost::stacktrace::stacktrace();
#endif
    if (error_states[abs(precise_code)].stacktrace) {
        // Add stacktrace as part of message, could use LOG(WARN) << "" << status will print both
        // the error message and the stacktrace
        return Status(TStatusCode::INTERNAL_ERROR, msg, precise_code,
                      boost::stacktrace::to_string(boost::stacktrace::stacktrace()));
    } else {
        return Status(TStatusCode::INTERNAL_ERROR, msg, precise_code, Slice());
    }
}

void Status::to_thrift(TStatus* s) const {
    s->error_msgs.clear();
    if (ok()) {
        s->status_code = TStatusCode::OK;
    } else {
        s->status_code = code();
        auto msg = message();
        s->error_msgs.emplace_back(msg.data, msg.size);
        s->__isset.error_msgs = true;
    }
}

TStatus Status::to_thrift() const {
    TStatus s;
    to_thrift(&s);
    return s;
}

void Status::to_protobuf(PStatus* s) const {
    s->clear_error_msgs();
    if (ok()) {
        s->set_status_code((int)TStatusCode::OK);
    } else {
        s->set_status_code(code());
        auto msg = message();
        s->add_error_msgs(msg.data, msg.size);
    }
}

std::string Status::code_as_string() const {
    switch (code()) {
    case TStatusCode::OK:
        return "OK";
    case TStatusCode::CANCELLED:
        return "Cancelled";
    case TStatusCode::NOT_IMPLEMENTED_ERROR:
        return "Not supported";
    case TStatusCode::RUNTIME_ERROR:
        return "Runtime error";
    case TStatusCode::MEM_LIMIT_EXCEEDED:
        return "Memory limit exceeded";
    case TStatusCode::INTERNAL_ERROR:
        return "Internal error";
    case TStatusCode::THRIFT_RPC_ERROR:
        return "Thrift rpc error";
    case TStatusCode::TIMEOUT:
        return "Timeout";
    case TStatusCode::MEM_ALLOC_FAILED:
        return "Memory alloc failed";
    case TStatusCode::BUFFER_ALLOCATION_FAILED:
        return "Buffer alloc failed";
    case TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE:
        return "Minimum reservation unavailable";
    case TStatusCode::PUBLISH_TIMEOUT:
        return "Publish timeout";
    case TStatusCode::LABEL_ALREADY_EXISTS:
        return "Label already exist";
    case TStatusCode::END_OF_FILE:
        return "End of file";
    case TStatusCode::NOT_FOUND:
        return "Not found";
    case TStatusCode::CORRUPTION:
        return "Corruption";
    case TStatusCode::INVALID_ARGUMENT:
        return "Invalid argument";
    case TStatusCode::IO_ERROR:
        return "IO error";
    case TStatusCode::ALREADY_EXIST:
        return "Already exist";
    case TStatusCode::NETWORK_ERROR:
        return "Network error";
    case TStatusCode::ILLEGAL_STATE:
        return "Illegal state";
    case TStatusCode::NOT_AUTHORIZED:
        return "Not authorized";
    case TStatusCode::REMOTE_ERROR:
        return "Remote error";
    case TStatusCode::SERVICE_UNAVAILABLE:
        return "Service unavailable";
    case TStatusCode::UNINITIALIZED:
        return "Uninitialized";
    case TStatusCode::CONFIGURATION_ERROR:
        return "Configuration error";
    case TStatusCode::INCOMPLETE:
        return "Incomplete";
    case TStatusCode::DATA_QUALITY_ERROR:
        return "Data quality error";
    default: {
        char tmp[30];
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
        return tmp;
    }
    }
    return std::string();
}

std::string Status::to_string() const {
    std::string result(code_as_string());
    if (ok()) {
        return result;
    }

    result.append(": ");
    Slice msg = message();
    result.append(reinterpret_cast<const char*>(msg.data), msg.size);
    int16_t posix = precise_code();
    if (posix != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), " (error %d)", posix);
        result.append(buf);
    }
    return result;
}

Slice Status::message() const {
    if (ok()) {
        return Slice();
    }

    return Slice(_state + HEADER_LEN, _length - HEADER_LEN);
}

Status Status::clone_and_prepend(const Slice& msg) const {
    if (ok()) {
        return *this;
    }
    return Status(code(), msg, precise_code(), message());
}

Status Status::clone_and_append(const Slice& msg) const {
    if (ok()) {
        return *this;
    }
    return Status(code(), message(), precise_code(), msg);
}

} // namespace doris