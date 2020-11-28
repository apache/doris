// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/status.h"

#include "gutil/strings/fastmem.h" // for memcpy_inlined

namespace doris {

inline const char* assemble_state(TStatusCode::type code, const Slice& msg, int16_t precise_code,
                                  const Slice& msg2) {
    DCHECK(code != TStatusCode::OK);

    const uint32_t len1 = msg.size;
    const uint32_t len2 = msg2.size;
    const uint32_t size = len1 + ((len2 > 0) ? (2 + len2) : 0);
    auto result = new char[size + 7];
    memcpy(result, &size, sizeof(size));
    result[4] = static_cast<char>(code);
    memcpy(result + 5, &precise_code, sizeof(precise_code));
    memcpy(result + 7, msg.data, len1);
    if (len2 > 0) {
        result[7 + len1] = ':';
        result[8 + len1] = ' ';
        memcpy(result + 9 + len1, msg2.data, len2);
    }
    return result;
}

const char* Status::copy_state(const char* state) {
    uint32_t size;
    strings::memcpy_inlined(&size, state, sizeof(size));
    auto result = new char[size + 7];
    strings::memcpy_inlined(result, state, size + 7);
    return result;
}

Status::Status(const TStatus& s) : _state(nullptr) {
    if (s.status_code != TStatusCode::OK) {
        if (s.error_msgs.empty()) {
            _state = assemble_state(s.status_code, Slice(), 1, Slice());
        } else {
            _state = assemble_state(s.status_code, s.error_msgs[0], 1, Slice());
        }
    }
}

Status::Status(const PStatus& s) : _state(nullptr) {
    TStatusCode::type code = (TStatusCode::type)s.status_code();
    if (code != TStatusCode::OK) {
        if (s.error_msgs_size() == 0) {
            _state = assemble_state(code, Slice(), 1, Slice());
        } else {
            _state = assemble_state(code, s.error_msgs(0), 1, Slice());
        }
    }
}

Status::Status(TStatusCode::type code, const Slice& msg, int16_t precise_code, const Slice& msg2)
        : _state(assemble_state(code, msg, precise_code, msg2)) {}

void Status::to_thrift(TStatus* s) const {
    s->error_msgs.clear();
    if (_state == nullptr) {
        s->status_code = TStatusCode::OK;
    } else {
        s->status_code = code();
        auto msg = message();
        s->error_msgs.emplace_back(msg.data, msg.size);
        s->__isset.error_msgs = true;
    }
}

void Status::to_protobuf(PStatus* s) const {
    s->clear_error_msgs();
    if (_state == nullptr) {
        s->set_status_code((int)TStatusCode::OK);
    } else {
        s->set_status_code(code());
        auto msg = message();
        s->add_error_msgs(msg.data, msg.size);
    }
}

std::string Status::code_as_string() const {
    if (_state == nullptr) {
        return "OK";
    }
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
    if (_state == nullptr) {
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
    if (_state == nullptr) {
        return Slice();
    }

    uint32_t length;
    memcpy(&length, _state, sizeof(length));
    return Slice(_state + 7, length);
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
