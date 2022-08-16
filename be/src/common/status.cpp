// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/status.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <boost/stacktrace.hpp>

#include "gen_cpp/types.pb.h" // for PStatus

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
    _code = s.status_code;
    if (_code != TStatusCode::OK) {
        // It is ok to set precise code == 1 here, because we do not know the precise code
        // just from thrift's TStatus
        _precise_code = 1;
        if (!s.error_msgs.empty()) {
            _err_msg = s.error_msgs[0];
        }
    }
}

// TODO yiguolei, maybe should init PStatus's precise code because OLAPInternal Error may
// tranfer precise code during BRPC
Status::Status(const PStatus& s) {
    _code = (TStatusCode::type)s.status_code();
    if (_code != TStatusCode::OK) {
        // It is ok to set precise code == 1 here, because we do not know the precise code
        // just from thrift's TStatus
        _precise_code = 1;
        if (s.error_msgs_size() > 0) {
            _err_msg = s.error_msgs(0);
        }
    }
}

// Implement it here to remove the boost header file from status.h to reduce precompile time
Status Status::ConstructErrorStatus(int16_t precise_code) {
// This will print all error status's stack, it maybe too many, but it is just used for debug
#ifdef PRINT_ALL_ERR_STATUS_STACKTRACE
    LOG(WARNING) << "Error occurred, error code = " << precise_code << ", with message: " << msg
                 << "\n caused by:" << boost::stacktrace::stacktrace();
#endif
    if (error_states[abs(precise_code)].stacktrace) {
        // Add stacktrace as part of message, could use LOG(WARN) << "" << status will print both
        // the error message and the stacktrace
        return Status(TStatusCode::INTERNAL_ERROR,
                      boost::stacktrace::to_string(boost::stacktrace::stacktrace()), precise_code);
    } else {
        return Status(TStatusCode::INTERNAL_ERROR, std::string_view(), precise_code);
    }
}

void Status::to_thrift(TStatus* s) const {
    s->error_msgs.clear();
    if (ok()) {
        s->status_code = TStatusCode::OK;
    } else {
        s->status_code = code();
        s->error_msgs.push_back(_err_msg);
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
        s->add_error_msgs(_err_msg);
    }
}

const char* Status::code_as_string() const {
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
        return "RPC error";
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
        return "Unknown code";
    }
    }
    return "Unknown code";
}

std::string Status::to_string() const {
    std::string result(code_as_string());
    if (ok()) {
        return result;
    }
    if (precise_code() != 1) {
        result.append(fmt::format("(error {})", precise_code()));
    }
    result.append(": ");
    result.append(_err_msg);
    return result;
}

Status& Status::prepend(std::string_view msg) {
    if (!ok()) {
        _err_msg = std::string(msg) + _err_msg;
    }
    return *this;
}

Status& Status::append(std::string_view msg) {
    if (!ok()) {
        _err_msg.append(msg);
    }
    return *this;
}

std::string Status::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // status
    writer.Key("status");
    writer.String(code_as_string());
    // msg
    writer.Key("msg");
    if (ok()) {
        writer.String("OK");
    } else {
        int16_t posix = precise_code();
        if (posix != 1) {
            char buf[64];
            snprintf(buf, sizeof(buf), " (error %d)", posix);
            writer.String((_err_msg + buf).c_str());
        } else {
            writer.String(_err_msg.c_str());
        }
    }
    writer.EndObject();
    return s.GetString();
}

} // namespace doris
