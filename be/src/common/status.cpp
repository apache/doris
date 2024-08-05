// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/status.h"

#include <gen_cpp/Status_types.h>
#include <gen_cpp/types.pb.h> // for PStatus
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <new>
#include <vector>

#include "service/backend_options.h"

namespace doris {
namespace ErrorCode {

ErrorCodeState error_states[MAX_ERROR_CODE_DEFINE_NUM];
ErrorCodeInitializer error_code_init(10);
} // namespace ErrorCode

void Status::to_thrift(TStatus* s) const {
    s->error_msgs.clear();
    if (ok()) {
        s->status_code = TStatusCode::OK;
        return;
    }
    // Currently, there are many error codes, not defined in thrift and need pass to FE.
    // DCHECK(_code > 0)
    //        << "The error code has to > 0 because TStatusCode need it > 0, it's actual value is "
    //        << _code;
    s->status_code = (int16_t)_code > 0 ? (TStatusCode::type)_code : TStatusCode::INTERNAL_ERROR;
    s->error_msgs.push_back(fmt::format("({})[{}]{}", BackendOptions::get_localhost(),
                                        code_as_string(), _err_msg ? _err_msg->_msg : ""));
    s->__isset.error_msgs = true;
}

TStatus Status::to_thrift() const {
    TStatus s;
    to_thrift(&s);
    return s;
}

void Status::to_protobuf(PStatus* s) const {
    s->clear_error_msgs();
    s->set_status_code((int)_code);
    if (!ok()) {
        s->add_error_msgs(fmt::format("({})[{}]{}", BackendOptions::get_localhost(),
                                      code_as_string(), _err_msg ? _err_msg->_msg : ""));
    }
}

Status& Status::prepend(std::string_view msg) {
    if (!ok()) {
        if (_err_msg == nullptr) {
            _err_msg = std::make_unique<ErrMsg>();
        }
        _err_msg->_msg = std::string(msg) + _err_msg->_msg;
    }
    return *this;
}

Status& Status::append(std::string_view msg) {
    if (!ok()) {
        if (_err_msg == nullptr) {
            _err_msg = std::make_unique<ErrMsg>();
        }
        _err_msg->_msg.append(msg);
    }
    return *this;
}

std::string Status::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
    writer.StartObject();
    // status
    writer.Key("status");
    writer.String(code_as_string().c_str());
    // msg
    writer.Key("msg");
    ok() ? writer.String("OK") : writer.String(_err_msg ? _err_msg->_msg.c_str() : "");
    writer.EndObject();
    return s.GetString();
}

} // namespace doris
