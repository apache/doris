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

#include "common/status.h"

#include <boost/algorithm/string/join.hpp>

#include "common/logging.h"

namespace doris {

// NOTE: this is statically initialized and we must be very careful what
// functions these constructors call.  In particular, we cannot call
// glog functions which also rely on static initializations.
// TODO: is there a more controlled way to do this.
const Status Status::OK;
const Status Status::CANCELLED(TStatusCode::CANCELLED, "Cancelled", true);
const Status Status::MEM_LIMIT_EXCEEDED(
    TStatusCode::MEM_LIMIT_EXCEEDED, "Memory limit exceeded", true);
const Status Status::THRIFT_RPC_ERROR(
    TStatusCode::THRIFT_RPC_ERROR, "Thrift RPC failed", true);

const Status Status::TIMEOUT(
    TStatusCode::TIMEOUT, "timeout", true);

Status::ErrorDetail::ErrorDetail(const TStatus& status) : 
        error_code(status.status_code),
        error_msgs(status.error_msgs) {
    DCHECK_NE(error_code, TStatusCode::OK);
}

Status::ErrorDetail::ErrorDetail(const PStatus& pstatus)
        : error_code((TStatusCode::type)pstatus.status_code()) {
    DCHECK_NE(error_code, TStatusCode::OK);
    for (auto& msg : pstatus.error_msgs()) {
        error_msgs.push_back(msg);
    }
}

Status::Status(const std::string& error_msg) : 
        _error_detail(new ErrorDetail(TStatusCode::INTERNAL_ERROR, error_msg)) {
    LOG(INFO) << error_msg << std::endl << get_stack_trace();
}

Status::Status(TStatusCode::type code, const std::string& error_msg)
    : _error_detail(new ErrorDetail(code, error_msg)) {
}

Status::Status(const std::string& error_msg, bool quiet) : 
        _error_detail(new ErrorDetail(TStatusCode::INTERNAL_ERROR, error_msg)) {
    if (!quiet) {
        LOG(INFO) << error_msg << std::endl << get_stack_trace();
    }
}

Status::Status(const TStatus& status) : 
        _error_detail(status.status_code == TStatusCode::OK ? NULL : new ErrorDetail(status)) {
}

Status& Status::operator=(const TStatus& status) {
    delete _error_detail;

    if (status.status_code == TStatusCode::OK) {
        _error_detail = NULL;
    } else {
        _error_detail = new ErrorDetail(status);
    }

    return *this;
}

Status::Status(const PStatus& pstatus) :
    _error_detail((TStatusCode::type)pstatus.status_code() == TStatusCode::OK
                  ? nullptr : new ErrorDetail(pstatus)) {
}

Status& Status::operator=(const PStatus& status) {
    delete _error_detail;
    if (status.status_code() == (TStatusCode::type)TStatusCode::OK) {
        _error_detail = nullptr;
    } else {
        _error_detail = new ErrorDetail(status);
    }
    return *this;
}

void Status::add_error_msg(TStatusCode::type code, const std::string& msg) {
    if (_error_detail == NULL) {
        _error_detail = new ErrorDetail(code, msg);
    } else {
        _error_detail->error_msgs.push_back(msg);
    }

    VLOG(2) << msg;
}

void Status::add_error_msg(const std::string& msg) {
    add_error_msg(TStatusCode::INTERNAL_ERROR, msg);
}

void Status::add_error(const Status& status) {
    if (status.ok()) {
        return;
    }

    add_error_msg(status.code(), status.get_error_msg());
}

void Status::get_error_msgs(std::vector<std::string>* msgs) const {
    msgs->clear();

    if (_error_detail != NULL) {
        *msgs = _error_detail->error_msgs;
    }
}

void Status::get_error_msg(std::string* msg) const {
    msg->clear();

    if (_error_detail != NULL) {
        *msg = boost::join(_error_detail->error_msgs, "\n");
    }
}

std::string Status::get_error_msg() const {
    std::string msg;
    get_error_msg(&msg);
    return msg;
}

void Status::to_thrift(TStatus* status) const {
    status->error_msgs.clear();

    if (_error_detail == NULL) {
        status->status_code = TStatusCode::OK;
    } else {
        status->status_code = _error_detail->error_code;

        for (int i = 0; i < _error_detail->error_msgs.size(); ++i) {
            status->error_msgs.push_back(_error_detail->error_msgs[i]);
        }

        status->__isset.error_msgs = !_error_detail->error_msgs.empty();
    }
}

void Status::to_protobuf(PStatus* pstatus) const {
    pstatus->clear_error_msgs();
    if (_error_detail == nullptr) {
        pstatus->set_status_code((int)TStatusCode::OK);
    } else {
        pstatus->set_status_code(_error_detail->error_code);
        pstatus->mutable_error_msgs()->Reserve(_error_detail->error_msgs.size());
        for (auto& err_msg : _error_detail->error_msgs) {
            pstatus->add_error_msgs(err_msg);
        }
    }
}

void Status::MergeStatus(const Status& status) {
  if (status.ok()) return;
  if (_error_detail == NULL) {
    _error_detail = new ErrorDetail(status.code());
  } else {
    std::vector<std::string> msgs_vector;
    status.get_error_msgs(&msgs_vector);
    for (const std::string& s: msgs_vector) add_error_msg(s);
  }
}
}
