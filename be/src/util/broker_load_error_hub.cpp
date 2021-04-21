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

#include "util/broker_load_error_hub.h"

#include "exec/broker_writer.h"
#include "util/defer_op.h"

namespace doris {

BrokerLoadErrorHub::BrokerLoadErrorHub(ExecEnv* env, const TBrokerErrorHubInfo& info,
                                       const std::string& error_log_file_name)
        : _env(env), _info(info, error_log_file_name), _broker_writer(nullptr) {}

BrokerLoadErrorHub::~BrokerLoadErrorHub() {
    delete _broker_writer;
    _broker_writer = nullptr;
}

Status BrokerLoadErrorHub::prepare() {
    _broker_writer = new BrokerWriter(_env, _info.addrs, _info.props, _info.path, 0);

    RETURN_IF_ERROR(_broker_writer->open());

    _is_valid = true;
    return Status::OK();
}

Status BrokerLoadErrorHub::export_error(const ErrorMsg& error_msg) {
    std::lock_guard<std::mutex> lock(_mtx);
    ++_total_error_num;

    if (!_is_valid) {
        return Status::OK();
    }

    _error_msgs.push(error_msg);
    if (_error_msgs.size() >= EXPORTER_THRESHOLD) {
        RETURN_IF_ERROR(write_to_broker());
    }

    return Status::OK();
}

Status BrokerLoadErrorHub::close() {
    std::lock_guard<std::mutex> lock(_mtx);

    if (!_is_valid) {
        return Status::OK();
    }

    if (!_error_msgs.empty()) {
        write_to_broker();
    }

    // close anyway
    _broker_writer->close();

    _is_valid = false;
    return Status::OK();
}

Status BrokerLoadErrorHub::write_to_broker() {
    std::stringstream ss;
    while (!_error_msgs.empty()) {
        ss << _error_msgs.front().job_id << ": " << _error_msgs.front().msg << "\n";
        _error_msgs.pop();
    }

    const std::string& msg = ss.str();
    size_t written_len = 0;
    RETURN_IF_ERROR(_broker_writer->write((uint8_t*)msg.c_str(), msg.length(), &written_len));
    return Status::OK();
}

std::string BrokerLoadErrorHub::debug_string() const {
    std::stringstream out;
    out << "(total_error_num=" << _total_error_num << ")";
    return out.str();
}

} // end namespace doris
