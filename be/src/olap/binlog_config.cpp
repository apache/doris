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

#include "olap/binlog_config.h"

#include <fmt/format.h>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"

namespace doris {
BinlogConfig& BinlogConfig::operator=(const TBinlogConfig& config) {
    if (config.__isset.enable) {
        _enable = config.enable;
    }
    if (config.__isset.ttl_seconds) {
        _ttl_seconds = config.ttl_seconds;
    }
    if (config.__isset.max_bytes) {
        _max_bytes = config.max_bytes;
    }
    if (config.__isset.max_history_nums) {
        _max_history_nums = config.max_history_nums;
    }
    return *this;
}

BinlogConfig& BinlogConfig::operator=(const BinlogConfigPB& config) {
    if (config.has_enable()) {
        _enable = config.enable();
    }
    if (config.has_ttl_seconds()) {
        _ttl_seconds = config.ttl_seconds();
    }
    if (config.has_max_bytes()) {
        _max_bytes = config.max_bytes();
    }
    if (config.has_max_history_nums()) {
        _max_history_nums = config.max_history_nums();
    }
    return *this;
}

void BinlogConfig::to_pb(BinlogConfigPB* config_pb) const {
    config_pb->set_enable(_enable);
    config_pb->set_ttl_seconds(_ttl_seconds);
    config_pb->set_max_bytes(_max_bytes);
    config_pb->set_max_history_nums(_max_history_nums);
}

std::string BinlogConfig::to_string() const {
    return fmt::format(
            "BinlogConfig enable: {}, ttl_seconds: {}, max_bytes: {}, max_history_nums: {}",
            _enable, _ttl_seconds, _max_bytes, _max_history_nums);
}

} // namespace doris
