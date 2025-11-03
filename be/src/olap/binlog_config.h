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

#pragma once

#include <cstdint>
#include <limits>
#include <string>

namespace doris {

class TBinlogConfig;
class BinlogConfigPB;

class BinlogConfig {
public:
    BinlogConfig() = default;
    BinlogConfig(bool enable, int64_t ttl_seconds, int64_t max_bytes, int64_t max_history_nums)
            : _enable(enable),
              _ttl_seconds(ttl_seconds),
              _max_bytes(max_bytes),
              _max_history_nums(max_history_nums) {}
    BinlogConfig(const BinlogConfig&) = default;
    BinlogConfig& operator=(const BinlogConfig&) = default;
    BinlogConfig(BinlogConfig&&) = default;
    BinlogConfig& operator=(BinlogConfig&&) = default;
    ~BinlogConfig() = default;

    bool is_enable() const { return _enable; }
    void set_enable(bool enable) { _enable = enable; }

    int64_t ttl_seconds() const { return _ttl_seconds; }
    void set_ttl_seconds(int64_t ttl_seconds) { _ttl_seconds = ttl_seconds; }

    int64_t max_bytes() const { return _max_bytes; }
    void set_max_bytes(int64_t max_bytes) { _max_bytes = max_bytes; }

    int64_t max_history_nums() const { return _max_history_nums; }
    void set_max_history_nums(int64_t max_history_nums) { _max_history_nums = max_history_nums; }

    BinlogConfig& operator=(const TBinlogConfig& config);
    BinlogConfig& operator=(const BinlogConfigPB& config);

    void to_pb(BinlogConfigPB* config_pb) const;
    std::string to_string() const;

private:
    bool _enable {false};
    int64_t _ttl_seconds {std::numeric_limits<int64_t>::max()};
    int64_t _max_bytes {std::numeric_limits<int64_t>::max()};
    int64_t _max_history_nums {std::numeric_limits<int64_t>::max()};
};

} // namespace doris
