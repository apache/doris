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

#include <atomic>
#include <map>
#include <string>

namespace doris {

class HeartbeatFlagsHelper {
public:
    static const std::string SET_DEFAULT_ROWSET_TYPE_TO_BETA;

    HeartbeatFlagsHelper(uint64_t origin_flags) : _flags(origin_flags) {
        _valid_flags[SET_DEFAULT_ROWSET_TYPE_TO_BETA] = 1 << 0;
    }

    HeartbeatFlagsHelper() : HeartbeatFlagsHelper(0) { }

    void update(uint64_t flags) {
        _flags.store(flags, std::memory_order_relaxed);
    }

    uint64_t get() {
        return _flags.load(std::memory_order_relaxed);
    }

    bool is_bit_set(const std::string& flag_name) {
        uint64_t current_flags = get();
        if (_valid_flags.find(flag_name) == _valid_flags.end()) {
            return false;
        }
        return (current_flags & _valid_flags[flag_name]) != 0;
    }

private:
    std::map<std::string, uint64_t> _valid_flags;
    std::atomic<uint64_t> _flags;
};

}
