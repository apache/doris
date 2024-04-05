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

namespace doris {

class ProfileCollector {
public:
    void collect_profile_at_runtime() { _collect_profile_at_runtime(); }

    void collect_profile_before_close() {
        bool expected = false;
        if (_collected.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            _collect_profile_before_close();
        }
    }

    virtual ~ProfileCollector() {}

protected:
    virtual void _collect_profile_at_runtime() {}
    virtual void _collect_profile_before_close() {}

private:
    std::atomic<bool> _collected = false;
};

} // namespace doris
