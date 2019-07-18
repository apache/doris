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

#ifndef DORIS_BE_SRC_UTIL_ONCE_H
#define DORIS_BE_SRC_UTIL_ONCE_H

#include <atomic>

#include "olap/olap_common.h"

namespace doris {

// Similar to the KuduOnceDynamic class, but accepts a lambda function.
class DorisInitOnce {
public:
    DorisInitOnce()
        : _init_succeeded(false) {}

    // If the underlying `once_flag` has yet to be invoked, invokes the provided
    // lambda and stores its return value. Otherwise, returns the stored Status.
    template<typename Fn>
    OLAPStatus init(Fn fn) {
        std::call_once(_once_flag, [this, fn] {
            _status = fn();
            if (OLAP_SUCCESS == _status) {
                _init_succeeded.store(true, std::memory_order_release);
            }
        });
        return _status;
    }

    // std::memory_order_acquire here and std::memory_order_release in
    // init(), taken together, mean that threads can safely synchronize on
    // _init_succeeded.
    bool init_succeeded() const {
        return _init_succeeded.load(std::memory_order_acquire);
    }

private:
    std::atomic<bool> _init_succeeded;
    std::once_flag _once_flag;
    OLAPStatus _status;
};

} // namespace doris

#endif // DORIS_BE_SRC_UTIL_ONCE_H
