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

// Utility class for implementing thread-safe call-once semantics.
//
// call() will return stored result regardless of whether the first invocation
// returns a success status or not.
//
// Example:
//   class Resource {
//   public:
//     Status init() {
//       _init_once.call([this] { return _do_init(); });
//     }
//
//     bool is_inited() const {
//       return _init_once.has_called() && _init_once.stored_result().ok();
//     }
//   private:
//     Status _do_init() { /* init logic here */ }
//     DorisCallOnce<Status> _init_once;
//   };
template <typename ReturnType>
class DorisCallOnce {
public:
    DorisCallOnce() : _has_called(false) {}

    // If the underlying `once_flag` has yet to be invoked, invokes the provided
    // lambda and stores its return value. Otherwise, returns the stored Status.
    template <typename Fn>
    ReturnType call(Fn fn) {
        std::call_once(_once_flag, [this, fn] {
            _status = fn();
            _has_called.store(true, std::memory_order_release);
        });
        return _status;
    }

    // Return whether `call` has been invoked or not.
    bool has_called() const {
        // std::memory_order_acquire here and std::memory_order_release in
        // init(), taken together, mean that threads can safely synchronize on
        // _has_called.
        return _has_called.load(std::memory_order_acquire);
    }

    // Return the stored result. The result is only meaningful when `has_called() == true`.
    ReturnType stored_result() const { return _status; }

private:
    std::atomic<bool> _has_called;
    std::once_flag _once_flag;
    ReturnType _status;
};

} // namespace doris

#endif // DORIS_BE_SRC_UTIL_ONCE_H
