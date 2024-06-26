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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/once.h
// and modified by Doris

#pragma once

#include <atomic>
#include <mutex>
#include <stdexcept>

#include "common/exception.h"
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

    // this method is not exception safe, it will core when exception occurs in
    // callback method. I have tested the code https://en.cppreference.com/w/cpp/thread/call_once.
    // If the underlying `once_flag` has yet to be invoked, invokes the provided
    // lambda and stores its return value. Otherwise, returns the stored Status.
    // template <typename Fn>
    // ReturnType call(Fn fn) {
    //     std::call_once(_once_flag, [this, fn] {
    //         _status = fn();
    //         _has_called.store(true, std::memory_order_release);
    //     });
    //     return _status;
    // }

    // If exception occurs in the function, the call flag is set, if user call
    // it again, the same exception will be thrown.
    // It is different from std::call_once. This is because if a method is called once
    // some internal state is changed, it maybe not called again although exception
    // occurred.
    template <typename Fn>
    ReturnType call(Fn fn) {
        // Avoid lock to improve performance
        if (has_called()) {
            if (_eptr) {
                std::rethrow_exception(_eptr);
            }
            return _status;
        }
        std::lock_guard l(_flag_lock);
        // should check again because maybe another thread call successfully.
        if (has_called()) {
            if (_eptr) {
                std::rethrow_exception(_eptr);
            }
            return _status;
        }
        try {
            _status = fn();
        } catch (...) {
            // Save the exception for next call.
            _eptr = std::current_exception();
            _has_called.store(true, std::memory_order_release);
            std::rethrow_exception(_eptr);
        }
        // This memory order make sure both status and eptr is set
        // and will be seen in another thread.
        _has_called.store(true, std::memory_order_release);
        return _status;
    }

    // Has to pay attention to memory order
    // see https://en.cppreference.com/w/cpp/atomic/memory_order
    // Return whether `call` has been invoked or not.
    bool has_called() const {
        // std::memory_order_acquire here and std::memory_order_release in
        // init(), taken together, mean that threads can safely synchronize on
        // _has_called.
        return _has_called.load(std::memory_order_acquire);
    }

    // Return the stored result. The result is only meaningful when `has_called() == true`.
    ReturnType stored_result() const {
        if (!has_called()) {
            // Could not return status if the method not called.
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "calling stored_result while has not been called");
        }
        if (_eptr) {
            std::rethrow_exception(_eptr);
        }
        return _status;
    }

private:
    std::atomic<bool> _has_called;
    // std::once_flag _once_flag;
    std::mutex _flag_lock;
    std::exception_ptr _eptr;
    ReturnType _status;
};

} // namespace doris
