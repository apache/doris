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

#include <glog/logging.h>

#include <atomic>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

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
//       return _init_once.is_called() && _init_once.stored_result().ok();
//     }
//   private:
//     Status _do_init() { /* init logic here */ }
//     DorisCallOnce<Status> _init_once;
//   };

template <typename R>
class DorisCallOnce {
public:
    DorisCallOnce() : _eptr(nullptr), _is_exception_thrown(false), _ret_val(std::nullopt) {}

    template <typename Fn>
        requires std::is_invocable_r_v<R, Fn>
    R call(Fn fn) {
        std::call_once(_once_flag, [&] {
            try {
                _ret_val = fn();
            } catch (...) {
                _eptr = std::current_exception();
                _is_exception_thrown.store(true, std::memory_order_relaxed);
            }
            // This memory order make sure both status and eptr is set
            // and will be seen in another thread.
            _is_called.store(true, std::memory_order_relaxed);
        });
        return _get_val();
    }

    bool is_called() const {
        // std::memory_order_acquire here and std::memory_order_release in
        // call(), taken together, mean that threads can safely synchronize on
        // _is_called().
        return _is_called.load(std::memory_order_acquire);
    }

    R stored_result() const {
        if (!is_called()) {
            // Could not return status if the method not called.
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "calling stored_result while has not been called");
        }
        return _get_val();
    }

private:
    R _get_val() const {
        DCHECK(is_called());
        if (_is_exception_thrown.load(std::memory_order_acquire)) {
            DCHECK(_eptr);
            std::rethrow_exception(_eptr);
        }
        DCHECK(_ret_val.has_value());
        return _ret_val.value();
    }

    std::once_flag _once_flag;
    std::exception_ptr _eptr;
    std::atomic<bool> _is_called;
    std::atomic<bool> _is_exception_thrown;
    std::optional<R> _ret_val;
};

} // namespace doris
