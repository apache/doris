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

#include <bthread/bthread.h>

#include <functional>
#include <memory>

#include "runtime/thread_context.h"

namespace doris {

// Launch a callable in a background bthread (fire-and-forget).
// The callable is heap-allocated and deleted after execution.
// Returns true on success, false if bthread creation failed (callable is still deleted).
//
// init_thread_ctx: when true, initialises the bthread's thread-local ThreadContext via
// ScopedInitThreadContext before invoking fn.  Set this when the bthread needs memory-tracker
// or workload-group context (e.g. attaches an AttachTask inside fn).  Defaults to false so
// existing call-sites that do not need thread-context initialisation are unaffected.
template <typename Fn>
bool start_bthread(Fn&& fn, bool init_thread_ctx = false, const bthread_attr_t* attr = nullptr) {
    struct Args {
        std::function<void()> fn;
        bool init_thread_ctx;
    };
    auto args = std::make_unique<Args>(Args {std::forward<Fn>(fn), init_thread_ctx});
    auto entry = [](void* arg) -> void* {
        // Reclaim ownership so Args is deleted when this scope exits.
        auto a = std::unique_ptr<Args>(reinterpret_cast<Args*>(arg));
        std::optional<ScopedInitThreadContext> scoped;
        if (a->init_thread_ctx) {
            scoped.emplace();
        }
        a->fn();
        return nullptr;
    };
    bthread_t tid;
    if (bthread_start_background(&tid, attr, entry, args.get()) != 0) {
        return false; // args unique_ptr destructs here, no leak
    }
    args.release(); // bthread entry now owns the pointer
    return true;
}

} // namespace doris
