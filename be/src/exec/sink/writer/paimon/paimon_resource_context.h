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

#include <memory>
#include <utility>

#include "runtime/thread_context.h"

namespace doris {

// Keep the same resource attribution on SDK/IO threads and during final buffer release.
// A null context is only used by isolated filesystem adapter tests.
template <typename F>
auto with_paimon_resource_context(const std::shared_ptr<ResourceContext>& context, F&& f)
        -> decltype(f()) {
    if (!context) return f();
    if (!pthread_context_ptr_init && bthread_self() == 0) {
        SCOPED_ATTACH_TASK(context);
        return f();
    }
    if (thread_context()->is_attach_task()) {
        SCOPED_SWITCH_RESOURCE_CONTEXT(context);
        return f();
    }
    SCOPED_ATTACH_TASK(context);
    return f();
}

} // namespace doris
