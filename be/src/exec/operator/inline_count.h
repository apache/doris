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

#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

// A grouped aggregation whose only function is COUNT(*) (or a COUNT whose argument
// is never NULL) keeps the counter directly in the AggregateDataPtr slot of the
// hash map instead of allocating an aggregate state ("inline count").
//
// The slot is a pointer object. It is only ever read and written by converting the
// pointer *value* to and from an integer: accessing the pointer object through a
// UInt64 lvalue would break the strict-aliasing rules the BE is compiled with
// (-fstrict-aliasing), and the optimizer could then keep a stale pointer value in
// a register across such an update.

inline UInt64 inline_count_get(AggregateDataPtr mapped) {
    return static_cast<UInt64>(reinterpret_cast<uintptr_t>(mapped));
}

inline void inline_count_add(AggregateDataPtr& mapped, UInt64 delta) {
    mapped = reinterpret_cast<AggregateDataPtr>(
            static_cast<uintptr_t>(inline_count_get(mapped) + delta));
}

} // namespace doris
