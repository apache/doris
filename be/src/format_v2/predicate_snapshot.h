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
#include <mutex>
#include <optional>
#include <utility>

#include "exprs/vexpr_fwd.h"

namespace doris::format {

// The scanner replaces this snapshot when its ready runtime-filter set changes. Splits keep
// independent executable conjuncts, while digest consumers share this immutable semantic key.
class PredicateSnapshot {
public:
    explicit PredicateSnapshot(VExprContextSPtrs conjuncts) : _conjuncts(std::move(conjuncts)) {}

    std::optional<uint64_t> digest() const;

private:
    const VExprContextSPtrs _conjuncts;
    mutable std::once_flag _digest_once;
    mutable std::optional<uint64_t> _digest;
};

} // namespace doris::format
