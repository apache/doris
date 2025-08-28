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

#include <variant>

#include "common/exception.h"
#include "mock_iterator.h"
#include "union_term_iterator.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

using DISI = std::variant<TermPositionsIterPtr, UnionTermIterPtr, MockIterPtr>;

template <typename DISIType, typename Func, typename... Args>
auto visit_node(DISIType&& disi, Func&& func, Args&&... args) {
    return std::visit(
            [&](const auto& iter) {
                return std::forward<Func>(func)(iter, std::forward<Args>(args)...);
            },
            disi);
}

struct DocID {
    template <typename T>
    int32_t operator()(const T& iter) const {
        return iter->doc_id();
    }
};

struct Freq {
    template <typename T>
    int32_t operator()(const T& iter) const {
        return iter->freq();
    }
};

struct NextDoc {
    template <typename T>
    int32_t operator()(const T& iter) const {
        return iter->next_doc();
    }
};

struct Advance {
    template <typename T>
    int32_t operator()(const T& iter, int32_t target) const {
        return iter->advance(target);
    }
};

struct DocFreq {
    template <typename T>
    int32_t operator()(const T& iter) const {
        return iter->doc_freq();
    }
};

struct NextPosition {
    template <typename T>
    int32_t operator()(const T& iter) const {
        return iter->next_position();
    }
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
