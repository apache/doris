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
#include <memory>
#include <roaring/roaring.hh>
#include <variant>

#include "common/status.h"

namespace doris::segment_v2::idx_query_v2 {

class ConjunctionOp;
class DisjunctionOp;
class BooleanQuery;
class TermQuery;
class RoaringQuery;

using ConjunctionOpPtr = std::shared_ptr<ConjunctionOp>;
using DisjunctionOpPtr = std::shared_ptr<DisjunctionOp>;
using TermQueryPtr = std::shared_ptr<TermQuery>;
using RoaringQueryPtr = std::shared_ptr<RoaringQuery>;
using BooleanQueryPtr = std::shared_ptr<BooleanQuery>;

using Node = std::variant<ConjunctionOpPtr, DisjunctionOpPtr, BooleanQueryPtr, TermQueryPtr,
                          RoaringQueryPtr>;

template <typename T>
constexpr bool always_false = false;

template <typename NodeType, typename Func, typename... Args>
auto visit_node(NodeType&& node, Func&& func, Args&&... args) {
    using DecayedNodeType = std::remove_const_t<std::decay_t<NodeType>>;
    using NodeBaseType = std::remove_pointer_t<DecayedNodeType>;

    static_assert(!std::is_pointer_v<DecayedNodeType>, "NodeType must not be a pointer");
    static_assert(std::is_same_v<std::remove_const_t<NodeBaseType>, Node>, "NodeType must be Node");

    return std::visit(
            [&](const auto& opOrClause) {
                return std::forward<Func>(func)(opOrClause, std::forward<Args>(args)...);
            },
            node);
}

template <typename T>
concept IsOpPtr = std::is_same_v<T, ConjunctionOpPtr> || std::is_same_v<T, DisjunctionOpPtr>;

template <typename T>
concept IsQueryPtr = std::is_same_v<T, BooleanQueryPtr> || std::is_same_v<T, TermQueryPtr> ||
                     std::is_same_v<T, RoaringQueryPtr>;

struct OpAddChild {
    template <typename T>
    Status operator()(const T& op, const Node& clause) const {
        if constexpr (IsOpPtr<T>) {
            return op->add_child(clause);
        } else {
            return Status::OK();
        }
    }
};

struct OpInit {
    template <typename T>
    Status operator()(const T& op) const {
        if constexpr (IsOpPtr<T>) {
            return op->init();
        } else {
            return Status::OK();
        }
    }
};

struct QueryExecute {
    template <typename T>
    void operator()(const T& query, const std::shared_ptr<roaring::Roaring>& result) const {
        if constexpr (IsQueryPtr<T>) {
            return query->execute(result);
        }
    }
};

struct DocId {
    template <typename T>
    int32_t operator()(const T& node) const {
        return node->doc_id();
    }
};

struct NextDoc {
    template <typename T>
    int32_t operator()(const T& node) const {
        return node->next_doc();
    }
};

struct Advance {
    template <typename T>
    int32_t operator()(const T& node, int32_t target) const {
        return node->advance(target);
    }
};

struct Cost {
    template <typename T>
    int64_t operator()(const T& node) const {
        return node->cost();
    }
};

} // namespace doris::segment_v2::idx_query_v2