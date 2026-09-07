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
#include <string>
#include <utility>
#include <vector>

namespace doris::segment_v2::gram {

// Boolean query tree over grams: ALL/NONE are short-circuit constants, while an AND/OR node may
// carry gram leaves directly (the grams field) as well as sub-queries (the subs field) -- both
// are operands of that node's operator. and_/or_ are the only entry points for building a
// non-trivial query, and they simplify while constructing (flattening, deduplication,
// absorption, NONE/ALL short-circuits, single-element collapse), so a GramQuery instance is in
// simplified form at all times. A canonical structural key supports deduplication, while a
// readable debug string supports EXPLAIN.
struct GramQuery {
    enum class Op : uint8_t { ALL, NONE, AND, OR };
    Op op = Op::ALL;
    std::vector<std::string> grams; // gram leaves held by this node (AND/OR only)
    std::vector<GramQuery> subs;    // sub-queries (AND/OR only)

    static GramQuery all() { return GramQuery {}; }
    static GramQuery none() {
        GramQuery q;
        q.op = Op::NONE;
        return q;
    }
    // A single gram is modelled as an AND node holding one gram, so it goes through the same
    // and_/or_ simplification logic.
    static GramQuery of_gram(std::string g) {
        GramQuery q;
        q.op = Op::AND;
        q.grams.push_back(std::move(g));
        return q;
    }
    // Build and simplify: flatten nested operators of the same kind, sort and deduplicate grams,
    // apply absorption (a child OR containing a gram already present in the AND is absorbed, and
    // a child AND containing a gram already present in the OR is absorbed), let a subset child
    // AND absorb its supersets inside an OR, deduplicate structurally, short-circuit NONE/ALL,
    // and collapse single-element nodes.
    static GramQuery and_(GramQuery a, GramQuery b);
    static GramQuery or_(GramQuery a, GramQuery b);
    bool is_all() const { return op == Op::ALL; }
    bool is_none() const { return op == Op::NONE; }
    // Total number of gram leaves, counted recursively over all sub-queries.
    size_t leaf_count() const;
    // Readable form for EXPLAIN, e.g. "(\"abc\" & (\"de\" | \"fg\"))".
    std::string to_debug_string() const;
    // Canonical string key used for structural deduplication: two queries with the same
    // structure (sub-query order does not matter, they are compared sorted) share the same key.
    std::string structural_key() const;
};

} // namespace doris::segment_v2::gram
