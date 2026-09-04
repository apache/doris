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

#include "storage/index/inverted/gram/gram_query.h"

#include <algorithm>
#include <set>

#include "util/url_coding.h"

namespace doris::segment_v2::gram {

// The BE storage target enables CMake unity builds (several .cpp files are compiled together,
// see UNITY_BUILD_BATCH_SIZE in be/src/storage/CMakeLists.txt), so every anonymous namespace in
// a batch is merged into one translation unit. A bare anonymous namespace then redefines any
// symbol whose name another file of the same batch happens to reuse (even in a different .cpp),
// and the batching changes as files are added to or removed from the directory, so "this batch
// only holds these files" cannot be assumed for long. Hence the extra named namespace private
// to this file, which isolates this file's anonymous namespace; the symbols inside it still
// have internal linkage (anonymous-namespace semantics are unaffected by a named enclosing
// namespace).
namespace gram_query_detail {

namespace {

// Deduplicate grams and sort them lexicographically: the canonical form of an AND/OR node's
// grams field.
void dedupe_grams(std::vector<std::string>& v) {
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

// Deduplicate sub-queries by structural_key() (i.e. their serialize() text), keeping the first
// occurrence.
void dedupe_subs(std::vector<GramQuery>& subs) {
    std::set<std::string> seen;
    std::vector<GramQuery> keep;
    for (auto& s : subs) {
        if (seen.insert(s.structural_key()).second) {
            keep.push_back(std::move(s));
        }
    }
    subs = std::move(keep);
}

// Whether the (already sorted) vector contains g.
bool has_gram(const std::vector<std::string>& sorted, const std::string& g) {
    return std::binary_search(sorted.begin(), sorted.end(), g);
}

// Inside an OR node: if the gram set of one "pure gram set" child AND (one with no sub-queries)
// is a subset of another such child AND's, the latter is implied by the former (satisfying the
// stricter AND necessarily satisfies the looser one), so under OR semantics the stricter branch
// is redundant and should be dropped. Indices to drop are first marked in drop[] and only moved
// out once all comparisons are done, so that an object still needed by later comparisons is
// never moved away mid-loop.
void or_absorb_subsets(std::vector<GramQuery>& subs) {
    std::vector<char> drop(subs.size(), 0);
    for (size_t i = 0; i < subs.size(); i++) {
        if (subs[i].op != GramQuery::Op::AND || !subs[i].subs.empty()) {
            continue;
        }
        for (size_t j = 0; j < subs.size() && !drop[i]; j++) {
            if (i == j || subs[j].op != GramQuery::Op::AND || !subs[j].subs.empty()) {
                continue;
            }
            // A side with more grams cannot be a subset of the other, so skip it; when the
            // counts are equal, compare only once (when j < i) so that two equal-sized identical
            // sets are not both dropped for being subsets of each other (normally dedupe_subs
            // has already removed such duplicates; this is only defensive).
            if (subs[j].grams.size() > subs[i].grams.size() ||
                (subs[j].grams.size() == subs[i].grams.size() && j > i)) {
                continue;
            }
            if (std::includes(subs[i].grams.begin(), subs[i].grams.end(), subs[j].grams.begin(),
                              subs[j].grams.end())) {
                drop[i] = 1;
            }
        }
    }
    std::vector<GramQuery> keep;
    for (size_t i = 0; i < subs.size(); i++) {
        if (!drop[i]) {
            keep.push_back(std::move(subs[i]));
        }
    }
    subs = std::move(keep);
}

} // namespace

} // namespace gram_query_detail

GramQuery GramQuery::and_(GramQuery a, GramQuery b) {
    if (a.is_none() || b.is_none()) {
        return none();
    }
    if (a.is_all()) {
        return b;
    }
    if (b.is_all()) {
        return a;
    }
    GramQuery r;
    r.op = Op::AND;
    // Flatten: when an operand is itself an AND, merge its grams/subs into r; otherwise keep it
    // whole as a sub-query.
    for (GramQuery* x : {&a, &b}) {
        if (x->op == Op::AND) {
            r.grams.insert(r.grams.end(), x->grams.begin(), x->grams.end());
            for (auto& s : x->subs) {
                r.subs.push_back(std::move(s));
            }
        } else {
            r.subs.push_back(std::move(*x));
        }
    }
    gram_query_detail::dedupe_grams(r.grams);
    // Absorption: if the AND already holds gram g, a child OR containing g is always true (an OR
    // only needs one of its branches), so it no longer constrains the AND and can be dropped
    // entirely.
    std::vector<GramQuery> keep;
    for (auto& s : r.subs) {
        bool absorbed = false;
        if (s.op == Op::OR) {
            for (const auto& g : s.grams) {
                if (gram_query_detail::has_gram(r.grams, g)) {
                    absorbed = true;
                    break;
                }
            }
        }
        if (!absorbed) {
            keep.push_back(std::move(s));
        }
    }
    r.subs = std::move(keep);
    gram_query_detail::dedupe_subs(r.subs);
    // Single-element collapse: an AND with no gram of its own and a single sub-query is
    // equivalent to that sub-query.
    if (r.grams.empty() && r.subs.size() == 1) {
        return std::move(r.subs[0]);
    }
    return r;
}

GramQuery GramQuery::or_(GramQuery a, GramQuery b) {
    if (a.is_all() || b.is_all()) {
        return all();
    }
    if (a.is_none()) {
        return b;
    }
    if (b.is_none()) {
        return a;
    }
    GramQuery r;
    r.op = Op::OR;
    // Flatten: when an operand is an OR, merge its grams/subs; an AND holding a single gram (as
    // produced by of_gram) is demoted to a gram leaf of this node; anything else is kept whole
    // as a sub-query.
    for (GramQuery* x : {&a, &b}) {
        if (x->op == Op::OR) {
            r.grams.insert(r.grams.end(), x->grams.begin(), x->grams.end());
            for (auto& s : x->subs) {
                r.subs.push_back(std::move(s));
            }
        } else if (x->op == Op::AND && x->grams.size() == 1 && x->subs.empty()) {
            r.grams.push_back(x->grams[0]);
        } else {
            r.subs.push_back(std::move(*x));
        }
    }
    gram_query_detail::dedupe_grams(r.grams);
    // Absorption: if the OR already holds gram g, a child AND containing g is implied by g (an
    // AND requires all of its conditions, and one of them is already satisfied by another branch
    // of the OR), so it no longer constrains the OR and can be dropped.
    std::vector<GramQuery> keep;
    for (auto& s : r.subs) {
        bool absorbed = false;
        if (s.op == Op::AND) {
            for (const auto& g : s.grams) {
                if (gram_query_detail::has_gram(r.grams, g)) {
                    absorbed = true;
                    break;
                }
            }
        }
        if (!absorbed) {
            keep.push_back(std::move(s));
        }
    }
    r.subs = std::move(keep);
    gram_query_detail::dedupe_subs(r.subs);
    gram_query_detail::or_absorb_subsets(r.subs);
    // Single-element collapse.
    if (r.grams.size() == 1 && r.subs.empty()) {
        return of_gram(r.grams[0]);
    }
    if (r.grams.empty() && r.subs.size() == 1) {
        return std::move(r.subs[0]);
    }
    return r;
}

size_t GramQuery::leaf_count() const {
    size_t c = grams.size();
    for (const auto& s : subs) {
        c += s.leaf_count();
    }
    return c;
}

std::string GramQuery::structural_key() const {
    return serialize();
}

std::string GramQuery::serialize() const {
    if (op == Op::ALL) {
        return "*";
    }
    if (op == Op::NONE) {
        return "!";
    }
    std::string s = op == Op::AND ? "&(" : "|(";
    bool first = true;
    for (const auto& g : grams) {
        if (!first) {
            s += ',';
        }
        first = false;
        std::string enc;
        doris::base64_encode(g, &enc);
        s += enc;
    }
    // Sub-queries are emitted sorted by their own serialize() text, so identical structures
    // always produce identical text.
    std::vector<std::string> ks;
    for (const auto& c : subs) {
        ks.push_back(c.serialize());
    }
    std::sort(ks.begin(), ks.end());
    for (const auto& k : ks) {
        if (!first) {
            s += ',';
        }
        first = false;
        s += k;
    }
    return s + ")";
}

namespace gram_query_detail {

namespace {

// Maximum nesting depth allowed for AND/OR (the top-level call is 1). Anything deeper is
// rejected at once, so that malformed or malicious input such as a repeated "&(" cannot recurse
// deep enough to blow the stack (this repository has seen such an overflow: CIR-21633).
constexpr int kMaxNestingDepth = 64;

// Parse one base64 gram token from an AND/OR item list (the case where t[i] is none of &, |, *,
// ! and parse_at has classified it as an ordinary item): scan from t[i] to the next ',' or ')'
// as the token boundary, base64-decode it and wrap it with GramQuery::of_gram; on success i
// points just past the token (the separator itself is not consumed). Split out of parse_at to
// reduce its complexity; the semantics are identical to the original inline else branch.
Status parse_gram_token(std::string_view t, size_t& i, GramQuery* out) {
    size_t j = i;
    while (j < t.size() && t[j] != ',' && t[j] != ')') {
        j++;
    }
    if (j == i) {
        return Status::InvalidArgument("gram query empty item at {}", i);
    }
    std::string dec;
    if (!doris::base64_decode(std::string(t.substr(i, j - i)), &dec)) {
        return Status::InvalidArgument("gram query bad base64 at {}", i);
    }
    if (dec.empty()) {
        return Status::InvalidArgument("gram query empty gram at {}", i);
    }
    *out = GramQuery::of_gram(std::move(dec));
    i = j;
    return Status::OK();
}

// Parse one GramQuery starting at t[i]; on success i points just past that query. depth is the
// current nesting depth (1 for the top-level call) and bounds the recursion to avoid a stack
// overflow.
//
// Every item of an AND/OR node is folded into an accumulator through the GramQuery::and_/or_
// combinators (starting from all() for AND and none() for OR) instead of being appended to the
// grams/subs fields directly: that way sorting, deduplication, absorption, ALL/NONE
// short-circuits and single-element collapse hold automatically. Those invariants are what make
// has_gram() (binary search) and or_absorb_subsets() (std::includes) correct -- any tree parsed
// from text must satisfy them before it can safely take part in further and_/or_ calls;
// assembling the fields directly would produce trees the invariants forbid and make later
// operations fail silently.
// The syntax is also checked strictly, rejecting looser spellings that serialize() never emits:
// an empty item (consecutive, leading or trailing comma), an AND/OR with no operand (such as
// "&()"), and a gram that decodes to an empty string.
Status parse_at(std::string_view t, size_t& i, int depth, GramQuery* out) {
    if (depth > kMaxNestingDepth) {
        return Status::InvalidArgument("gram query nesting too deep");
    }
    if (i >= t.size()) {
        return Status::InvalidArgument("gram query truncated");
    }
    if (t[i] == '*') {
        i++;
        *out = GramQuery::all();
        return Status::OK();
    }
    if (t[i] == '!') {
        i++;
        *out = GramQuery::none();
        return Status::OK();
    }
    if ((t[i] != '&' && t[i] != '|') || i + 1 >= t.size() || t[i + 1] != '(') {
        return Status::InvalidArgument("gram query bad token at {}", i);
    }
    bool is_and = t[i] == '&';
    i += 2;
    GramQuery acc = is_and ? GramQuery::all() : GramQuery::none();
    size_t count = 0;
    while (true) {
        if (i >= t.size()) {
            return Status::InvalidArgument("gram query truncated");
        }
        if (t[i] == ')') {
            break;
        }
        GramQuery item;
        if (t[i] == '&' || t[i] == '|' || t[i] == '*' || t[i] == '!') {
            RETURN_IF_ERROR(parse_at(t, i, depth + 1, &item));
        } else {
            RETURN_IF_ERROR(parse_gram_token(t, i, &item));
        }
        count++;
        acc = is_and ? GramQuery::and_(std::move(acc), std::move(item))
                     : GramQuery::or_(std::move(acc), std::move(item));
        if (i < t.size() && t[i] == ',') {
            i++;
            if (i < t.size() && t[i] == ')') {
                return Status::InvalidArgument("gram query trailing comma at {}", i);
            }
            continue;
        }
        break;
    }
    if (i >= t.size() || t[i] != ')') {
        return Status::InvalidArgument("gram query missing ')'");
    }
    i++;
    if (count == 0) {
        return Status::InvalidArgument("gram query empty {} group", is_and ? "AND" : "OR");
    }
    *out = std::move(acc);
    return Status::OK();
}

} // namespace

} // namespace gram_query_detail

Status GramQuery::parse(std::string_view text, GramQuery* out) {
    size_t i = 0;
    // Parse into a local first: *out is written only when the whole parse succeeds (with no
    // trailing input), so a caller never observes a half-built tree on failure (the old
    // implementation, for instance, had already written the top-level token's partial result
    // into *out in the trailing-input case).
    GramQuery local;
    RETURN_IF_ERROR(gram_query_detail::parse_at(text, i, /*depth=*/1, &local));
    if (i != text.size()) {
        return Status::InvalidArgument("gram query trailing input");
    }
    *out = std::move(local);
    return Status::OK();
}

std::string GramQuery::to_debug_string() const {
    if (op == Op::ALL) {
        return "ALL";
    }
    if (op == Op::NONE) {
        return "NONE";
    }
    std::string sep = op == Op::AND ? " & " : " | ";
    std::string s = "(";
    bool first = true;
    for (const auto& g : grams) {
        if (!first) {
            s += sep;
        }
        first = false;
        s += "\"" + g + "\"";
    }
    for (const auto& c : subs) {
        if (!first) {
            s += sep;
        }
        first = false;
        s += c.to_debug_string();
    }
    return s + ")";
}

} // namespace doris::segment_v2::gram
