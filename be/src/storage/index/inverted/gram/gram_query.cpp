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

// BE 的 Storage 目标开启了 CMake Unity Build（多个 .cpp 合并编译，见
// be/src/storage/CMakeLists.txt 的 UNITY_BUILD_BATCH_SIZE），同一批次内所有
// 文件的匿名命名空间会被合并进同一个翻译单元，裸的匿名命名空间一旦与批次内
// 其他文件重名（哪怕在不同 .cpp 里）就会重定义报错，且批次分组会随目录下
// 文件增删而变化，无法长期假设「这批次只有这几个文件」。因此这里额外套一层
// 本文件专属的具名命名空间，隔离本文件的匿名命名空间，不影响其中各符号本身
// 仍是内部链接（匿名命名空间语义不受具名外层嵌套影响）。
namespace gram_query_detail {

namespace {

// gram 去重并按字典序排序，作为 AND/OR 节点 grams 字段的规范形态。
void dedupe_grams(std::vector<std::string>& v) {
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

// 子查询按 structural_key()（即 serialize() 文本）去重，保留首次出现的那个。
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

// sorted（已排序）是否包含 g。
bool has_gram(const std::vector<std::string>& sorted, const std::string& g) {
    return std::binary_search(sorted.begin(), sorted.end(), g);
}

// OR 节点内：若某个「纯 gram 集」子 AND（不含子查询）的 gram 集是另一个同类子 AND
// 的子集，则后者被前者蕴含（满足更严格的 AND 必然满足更宽松的 AND），OR 语义下
// 更严格的那个是多余分支，应删除。先在 drop[] 中统一标记要删除的下标，比较全部
// 结束后再一次性搬移，避免遍历中把仍需参与后续比较的对象 move 走。
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
            // gram 数更多的一方不可能是另一方的子集，跳过；数量相等时只在 j<i
            // 时才比较一次，避免等长的重复集合互相判定为对方子集而被同时删除
            // （正常情况下等长的重复集合已被 dedupe_subs 提前去重，这里仅作
            // 防御性处理）。
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
    // 扁平化：操作数本身是 AND 时把它的 grams/subs 并入 r；否则整体作为子查询。
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
    // 吸收律：AND 内已有 gram g，则含 g 的子 OR 恒真（OR 只需其中一支满足），
    // 对整个 AND 不再构成约束，可整体删除。
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
    // 单元素退化：AND 没有直属 gram、只剩一个子查询时，等价于该子查询本身。
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
    // 扁平化：操作数是 OR 时并入其 grams/subs；单 gram 的 AND（of_gram 的产物）
    // 直接降级为本节点的 gram 叶子；其余整体作为子查询。
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
    // 吸收律：OR 内已有 gram g，则含 g 的子 AND 恒被 g 蕴含（AND 要求同时满足，
    // 其中一个条件已在 OR 的其他分支满足），对整个 OR 不再构成约束，可删除。
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
    // 单元素退化。
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
    // 子查询按各自 serialize() 文本排序后输出，保证结构相同则文本相同。
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

// AND/OR 允许嵌套的最大深度（顶层调用为 1）。超过后立即拒绝，避免形如重复
// "&(" 的畸形/恶意输入递归过深导致爆栈（本仓库有过深递归爆栈先例 CIR-21633）。
constexpr int kMaxNestingDepth = 64;

// 解析 AND/OR item 列表里的一个 base64 gram token（即 t[i] 不是 &/|/*/! 之一、
// parse_at 判定为普通 item 的情形）：从 t[i] 起扫到下一个 ',' 或 ')' 作为 token
// 边界，base64 解码后包成 GramQuery::of_gram；解析成功后 i 指向该 token 结束
// 后的下一个位置（未消费分隔符本身）。从 parse_at 抽出以降低其复杂度，语义与
// 原内联的 else 分支完全一致。
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

// 从 t[i] 起解析一个 GramQuery；解析成功后 i 指向该查询结束后的下一个位置。
// depth 为当前嵌套深度（顶层调用为 1），用于限制递归层数，防止爆栈。
//
// AND/OR 节点里的每个 item 都通过 GramQuery::and_/or_ 组合子折叠进累加器
// （AND 从 all() 起、OR 从 none() 起），而不是直接拼装 grams/subs 字段：这样
// 排序、去重、吸收律、ALL/NONE 短路、单元素退化等不变式自动成立。这些不变式
// 是 has_gram()（二分查找）与 or_absorb_subsets()（std::includes）正确工作的
// 前提——任何文本解析出的树都必须先满足它们，才能安全地参与后续 and_/or_
// 调用；直接拼装字段会产出这些不变式不允许的树，使后续操作静默出错。
// 同时严格校验语法，拒绝 serialize() 不会产出的宽松写法：空 item（连续逗号/
// 开头逗号/结尾逗号）、零操作数的 AND/OR（如 "&()"）、解码为空串的 gram。
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
    // 先解析到局部变量：只有整体成功（且没有尾随输入）才写回 *out，避免调用方
    // 在解析失败时观察到半成品树（例如旧实现里 trailing-input 场景下 *out
    // 已经被写入了顶层 token 对应的半成品结果）。
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
