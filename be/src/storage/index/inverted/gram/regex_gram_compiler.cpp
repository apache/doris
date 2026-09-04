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

#include "storage/index/inverted/gram/regex_gram_compiler.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "storage/index/inverted/gram/regex_ast.h"

namespace doris::segment_v2::gram {

// Storage 是 Unity Build：同目录其他 .cpp 也有 utf8_len / codepoint_len 之类的同名
// 文件级 helper，故本文件的 helper 统一放进文件专属命名空间再套匿名命名空间。
namespace regex_gram_compiler_detail {
namespace {

// exact 集合「全部串都已 ≥ n」时更容易产出有效 gram，此时超过 4 个就降级；与
// kMaxExact（不管长短一律降级的上限）配合，来自原型 simplify 的两级阈值。
constexpr size_t kMaxLongExact = 4;
// analyze 递归深度上限。parse_regex 只限制了分组嵌套（64 层），而 `a++++...` 这类
// 堆叠量词会生成同样深的 PLUS/REPEAT 链，超限时退化为 info_any_match（不施加任何
// 约束，保守且安全），避免在用户可控的 pattern 上把栈跑爆。
constexpr int kMaxAnalyzeDepth = 200;
// REPEAT 展开的最大拷贝数：x{m,..} 精确展开 min(m, 4) 次再按 plus 处理。
constexpr int kMaxRepeatUnroll = 4;

// 前导字节 c 所属 UTF-8 序列的字节长度（1/2/3/4）；非法前导字节按 1 处理。
inline int utf8_len(unsigned char c) {
    if (c < 0x80) {
        return 1;
    }
    if ((c >> 5) == 0x6) {
        return 2;
    }
    if ((c >> 4) == 0xE) {
        return 3;
    }
    if ((c >> 3) == 0x1E) {
        return 4;
    }
    return 1;
}

// 从 p 开始的一个合法 UTF-8 码点的字节数；截断或延续字节不合法时返回 1。口径与
// GramExtractor 完全一致：非 ASCII 码点在索引侧是整体 1-gram。
inline size_t codepoint_len(const char* p, size_t remain) {
    int l = utf8_len((unsigned char)p[0]);
    if (l == 1 || (size_t)l > remain) {
        return 1;
    }
    for (int k = 1; k < l; k++) {
        if (((unsigned char)p[k] & 0xC0) != 0x80) {
            return 1;
        }
    }
    return l;
}

// s 的前 ≤ k 字节，且必须停在码点边界上。裁剪时切断多字节码点会拼出索引里根本不
// 存在的「假 gram」，那会导致误杀，因此这里宁可少留几个字节。
std::string head_units(const std::string& s, size_t k) {
    size_t i = 0;
    while (i < s.size()) {
        size_t l = codepoint_len(s.data() + i, s.size() - i);
        if (i + l > k) {
            break;
        }
        i += l;
    }
    return s.substr(0, i);
}

// s 的后 ≤ k 字节，且必须从码点边界起始（理由同 head_units）。
std::string tail_units(const std::string& s, size_t k) {
    if (s.size() <= k) {
        return s;
    }
    size_t i = 0;
    while (i < s.size() && s.size() - i > k) {
        i += codepoint_len(s.data() + i, s.size() - i);
    }
    return s.substr(i);
}

// 笛卡尔积拼接；规模超过 kMaxSet 时置 *too_big 并返回空集（调用方改走降级路径）。
std::set<std::string> cross(const std::set<std::string>& a, const std::set<std::string>& b,
                            bool* too_big) {
    std::set<std::string> r;
    *too_big = a.size() * b.size() > RegexGramCompiler::kMaxSet;
    if (*too_big) {
        return r;
    }
    for (const auto& x : a) {
        for (const auto& y : b) {
            r.insert(x + y);
        }
    }
    return r;
}

std::set<std::string> uni(const std::set<std::string>& a, const std::set<std::string>& b) {
    std::set<std::string> r = a;
    r.insert(b.begin(), b.end());
    return r;
}

// 集合中最短串的字节数；空集记为 0。
size_t min_str_len(const std::set<std::string>& s) {
    size_t m = SIZE_MAX;
    for (const auto& x : s) {
        m = std::min(m, x.size());
    }
    return m == SIZE_MAX ? 0 : m;
}

// Cox 五元组：一个正则子树能匹配的串集合的有限近似。
//   can_empty  子树可以匹配空串
//   has_exact  匹配集合已被 exact 完整枚举（此时 prefix/suffix 无意义）
//   exact      完整枚举的匹配串集合
//   prefix     所有匹配串的可能开头（集合外的开头不可能出现）
//   suffix     所有匹配串的可能结尾
//   match      已经确定「一定出现在匹配串里」的 gram 条件
// 不变式：has_exact 时 exact 非空；否则 prefix、suffix 均非空（"" 表示无约束）。
struct Info {
    bool can_empty = false;
    bool has_exact = false;
    std::set<std::string> exact;
    std::set<std::string> prefix;
    std::set<std::string> suffix;
    GramQuery match; // 默认即 ALL
};

// 只匹配空串。
Info info_empty() {
    Info i;
    i.can_empty = true;
    i.has_exact = true;
    i.exact = {""};
    return i;
}

// 匹配恰好一个未知字符（`.`、大类、含 NUL 的不可索引字面量）。
Info info_any_char() {
    Info i;
    i.prefix = {""};
    i.suffix = {""};
    return i;
}

// 匹配任意串（`x*`、无法推导的子树）：不施加任何约束。
Info info_any_match() {
    Info i;
    i.can_empty = true;
    i.prefix = {""};
    i.suffix = {""};
    return i;
}

const std::set<std::string>& pre(const Info& x) {
    return x.has_exact ? x.exact : x.prefix;
}

const std::set<std::string>& suf(const Info& x) {
    return x.has_exact ? x.exact : x.suffix;
}

// Cox 推导的执行体。所有会产出 gram 的地方都经过 q_of_string / q_of_set，二者在
// 拿不到 gram 时返回 ALL，从而保证「宁可不过滤，也不误杀」。
class CoxAnalyzer {
public:
    explicit CoxAnalyzer(GramExtractor& extractor)
            : _extractor(extractor), _scheme(extractor.scheme()) {}

    // 整棵树 → gram 查询。
    GramQuery compile(const RegexNode* root) {
        Info info = _analyze(root, 0);
        GramQuery m = std::move(info.match);
        if (info.has_exact) {
            m = GramQuery::and_(std::move(m), q_of_set(info.exact));
        } else {
            m = GramQuery::and_(std::move(m), q_of_set(info.prefix));
            m = GramQuery::and_(std::move(m), q_of_set(info.suffix));
        }
        return m;
    }

    // 字面量 s 的全部 gram 之 AND。s 切不出 gram（太短 / 无 CDC 边界）时返回 ALL。
    GramQuery q_of_string(const std::string& s) {
        // 索引侧不会产出含 NUL 的 gram，这里也绝不能产出（Ruling R9）。analyze 已把
        // 含 NUL 的字面量节点当作 anyChar，这里是 compile_like 等其他入口的兜底。
        if (s.find('\0') != std::string::npos) {
            return GramQuery::all();
        }
        std::vector<std::string> g;
        _extractor.grams_of_literal(s, &g);
        if (g.empty()) {
            return GramQuery::all();
        }
        GramQuery q;
        q.op = GramQuery::Op::AND;
        q.grams = std::move(g);
        std::sort(q.grams.begin(), q.grams.end());
        q.grams.erase(std::unique(q.grams.begin(), q.grams.end()), q.grams.end());
        return q;
    }

    // 集合内任一串出现即可 → OR。空集表示「匹配不到任何串」，故为 NONE。
    GramQuery q_of_set(const std::set<std::string>& ss) {
        if (ss.empty()) {
            return GramQuery::none();
        }
        GramQuery r = GramQuery::none();
        for (const auto& s : ss) {
            r = GramQuery::or_(std::move(r), q_of_string(s));
        }
        return r;
    }

private:
    // prefix/suffix 的保留长度：SPARSE 的 gram 最长 max_len，故要留够一整个 gram；
    // DENSE 定长 n，只需留 n-1（再长也只是重复已折进 match 的 gram）。
    size_t _keep() const {
        if (_scheme.mode == GramMode::SPARSE) {
            return _scheme.max_len;
        }
        return _scheme.min_len >= 1 ? _scheme.min_len - 1 : 0;
    }

    // 集合里每个串是否都够长（≥ n）。只有全部够长时才值得把整个集合折进 match：
    // 只要有一个串切不出 gram，OR 起来就是 ALL，折了也没有任何约束力。
    bool _all_long(const std::set<std::string>& s) const {
        return std::ranges::all_of(s,
                                   [this](const auto& x) { return x.size() >= _scheme.min_len; });
    }

    // scheme.lower_case 时索引侧已把 ASCII 字母折成小写，字面量必须同样折叠。
    // 注意不能对整个 pattern 串做 lowercase——那会破坏 `\E \B \W \D \S \P \A`
    // 这些区分大小写的转义，所以折叠只发生在 AST 叶子（LIT / CLASS 元素）上。
    std::string _fold(const std::string& s) const {
        if (!_scheme.lower_case) {
            return s;
        }
        std::string r = s;
        for (auto& ch : r) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return r;
    }

    // 把集合的 gram 折进 match（要求集合中每个串都 ≥ n，否则无信息），再把串裁到
    // keep 个字节；裁完仍超过 kMaxSet 就继续缩短 keep，直到集合够小或裁成空串。
    void _trim_set(std::set<std::string>* s, GramQuery* match, bool is_suffix) {
        if (s->empty()) {
            return;
        }
        if (_all_long(*s)) {
            *match = GramQuery::and_(std::move(*match), q_of_set(*s));
        }
        size_t keep = _keep();
        for (;;) {
            std::set<std::string> t;
            for (const auto& x : *s) {
                t.insert(is_suffix ? tail_units(x, keep) : head_units(x, keep));
            }
            *s = std::move(t);
            if (s->size() <= RegexGramCompiler::kMaxSet || keep == 0) {
                break;
            }
            keep--;
        }
    }

    // 把 exact 降级为 prefix/suffix：完整枚举维持不住了，但「匹配串一定以 exact
    // 中某个串开头、也一定以它结尾」仍然成立，同时先把 gram 折进 match 保住信息。
    void _demote(Info* x) {
        if (!x->has_exact) {
            return;
        }
        if (_all_long(x->exact)) {
            x->match = GramQuery::and_(std::move(x->match), q_of_set(x->exact));
        }
        x->prefix = x->exact;
        x->suffix = x->exact;
        x->exact.clear();
        x->has_exact = false;
    }

    // 集合规模/串长超阈值时降级 exact，并对 prefix/suffix 做裁剪。
    // 原型 simplify 还有一个 force 形参，但所有调用点都传 false，此处省略。
    Info _simplify(Info x) {
        if (x.has_exact) {
            const size_t ml = min_str_len(x.exact);
            const bool all_long = _all_long(x.exact);
            // 三个降级条件：枚举太多（kMaxExact）；虽然不多但每个串都已能切出 gram，
            // 再枚举下去只是让 OR 分支变多（kMaxLongExact）；串已长到 ≥ 2n，继续拼接
            // 只会让 exact 更长而不再新增 gram。三者都不如降级成 prefix/suffix、把已
            // 有的 gram 落进 match 划算。
            if (x.exact.size() > RegexGramCompiler::kMaxExact ||
                (all_long && x.exact.size() > kMaxLongExact) ||
                ml >= static_cast<size_t>(2) * _scheme.min_len) {
                _demote(&x);
            }
        }
        if (!x.has_exact) {
            _trim_set(&x.prefix, &x.match, false);
            _trim_set(&x.suffix, &x.match, true);
        }
        return x;
    }

    // 连接 xy。
    Info _concat_info(Info x, Info y) {
        Info r;
        if (x.has_exact && y.has_exact) {
            bool big = false;
            std::set<std::string> c = cross(x.exact, y.exact, &big);
            if (!big) {
                r.has_exact = true;
                r.exact = std::move(c);
            } else {
                _demote(&x);
                _demote(&y);
            }
        }
        if (!r.has_exact) {
            if (x.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.exact, y.prefix, &big);
                if (big) {
                    _demote(&x);
                    r.prefix = x.prefix;
                    if (x.can_empty) {
                        r.prefix = uni(r.prefix, y.prefix);
                    }
                } else {
                    r.prefix = std::move(c);
                }
            } else {
                r.prefix = x.prefix;
                if (x.can_empty) {
                    r.prefix = uni(r.prefix, pre(y));
                }
            }
            if (y.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.suffix, y.exact, &big);
                if (big) {
                    _demote(&y);
                    r.suffix = y.suffix;
                    if (y.can_empty) {
                        r.suffix = uni(r.suffix, x.suffix);
                    }
                } else {
                    r.suffix = std::move(c);
                }
            } else {
                r.suffix = y.suffix;
                if (y.can_empty) {
                    r.suffix = uni(r.suffix, suf(x));
                }
            }
            // 边界 gram：x 的某个后缀与 y 的某个前缀在匹配串里一定是连续相接的，
            // 拼起来的串因此必然是匹配串的子串，可以直接折进 match。
            if (!x.has_exact && !y.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.suffix, y.prefix, &big);
                if (!big && !c.empty() && _all_long(c)) {
                    r.match = GramQuery::and_(std::move(r.match), q_of_set(c));
                }
            }
        }
        r.match = GramQuery::and_(std::move(r.match),
                                  GramQuery::and_(std::move(x.match), std::move(y.match)));
        r.can_empty = x.can_empty && y.can_empty;
        return _simplify(std::move(r));
    }

    // 选择 x|y。
    Info _alt_info(Info x, Info y) {
        Info r;
        if (x.has_exact && y.has_exact) {
            std::set<std::string> u = uni(x.exact, y.exact);
            if (u.size() <= RegexGramCompiler::kMaxSet) {
                r.has_exact = true;
                r.exact = std::move(u);
            } else {
                _demote(&x);
                _demote(&y);
            }
        }
        if (!r.has_exact) {
            _demote(&x);
            _demote(&y);
            r.prefix = uni(x.prefix, y.prefix);
            r.suffix = uni(x.suffix, y.suffix);
        }
        r.can_empty = x.can_empty || y.can_empty;
        r.match = GramQuery::or_(std::move(x.match), std::move(y.match));
        return _simplify(std::move(r));
    }

    // x+：重复次数未知，只能保留「以 x 的某个串开头、以 x 的某个串结尾」。
    Info _plus_info(Info x) {
        _demote(&x);
        return _simplify(std::move(x));
    }

    // 区间量词 REPEAT `{m}`/`{m,}`/`{m,n}`：精确展开 min(m, kMaxRepeatUnroll) 次
    // （能捕获跨拷贝的边界 gram），次数还可能更多（rmax 未达上界或超过展开次数）
    // 时再按 plus 处理只保留首尾。从 _analyze 抽出以降低其复杂度/长度，语义与
    // 原 switch case 完全一致。
    Info _repeat_info(const RegexNode* n, int depth) {
        // rmin < 0 只可能来自解析阶段的计数溢出，此时区间不可信，保守处理。
        if (n->kids.empty() || n->rmin < 0) {
            return info_any_match();
        }
        if (n->rmin == 0 && n->rmax == 0) {
            return info_empty();
        }
        if (n->rmin == 0) {
            return info_any_match();
        }
        Info acc = info_empty();
        const int reps = std::min(n->rmin, kMaxRepeatUnroll);
        for (int k = 0; k < reps; k++) {
            acc = _concat_info(std::move(acc), _analyze(n->kids[0].get(), depth + 1));
        }
        if (n->rmax == n->rmin && n->rmin <= kMaxRepeatUnroll) {
            return acc;
        }
        return _plus_info(std::move(acc));
    }

    Info _analyze(const RegexNode* n, int depth) {
        if (n == nullptr || depth > kMaxAnalyzeDepth) {
            return info_any_match();
        }
        switch (n->type) {
        case RegexNode::Type::EMPTY:
            return info_empty();
        case RegexNode::Type::LIT: {
            std::string lit = _fold(n->lit);
            // Ruling R9：含 NUL 的字面量不可索引，整个节点当作一个未知字符。
            if (lit.find('\0') != std::string::npos) {
                return info_any_char();
            }
            Info i;
            i.has_exact = true;
            i.exact.insert(std::move(lit));
            return _simplify(std::move(i));
        }
        case RegexNode::Type::CLASS: {
            DCHECK(!n->big_class || n->cls.empty());
            if (n->big_class || n->cls.empty()) {
                return info_any_char();
            }
            Info i;
            i.has_exact = true;
            for (const auto& s : n->cls) {
                std::string f = _fold(s);
                // 只要有一项不可索引，整个类就退化为未知字符：丢掉其中一项会让
                // exact 不再覆盖全部可能，那正是误杀的来源。
                if (f.find('\0') != std::string::npos) {
                    return info_any_char();
                }
                i.exact.insert(std::move(f)); // 折叠后可能重复，set 自动去重
            }
            return _simplify(std::move(i));
        }
        case RegexNode::Type::ANY:
            return info_any_char();
        case RegexNode::Type::CAT: {
            Info acc = info_empty();
            for (const auto& k : n->kids) {
                acc = _concat_info(std::move(acc), _analyze(k.get(), depth + 1));
            }
            return acc;
        }
        case RegexNode::Type::ALT: {
            if (n->kids.empty()) {
                return info_any_match();
            }
            Info acc = _analyze(n->kids[0].get(), depth + 1);
            for (size_t k = 1; k < n->kids.size(); k++) {
                acc = _alt_info(std::move(acc), _analyze(n->kids[k].get(), depth + 1));
            }
            return acc;
        }
        case RegexNode::Type::STAR:
            return info_any_match();
        case RegexNode::Type::PLUS:
            if (n->kids.empty()) {
                return info_any_match();
            }
            return _plus_info(_analyze(n->kids[0].get(), depth + 1));
        case RegexNode::Type::QUEST:
            if (n->kids.empty()) {
                return info_any_match();
            }
            return _alt_info(_analyze(n->kids[0].get(), depth + 1), info_empty());
        case RegexNode::Type::REPEAT:
            return _repeat_info(n, depth);
        }
        return info_any_match();
    }

    GramExtractor& _extractor;
    const GramScheme& _scheme;
};

} // namespace
} // namespace regex_gram_compiler_detail

RegexGramCompiler::RegexGramCompiler(const GramScheme& scheme) : _extractor(scheme) {}

Status RegexGramCompiler::compile_regexp(std::string_view pattern, GramQuery* out) {
    std::unique_ptr<RegexNode> root;
    bool case_insensitive = false;
    // 解析失败一律保守回退为 ALL，绝不返回错误让上层查询失败。
    if (!parse_regex(pattern, &root, &case_insensitive).ok() || root == nullptr) {
        *out = GramQuery::all();
        return Status::OK();
    }
    // case_insensitive 本身不需要额外处理：lower_case=false 时 parse_regex 已把
    // `(?i)` 下的 ASCII 字母字面量展开成 CLASS{c,C}（Cox 做法）；lower_case=true 时
    // 索引与查询都折叠小写，CoxAnalyzer 在 AST 叶子上统一折叠即可。
    regex_gram_compiler_detail::CoxAnalyzer analyzer(_extractor);
    *out = analyzer.compile(root.get());
    return Status::OK();
}

Status RegexGramCompiler::compile_like(std::string_view like_pattern, GramQuery* out) {
    regex_gram_compiler_detail::CoxAnalyzer analyzer(_extractor);
    GramQuery q = GramQuery::all();
    std::string seg;
    // 通配符切断字面量段：段内的字节在匹配行里必然连续出现，可以直接取 gram。
    auto flush = [&] {
        if (!seg.empty()) {
            q = GramQuery::and_(std::move(q), analyzer.q_of_string(seg));
            seg.clear();
        }
    };
    for (size_t i = 0; i < like_pattern.size(); i++) {
        const char c = like_pattern[i];
        if (c == '\\') {
            if (i + 1 < like_pattern.size()) {
                const char next = like_pattern[i + 1];
                if (next == '%' || next == '_' || next == '\\') {
                    // Doris LIKE 真正的转义只有这三种：反斜杠被吃掉，next 作为字面量
                    // 并入当前段（next 与它前后的字符在匹配行里必然连续出现）。
                    seg.push_back(next);
                    i++;
                    continue;
                }
                // `\x`（x 不是 % _ \ 之一）：不确定引擎是保留反斜杠本身（行内是
                // "\x" 两字节）还是丢弃反斜杠（行内只有 "x"，旧实现的假设）。
                // 两种语义下都能确定的只有「x 与它之后的字符连续」，x 与它之前
                // 的字符之间是否隔着一个反斜杠是未知的，因此在反斜杠处切段——
                // 反斜杠本身不产生任何 gram，x 留给下一轮当作新段的起点，不与
                // 已切出的旧段合并（只损失一点裁剪力，绝不会漏杀）。
                flush();
                continue;
            }
            // 模式尾部单独一个反斜杠，没有可转义的对象：切段后忽略。
            flush();
            continue;
        }
        if (c == '%' || c == '_') {
            flush();
            continue;
        }
        seg.push_back(c);
    }
    flush();
    *out = std::move(q);
    return Status::OK();
}

} // namespace doris::segment_v2::gram
