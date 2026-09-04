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

#include "storage/index/inverted/gram/regex_ast.h"

#include <algorithm>
#include <cctype>
#include <cstdint>

namespace doris::segment_v2::gram {

// BE 的 Storage 目标开启了 CMake Unity Build（多个 .cpp
// 合并编译，见 be/src/storage/CMakeLists.txt 的 UNITY_BUILD_BATCH_SIZE），
// 同一批次内所有文件的匿名命名空间会被合并进同一个翻译单元，裸的匿名命名空间
// 一旦与批次内其他文件重名（哪怕在不同 .cpp 里）就会重定义报错，且批次分组
// 会随目录下文件增删而变化，无法长期假设「这批次只有这几个文件」。因此这里
// 额外套一层本文件专属的具名命名空间，隔离本文件的匿名命名空间，不影响其中
// 各符号本身仍是内部链接（匿名命名空间语义不受具名外层嵌套影响）。
namespace regex_ast_detail {

namespace {

// 分组 `(...)` 的最大递归嵌套深度：解析每多套一层分组，就会多递归一层
// parse_alt/parse_cat/parse_atom 调用链。畸形（或恶意构造）的正则可以用
// 大量嵌套括号把这个调用链撑得很深，从而爆栈；本仓库有过深递归爆栈的
// 先例（CIR-21633），因此这里做一个硬上限，超过直接报错而不是继续递归。
constexpr int kMaxNestingDepth = 64;

// ------------------------------------------------------------------
// 以下 UTF-8 编解码工具与 Parser 均从原型 tools/regex-ngram-model/ngram_model.cpp
// 的 utf8_len/decode_cps/encode_cp 与 struct Parser 移植而来，语义保持一致；
// 移植改动仅：Node → RegexNode（含枚举 T → Type 的相应限定写法）、ok/err
// 在 parse_regex 里转换为 Status::InvalidArgument、新增 depth 嵌套深度计数。
// ------------------------------------------------------------------

// 前导字节推断该 UTF-8 序列的字节长度；非法前导字节按单字节处理。
int utf8_len(unsigned char c) {
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
    return 1; // 非法前导字节：按单字节处理
}

// 解码为码点序列；非法字节映射到 0x110000+byte（仍 < 2^21，不与合法码点冲突）。
void decode_cps(std::string_view s, std::vector<uint32_t>* out) {
    out->clear();
    size_t i = 0;
    while (i < s.size()) {
        unsigned char c = s[i];
        int l = utf8_len(c);
        if (l == 1) {
            out->push_back(c < 0x80 ? c : 0x110000 + c);
            i++;
            continue;
        }
        if (i + l > s.size()) {
            out->push_back(0x110000 + c);
            i++;
            continue;
        }
        uint32_t v = (l == 2) ? (c & 0x1F) : (l == 3) ? (c & 0x0F) : (c & 0x07);
        bool ok = true;
        for (int k = 1; k < l; k++) {
            unsigned char cc = s[i + k];
            if ((cc & 0xC0) != 0x80) {
                ok = false;
                break;
            }
            v = (v << 6) | (cc & 0x3F);
        }
        if (!ok) {
            out->push_back(0x110000 + c);
            i++;
            continue;
        }
        out->push_back(v);
        i += l;
    }
}

// 把一个码点编码为 UTF-8 追加到 out。
void encode_cp(uint32_t cp, std::string* out) {
    if (cp < 0x80) {
        out->push_back((char)cp);
    } else if (cp < 0x800) {
        out->push_back((char)(0xC0 | (cp >> 6)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out->push_back((char)(0xE0 | (cp >> 12)));
        out->push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    } else {
        out->push_back((char)(0xF0 | (cp >> 18)));
        out->push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
        out->push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    }
}

using NP = std::unique_ptr<RegexNode>;

NP mk(RegexNode::Type t) {
    auto p = std::make_unique<RegexNode>();
    p->type = t;
    return p;
}

// RE2 语法子集的递归下降解析器，从原型 struct Parser 移植。除头部注释所述
// 的三处改动外，函数拆分、控制流、每条语法规则的处理顺序均与原型一致。
struct Parser {
    std::string_view p;
    size_t i = 0;
    bool icase = false;
    bool ok = true;
    std::string err;
    int depth = 0; // 当前分组嵌套深度，见 kMaxNestingDepth

    explicit Parser(std::string_view s) : p(s) {}

    bool eof() const { return i >= p.size(); }
    char peek() const { return eof() ? 0 : p[i]; }

    uint32_t next_cp(std::string* utf8) {
        if (eof()) {
            // 防御性兜底：正常调用点在进 next_cp 前都会先确认还有字符可读，这里
            // 只是不信任调用方、避免 string_view（不保证 NUL 结尾，不同于原型用
            // 的 std::string）在 i==size() 时被 p[i] 越界读。
            utf8->clear();
            return 0;
        }
        int l = utf8_len((unsigned char)p[i]);
        if (i + l > p.size()) {
            l = 1;
        }
        *utf8 = std::string(p.substr(i, l));
        std::vector<uint32_t> cps;
        decode_cps(*utf8, &cps);
        i += l;
        return cps.empty() ? 0 : cps[0];
    }

    NP parse() {
        NP r = parse_alt();
        if (!eof()) {
            ok = false;
            err = "trailing input at " + std::to_string(i);
        }
        return r;
    }

    NP parse_alt() {
        std::vector<NP> branches;
        branches.push_back(parse_cat());
        while (peek() == '|') {
            i++;
            branches.push_back(parse_cat());
        }
        if (branches.size() == 1) {
            return std::move(branches[0]);
        }
        NP a = mk(RegexNode::Type::ALT);
        a->kids = std::move(branches);
        return a;
    }

    NP parse_cat() {
        NP c = mk(RegexNode::Type::CAT);
        while (!eof() && peek() != '|' && peek() != ')') {
            NP atom = parse_atom();
            if (!ok) {
                return c;
            }
            if (!atom) {
                continue; // 例如 (?i) 这种仅设标志的空原子
            }
            atom = parse_quant(std::move(atom));
            c->kids.push_back(std::move(atom));
        }
        return c;
    }

    NP parse_quant(NP a) {
        while (!eof()) {
            char c = peek();
            if (c == '*') {
                i++;
                NP s = mk(RegexNode::Type::STAR);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '+') {
                i++;
                NP s = mk(RegexNode::Type::PLUS);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '?') {
                i++;
                NP s = mk(RegexNode::Type::QUEST);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '{') {
                size_t save = i;
                i++;
                int mn = 0;
                int mx = -1;
                bool has = false;
                while (!eof() && std::isdigit(static_cast<unsigned char>(peek()))) {
                    mn = mn * 10 + (peek() - '0');
                    i++;
                    has = true;
                }
                if (!has) {
                    i = save;
                    break;
                }
                if (peek() == ',') {
                    i++;
                    if (std::isdigit(static_cast<unsigned char>(peek()))) {
                        mx = 0;
                        while (!eof() && std::isdigit(static_cast<unsigned char>(peek()))) {
                            mx = mx * 10 + (peek() - '0');
                            i++;
                        }
                    }
                } else {
                    mx = mn;
                }
                if (peek() != '}') {
                    i = save;
                    break;
                }
                i++;
                NP s = mk(RegexNode::Type::REPEAT);
                s->rmin = mn;
                s->rmax = mx;
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else {
                break;
            }
            if (peek() == '?') {
                i++; // 懒惰量词不影响匹配集合
            }
        }
        return a;
    }

    // class_escape 里 `\x` 转义的十六进制取值：调用时 "\x" 已被消费（i 指向
    // 花括号或首个十六进制数字）。Ruling R12：`\x{...}` 形式要求花括号内至少
    // 一位十六进制数字且必须闭合；裸 `\xHH` 形式要求恰好两位十六进制数字，
    // 不足（到串尾或遇到非十六进制字符）一律报错，与 RE2 拒绝 `\x4` 的行为
    // 对齐。成功时把值写入 *v 并返回 true；失败则置 ok=false、err 并返回
    // false（调用方据此直接 return，不再继续）。从 class_escape 抽出以降低
    // 其复杂度/长度，语义与原内联代码完全一致。
    bool parse_hex_escape_value(uint32_t* v) {
        *v = 0;
        if (peek() == '{') {
            i++;
            int cnt = 0;
            while (!eof() && peek() != '}') {
                if (!std::isxdigit(static_cast<unsigned char>(peek()))) {
                    ok = false;
                    err = "bad \\x escape";
                    return false;
                }
                *v = *v * 16 +
                     (std::isdigit(static_cast<unsigned char>(peek()))
                              ? peek() - '0'
                              : (std::tolower(static_cast<unsigned char>(peek())) - 'a' + 10));
                i++;
                cnt++;
            }
            if (eof() || cnt == 0) {
                ok = false;
                err = "bad \\x escape";
                return false;
            }
            i++; // 消费 '}'
            return true;
        }
        for (int cnt = 0; cnt < 2; cnt++) {
            if (eof() || !std::isxdigit(static_cast<unsigned char>(peek()))) {
                ok = false;
                err = "bad \\x escape";
                return false;
            }
            *v = *v * 16 +
                 (std::isdigit(static_cast<unsigned char>(peek()))
                          ? peek() - '0'
                          : (std::tolower(static_cast<unsigned char>(peek())) - 'a' + 10));
            i++;
        }
        return true;
    }

    // 把类里的一个码点或转义加入 out（大类置 big）。
    void class_escape(std::vector<uint32_t>* out, bool* big) {
        // 调用方总是先消费了触发转义的 '\\' 再调用本函数；若此时已经 eof，说明
        // 该 '\\' 是整个 pattern 的最后一个字符，后面没有被转义的字符——与顶层
        // parse_atom 里 "trailing backslash" 是同一种错误，同样处理：直接报错，
        // 不再往下走到 default 分支里 next_cp 会再读一次 i（此时 i==p.size()）。
        if (eof()) {
            ok = false;
            err = "trailing backslash";
            return;
        }
        char c = peek();
        i++;
        switch (c) {
        case 'd':
        case 'w':
        case 's':
        case 'D':
        case 'W':
        case 'S':
            *big = true;
            break;
        case 'n':
            out->push_back('\n');
            break;
        case 't':
            out->push_back('\t');
            break;
        case 'r':
            out->push_back('\r');
            break;
        case 'x': {
            uint32_t v = 0;
            if (!parse_hex_escape_value(&v)) {
                return;
            }
            out->push_back(v);
            break;
        }
        case 'p':
        case 'P':
            *big = true;
            if (peek() == '{') {
                while (!eof() && peek() != '}') {
                    i++;
                }
                i++;
            } else {
                i++;
            }
            break;
        default: {
            i--;
            std::string u;
            out->push_back(next_cp(&u));
            break;
        }
        }
    }

    // [...] 内单个码点 lo 之后的 `-hi` 区间处理：是区间就展开 [lo,hi]（超过 4 项
    // 连同已有 items 一起退化为大类）；不是区间（下一字符不是 '-'，或 '-' 紧邻
    // ']'，即字面量 '-'）就把 lo 本身作为一个单独 item。从 parse_class 抽出以
    // 降低其复杂度/长度，语义与原内联代码完全一致。
    void parse_class_range_or_single(uint32_t lo, std::vector<uint32_t>* items, bool* big) {
        if (!(peek() == '-' && i + 1 < p.size() && p[i + 1] != ']')) {
            items->push_back(lo);
            return;
        }
        i++;
        uint32_t hi;
        if (peek() == '\\') {
            i++;
            std::vector<uint32_t> tmp;
            bool b2 = false;
            class_escape(&tmp, &b2);
            hi = tmp.empty() ? lo : tmp.back();
            if (b2) {
                *big = true;
            }
        } else {
            std::string u;
            hi = next_cp(&u);
        }
        if (hi < lo) {
            std::swap(lo, hi);
        }
        if (hi - lo + 1 + items->size() > 4) {
            *big = true;
        } else {
            for (uint32_t x = lo; x <= hi; x++) {
                items->push_back(x);
            }
        }
    }

    NP parse_class() {
        // 已消费 '['
        NP n = mk(RegexNode::Type::CLASS);
        bool neg = false;
        bool big = false;
        if (peek() == '^') {
            neg = true;
            i++;
        }
        std::vector<uint32_t> items;
        bool first = true;
        while (!eof() && (peek() != ']' || first)) {
            first = false;
            if (peek() == '[' && i + 1 < p.size() && p[i + 1] == ':') { // [:alpha:] 等 POSIX 类
                size_t e = p.find(":]", i);
                if (e == std::string_view::npos) {
                    ok = false;
                    err = "bad posix class";
                    return n;
                }
                i = e + 2;
                big = true;
                continue;
            }
            uint32_t lo;
            if (peek() == '\\') {
                i++;
                size_t before = items.size();
                class_escape(&items, &big);
                if (items.size() == before) {
                    continue;
                }
                lo = items.back();
                items.pop_back();
            } else {
                std::string u;
                lo = next_cp(&u);
            }
            parse_class_range_or_single(lo, &items, &big);
        }
        if (peek() != ']') {
            ok = false;
            err = "unterminated class";
            return n;
        }
        i++;
        if (neg || big || items.size() > 4) {
            n->big_class = true;
            return n;
        }
        for (auto cp : items) {
            std::string u;
            encode_cp(cp, &u);
            n->cls.push_back(u);
            if (icase && cp < 128 && std::isalpha(static_cast<unsigned char>(cp))) {
                std::string v;
                encode_cp(std::islower(static_cast<unsigned char>(cp))
                                  ? std::toupper(static_cast<unsigned char>(cp))
                                  : std::tolower(static_cast<unsigned char>(cp)),
                          &v);
                n->cls.push_back(v);
            }
        }
        std::sort(n->cls.begin(), n->cls.end());
        n->cls.erase(std::unique(n->cls.begin(), n->cls.end()), n->cls.end());
        if (n->cls.size() > 4) {
            // `(?i)` 下大小写展开可能把 ≤4 个原始码点翻倍到 >4 项（如
            // (?i)[abc] 展开成 6 项），此时退化为大类；big_class=true 时 cls
            // 必须清空（见头文件 RegexNode 注释的不变式），否则调用方无法仅凭
            // big_class 判断「是否可枚举」。
            n->big_class = true;
            n->cls.clear();
        }
        return n;
    }

    NP make_lit(uint32_t cp) const {
        std::string u;
        encode_cp(cp, &u);
        if (icase && cp < 128 && std::isalpha(static_cast<unsigned char>(cp))) {
            NP n = mk(RegexNode::Type::CLASS);
            std::string v;
            encode_cp(std::islower(static_cast<unsigned char>(cp))
                              ? std::toupper(static_cast<unsigned char>(cp))
                              : std::tolower(static_cast<unsigned char>(cp)),
                      &v);
            n->cls = {u, v};
            std::sort(n->cls.begin(), n->cls.end());
            return n;
        }
        NP n = mk(RegexNode::Type::LIT);
        n->lit = u;
        return n;
    }

    // parse_atom 里 `\` 转义分支的具体解码：调用时 '\\' 已被消费、且已确认未到
    // 达 eof（trailing-backslash 由调用方在切换前处理），据 peek() 到的转义选择
    // 字符分派。从 parse_atom 抽出以降低其复杂度/长度，语义与原内联 switch
    // 完全一致。
    NP parse_backslash_escape() {
        char e = peek();
        switch (e) {
        case 'd':
        case 'w':
        case 's':
        case 'D':
        case 'W':
        case 'S': {
            i++;
            NP n = mk(RegexNode::Type::CLASS);
            n->big_class = true;
            return n;
        }
        case 'b':
        case 'B':
        case 'A':
        case 'z':
            i++;
            return mk(RegexNode::Type::EMPTY);
        case 'p':
        case 'P': {
            i++;
            if (peek() == '{') {
                while (!eof() && peek() != '}') {
                    i++;
                }
                i++;
            } else {
                i++;
            }
            NP n = mk(RegexNode::Type::CLASS);
            n->big_class = true;
            return n;
        }
        case 'n':
            i++;
            return make_lit('\n');
        case 't':
            i++;
            return make_lit('\t');
        case 'r':
            i++;
            return make_lit('\r');
        case 'x': {
            i++;
            std::vector<uint32_t> tmp;
            bool big = false;
            i--;
            class_escape(&tmp, &big);
            return make_lit(tmp.empty() ? 0 : tmp[0]);
        }
        case 'Q': { // \Q...\E 字面量
            i++;
            NP cat = mk(RegexNode::Type::CAT);
            while (!eof() && !(peek() == '\\' && i + 1 < p.size() && p[i + 1] == 'E')) {
                std::string u;
                uint32_t cp = next_cp(&u);
                cat->kids.push_back(make_lit(cp));
            }
            if (!eof()) {
                i += 2;
            }
            return cat;
        }
        default: {
            std::string u;
            uint32_t cp = next_cp(&u);
            return make_lit(cp);
        }
        }
    }

    // parse_atom 里 '(' 分支：解析捕获组 / 非捕获组 `(?:...)` / 命名组
    // `(?P<name>...)`、`(?<name>...)` 与内联标志 `(?i) (?is) (?i:...)`，随后
    // 递归解析组内内容并校验闭合括号，同时维护 kMaxNestingDepth 递归深度上限。
    // 调用时前导 '(' 已被消费。从 parse_atom 抽出以降低其复杂度/长度，语义与
    // 原内联代码完全一致。
    NP parse_group() {
        if (peek() == '?') {
            i++;
            if (peek() == ':') {
                i++;
            } else if (peek() == 'P' || peek() == '<') { // 命名组
                if (peek() == 'P') {
                    i++;
                }
                if (peek() != '<') {
                    ok = false;
                    err = "bad group";
                    return nullptr;
                }
                while (!eof() && peek() != '>') {
                    i++;
                }
                i++;
            } else { // 标志 (?i) (?is) (?i:...)
                bool neg = false;
                while (!eof() && peek() != ')' && peek() != ':') {
                    char f = peek();
                    i++;
                    if (f == '-') {
                        neg = true;
                    } else if (f == 'i') {
                        icase = !neg;
                    } else if (f == 's' || f == 'm' || f == 'U') {
                        // 已识别但不影响 AST 结构：(?s)(?m)(?U) 不改变
                        // 字面量/类的匹配集合，gram 编译不需要区分。
                    } else {
                        ok = false;
                        err = "unknown flag";
                        return nullptr;
                    }
                }
                if (peek() == ')') {
                    i++;
                    return nullptr;
                }
                i++; // ':'
            }
        }
        depth++;
        if (depth > kMaxNestingDepth) {
            ok = false;
            err = "regex nesting too deep";
            depth--;
            return nullptr;
        }
        NP inner = parse_alt();
        depth--;
        if (peek() != ')') {
            ok = false;
            err = "missing )";
            return inner;
        }
        i++;
        return inner;
    }

    NP parse_atom() {
        char c = peek();
        if (c == '(') {
            i++;
            return parse_group();
        }
        if (c == '[') {
            i++;
            return parse_class();
        }
        if (c == '.') {
            i++;
            return mk(RegexNode::Type::ANY);
        }
        if (c == '^' || c == '$') {
            i++;
            return mk(RegexNode::Type::EMPTY);
        }
        if (c == '\\') {
            i++;
            if (eof()) {
                ok = false;
                err = "trailing backslash";
                return nullptr;
            }
            return parse_backslash_escape();
        }
        if (c == '*' || c == '+' || c == '?') {
            ok = false;
            err = "bad quantifier";
            return nullptr;
        }
        std::string u;
        uint32_t cp = next_cp(&u);
        return make_lit(cp);
    }
};

} // namespace

} // namespace regex_ast_detail

Status parse_regex(std::string_view pattern, std::unique_ptr<RegexNode>* root,
                   bool* case_insensitive) {
    regex_ast_detail::Parser p(pattern);
    std::unique_ptr<RegexNode> r = p.parse();
    if (!p.ok) {
        return Status::InvalidArgument("regex parse error: {}", p.err);
    }
    *case_insensitive = p.icase;
    *root = std::move(r);
    return Status::OK();
}

} // namespace doris::segment_v2::gram
