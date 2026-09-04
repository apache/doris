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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace doris::segment_v2::gram {

// RE2 语法子集解析出的正则语法树节点。这棵树只保留 gram 编译器（Task
// 5：RegexGramCompiler）需要的信息，不是完整的正则引擎 AST：
//   - LIT：一个码点的 UTF-8 编码；多字节字面量（如中文）按码点整体成一个
//     LIT，不再拆到字节级别；
//   - CLASS：≤4 个码点的小类在 cls 里展开（已排序去重）；big_class=true
//     表示「不可枚举」的大类/取反类/`\d \w \s \D \W \S \pL \p{..}`/POSIX
//     类等，此时 cls 必为空；
//   - EMPTY：不产生字面量约束的节点（`^ $ \b \B \A \z`、纯标志分组等）；
//   - CAT/ALT/STAR/PLUS/QUEST：运算节点，语义与正则语法一致，通过 kids
//     挂子节点；
//   - REPEAT：区间量词 `{m}`/`{m,}`/`{m,n}`，rmax=-1 表示无上界（`{m,}`）。
// `(?i)` 标志开启后，ASCII 字母的字面量/类项会展开成大小写两个码点（如
// 'a' 产出 CLASS{'A','a'}），使上层在不区分大小写时仍能安全地把字面量折叠
// 成 gram 查询。
struct RegexNode {
    enum class Type : uint8_t { EMPTY, LIT, CLASS, ANY, CAT, ALT, STAR, PLUS, QUEST, REPEAT };
    Type type = Type::EMPTY;
    std::string lit; // LIT：一个码点的 UTF-8
    std::vector<std::string> cls; // CLASS：≤4 个码点的小类展开；空 + big_class=true 表示大类/取反类
    bool big_class = false;
    std::vector<std::unique_ptr<RegexNode>> kids;
    int rmin = 0, rmax = -1; // REPEAT
};

// 把 RE2 语法子集的 pattern 解析成 RegexNode AST。支持：字面量；转义
// （`\. \n \t \r \xHH \x{...} \Q..\E`）；类（`[...]`、取反、区间、POSIX
// 类、`\d \w \s \D \W \S \pL \p{..}`）；`.`；分组（捕获 / `(?:` /
// `(?P<name>` / `(?<name>`）；标志 `(?i) (?s) (?m) (?U)` 与 `(?i:...)`；
// 量词 `* + ? {m} {m,} {m,n}` 及其懒惰后缀；锚点 `^ $ \b \B \A \z`；`|`。
// 解析成功时 *root 持有整棵树、*case_insensitive 表示 pattern 中是否出现过
// `(?i)`；解析失败（语法错误、未闭合分组/类、悬空转义、分组嵌套过深等）
// 返回 Status::InvalidArgument，此时 *root、*case_insensitive
// 不保证被写入，调用方应据此保守回退（视为匹配全部行）。
Status parse_regex(std::string_view pattern, std::unique_ptr<RegexNode>* root,
                   bool* case_insensitive);

} // namespace doris::segment_v2::gram
