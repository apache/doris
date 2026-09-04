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
#include <cstddef>
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {

// 把正则 / LIKE 模式编译成 gram 上的布尔查询（Cox「五元组」推导：
// can_empty / exact / prefix / suffix / match）。
//
// 唯一的硬性不变量是「只能漏杀、不能误杀」：对任意行 r，若模式匹配 r，则编译出的
// GramQuery 在「按同一个 GramScheme 对 r 提取出的 gram 集合」上必然为真。因此凡是
// 无法可靠推导的地方（解析失败、字符类过大、集合爆炸、字面量不足一个 gram 长等）
// 一律退化为 ALL（不过滤任何行），绝不产出可能筛掉命中行的更强条件。
//
// 编译器只把字面量折叠成 gram，不做任何正则语义判断：真正的行级匹配仍由上层的
// regexp / like 表达式完成，本查询只用于跳过绝无可能命中的行。
//
// 实例持有 GramExtractor 的内部缓冲，因此两个 compile_* 都是非 const 且非线程安全：
// 每个使用线程各自构造一个（构造只需算一张 8 KB 边界位图）。
class RegexGramCompiler {
public:
    explicit RegexGramCompiler(const GramScheme& scheme);

    // 编译正则。任何解析 / 推导失败都返回 OK 且 *out = ALL（保守回退全表扫描），
    // 只有内部断言失败才返回非 OK。
    Status compile_regexp(std::string_view pattern, GramQuery* out);

    // 编译 LIKE：在 % 与 _ 处切断为字面量段，每段的 gram 之 AND 再彼此 AND；
    // 全通配（无字面量段）时为 ALL。
    //
    // 转义字符固定假定为 `\`；且只把 `\%`、`\_`、`\\` 当作转义（对应 Doris LIKE
    // 的实际语义），转义后的字符作为字面量并入当前段。其余 `\x`（x 不是这三者
    // 之一）无法确定引擎是否保留反斜杠本身，因此在两种可能语义下都保守地在
    // 反斜杠处切段、不产生跨越该处的 gram，x 仍作为下一段的起点参与后续字面量
    // （只损失一点裁剪力，绝不会漏杀）；模式尾部单独一个 `\` 同样切段后忽略。
    // 不支持 `ESCAPE` 子句：本函数的字面量/通配符切分固定假定 `\` 是转义字符，
    // 调用方遇到非 `\` 的自定义转义字符时必须不走索引，否则推导出的边界会与
    // 引擎实际语义不一致，可能误杀（把真正匹配的行错误过滤掉）。
    //
    // ILIKE 语义由调用方通过 scheme.lower_case 决定（索引与查询同时折叠）。
    Status compile_like(std::string_view like_pattern, GramQuery* out);

    // exact/prefix/suffix 集合的规模上限：笛卡尔积或并集超过它就降级，避免枚举爆炸。
    static constexpr size_t kMaxSet = 20;
    // exact 集合超过它就降级为 prefix/suffix（先把已有的 gram 折进 match）。
    static constexpr size_t kMaxExact = 7;

private:
    // 提取器同时是 scheme 的持有者（_extractor.scheme()），避免两份配置不一致。
    GramExtractor _extractor;
};

} // namespace doris::segment_v2::gram
