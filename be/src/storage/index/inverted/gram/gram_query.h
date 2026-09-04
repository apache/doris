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
#include <string_view>
#include <vector>

#include "common/status.h"

namespace doris::segment_v2::gram {

// gram 上的布尔查询树：ALL/NONE 是短路常量，AND/OR 节点既可以直接挂 gram 叶子
// （grams 字段），也可以挂子查询（subs 字段），二者在语义上都是该节点算子的操作数。
// and_/or_ 是构造非平凡查询的唯一入口，构造时即完成化简（扁平化、去重、吸收律、
// NONE/ALL 短路、单元素退化），因此任意时刻的 GramQuery 实例都已是化简后的规范
// 形态，可以安全地序列化为文本（用于 InvertedIndexParam::query_value 与缓存
// key）或还原为可读的调试字符串（EXPLAIN 用）。
struct GramQuery {
    enum class Op : uint8_t { ALL, NONE, AND, OR };
    Op op = Op::ALL;
    std::vector<std::string> grams; // 本节点直接持有的 gram 叶子（仅 AND/OR 有效）
    std::vector<GramQuery> subs;    // 子查询（仅 AND/OR 有效）

    static GramQuery all() { return GramQuery {}; }
    static GramQuery none() {
        GramQuery q;
        q.op = Op::NONE;
        return q;
    }
    // 单个 gram 视为只含一个 gram 的 AND 节点，从而统一走 and_/or_ 的化简逻辑。
    static GramQuery of_gram(std::string g) {
        GramQuery q;
        q.op = Op::AND;
        q.grams.push_back(std::move(g));
        return q;
    }
    // 构造并化简：扁平化同类算子、gram 去重排序、吸收律（AND 内已有 gram 时含它的
    // 子 OR 被吸收，OR 内已有 gram 时含它的子 AND 被吸收）、OR 内子 AND 的子集吸收
    // 超集、按结构去重、NONE/ALL 短路、单元素退化。
    static GramQuery and_(GramQuery a, GramQuery b);
    static GramQuery or_(GramQuery a, GramQuery b);
    bool is_all() const { return op == Op::ALL; }
    bool is_none() const { return op == Op::NONE; }
    // 叶子 gram 总数，递归统计所有子查询。
    size_t leaf_count() const;
    // 文本格式：ALL→"*"，NONE→"!"，AND→"&(" items ")"，OR→"|(" items ")"；
    // gram 用 base64 编码以避免与分隔符冲突；items 以 ',' 分隔，子查询按各自
    // serialize() 结果排序后输出，从而保证结构相同则文本相同。
    std::string serialize() const;
    // serialize() 的逆操作：语法必须严格匹配 serialize() 的输出形状（拒绝空
    // item、空 gram、零操作数的 AND/OR 等宽松写法），解析出的树按 and_/or_
    // 重新化简以满足不变式；嵌套深度有上限。输入不合法或嵌套过深时返回
    // Status::InvalidArgument，此时 *out 保持调用前的值不变。
    static Status parse(std::string_view text, GramQuery* out);
    // 可读形式，供 EXPLAIN 使用，例如 "(\"abc\" & (\"de\" | \"fg\"))"。
    std::string to_debug_string() const;
    // 结构去重用的规范字符串键：两个查询结构相同（子查询顺序无关，排序后比较）
    // 时该键相等。
    std::string structural_key() const;
};

} // namespace doris::segment_v2::gram
