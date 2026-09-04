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

#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {

// 按 GramScheme 把一行文本切成一组 gram：DENSE 用定长滑窗，SPARSE 用内容定义分块
// （CDC）规则，依据字节对边界哈希切出变长 gram。切分规则只依赖局部字节内容（当前
// 位置起、至多 max_len 字节的窗口），因此查询侧对字面量重新提取时，得到的 gram
// 集合必然是索引侧对整行提取结果的子集，可用于编译器折叠正则字面量。
//
// Ruling R9：产出的 gram 一律不含 NUL 字节（0x00）。候选窗口只要跨过 NUL 就整体
// 跳过、不产出，窗口边界本身的计算不受影响（局部性不因此被破坏）；非 ASCII 码点
// 走 1-gram 路径，天然不可能含 NUL，无需额外处理。RegexGramCompiler 一侧对含
// NUL 的字面量 / 类项同样退化为不可索引的未知字符（当作 anyChar），索引与查询
// 两侧口径保持一致，避免 NUL 字节被单独一侧当作可索引内容而导致误杀。
class GramExtractor {
public:
    explicit GramExtractor(const GramScheme& scheme);

    // 对一个列值提取 gram；返回的 string_view 指向提取器内部缓冲（lower_case 时）
    // 或原始 value，在下一次 extract 调用前有效。行内已去重，且按出现顺序稳定。
    // lower_case=true 时先对输入做 ASCII 折叠再切分（边界哈希在折叠后的字节上计算）。
    void extract(std::string_view value, std::vector<std::string_view>* out);

    // 查询侧使用：只返回「窗口完整落在 s 内」的 gram，规则与 extract 完全一致——
    // extract 本身产出的窗口就总是完整落在其所属 ASCII 段（或单个码点）内，因此
    // 这里等价于 extract，只是返回具备独立生命周期的 std::string，供编译器折叠
    // 正则里的字面量片段使用。
    void grams_of_literal(std::string_view s, std::vector<std::string>* out);

    const GramScheme& scheme() const { return _scheme; }

    // 边界判定：字节对 (a,b) 是否为 CDC 边界。65536 项位图，构造时按
    // (hash_version, density_permille) 一次性算好，查询时是 O(1) 位测试。
    bool is_boundary(uint8_t a, uint8_t b) const {
        unsigned idx = ((unsigned)a << 8) | b;
        return (_boundary_bits[idx >> 3] >> (idx & 7)) & 1;
    }

private:
    // 构造边界位图：mix64((((uint64_t)a<<8)|b) ^ 0x5bd1e995) & 0xFFFF <
    // density_permille * 65536 / 1000。
    void _build_boundary_table();
    // 对一个纯 ASCII 段按 scheme 切 gram（DENSE 定长滑窗 / SPARSE CDC 规则）。
    void _ascii_segment(std::string_view seg, std::vector<std::string_view>* out);
    // 行内去重，保持首次出现的相对顺序。
    void _dedupe(std::vector<std::string_view>* out);

    GramScheme _scheme;
    std::vector<uint8_t> _boundary_bits; // 65536 bit 边界位图，8192 字节
    std::string _folded; // lower_case 时的 ASCII 折叠副本，输出 view 可能指向它
    std::vector<uint8_t> _is_boundary_at; // SPARSE 模式下复用的每位置边界标记缓冲
};

} // namespace doris::segment_v2::gram
