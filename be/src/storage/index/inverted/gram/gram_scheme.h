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
#include <map>
#include <string>

#include "common/status.h"

namespace doris::segment_v2::gram {

// gram 方案的编码模式：DENSE 为逐字节滑窗全量 gram，SPARSE 为按哈希抽样的稀疏 gram。
// auto 是用户可见的第三个取值，但只存在于写入侧：解析样本后落盘为二者之一，
// 因此 GramScheme（落盘/缓存 key 的真源）里不出现 auto。
enum class GramMode : uint8_t { DENSE = 1, SPARSE = 2 };

// gram 方案的全部参数，作为 GramExtractor / RegexGramCompiler 等下游组件的唯一真源。
// 一个 GramScheme 实例完全决定了「同一段文本会切出哪些 gram」，因此它既要能从
// tokenizer/索引属性构造，也要能序列化回属性（写入段元数据）以及生成缓存 key。
struct GramScheme {
    GramMode mode = GramMode::SPARSE;
    uint32_t min_len = 3;            // n（字节）
    uint32_t max_len = 16;           // L（字节，仅 SPARSE 使用）
    uint32_t density_permille = 250; // p×1000（仅 SPARSE 使用）
    uint32_t stop_df_permille = 100; // τ×1000，0 表示不做高频 gram 裁剪
    bool lower_case = false;
    uint32_t hash_version = 1;

    // 从 tokenizer/索引属性构造 GramScheme；缺省值同上；出现非法值时返回 InvalidArgument。
    static Status from_properties(const std::map<std::string, std::string>& props, GramScheme* out);
    // 写回属性表，用于段元数据持久化与缓存 key 计算的输入。
    std::map<std::string, std::string> to_properties() const;
    // 形如 "gram:v1:sparse:3:16:250:100:lc0" 的缓存 key，唯一标识一套方案参数。
    std::string cache_key() const;
    bool operator==(const GramScheme& o) const {
        return mode == o.mode && min_len == o.min_len && max_len == o.max_len &&
               density_permille == o.density_permille && stop_df_permille == o.stop_df_permille &&
               lower_case == o.lower_case && hash_version == o.hash_version;
    }
};

} // namespace doris::segment_v2::gram
