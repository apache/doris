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

#include <map>
#include <optional>
#include <string>

#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris {
class IndexPolicyMgr;
}

namespace doris::segment_v2::gram {

// 从索引属性解析 analyzer/normalizer 名 -> 策略 -> gram 方案，供只有索引属性、拿不到
// analyzer provider 的调用方（查询侧）判断一个索引是否属于"gram 族"（tokenizer 为 ngram
// 且携带 mode 属性）并取得其 GramScheme 参数。
//
// 返回 nullopt 的情形：analyzer 名为空、名字是内置 analyzer（standard/english/... —— 这些
// 名字从不进策略管理器，由 InvertedIndexAnalyzer::create_analyzer_provider 自己兜住）、
// 策略管理器未就绪、或策略存在但不是 gram 族。
//
// 抛异常的情形：名字既不是内置 analyzer 又在策略管理器里找不到对应策略时，
// IndexPolicyMgr::get_analyzer_provider_by_name 抛 "Policy not found"。本函数不吞掉它——
// 那是真正的配置错误，由调用方（阶段 C 的查询侧）决定是报错还是降级为全表扫描。
//
// 注意：写入侧（SNII writer）不走这里。它自己就要创建 analyzer provider，直接问那个
// provider 要 gram_scheme() 即可，既少一次策略解析，也保证"实际用于分词的 analyzer"与
// "被判定为 gram 族的 analyzer"永远是同一个对象（Ruling R21）。
std::optional<GramScheme> resolve_gram_scheme(
        const std::map<std::string, std::string>& index_properties, IndexPolicyMgr* mgr);

} // namespace doris::segment_v2::gram
