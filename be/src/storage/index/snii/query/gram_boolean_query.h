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
#include <roaring/roaring.hh>
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"

// gram_boolean_query —— 对一棵 gram::GramQuery 布尔查询树在某个 SNII 段的
// gram 族词典/postings 上求值，产出一个 docid 位图。索引只能收窄候选集合：
// gram 缺失、查询形态不支持、或查找失败，都只能在上层退化为「不加速」，
// 绝不能改变查询结果，因此这里的每条代码路径都以 Status 返回，而不是断言
// gram 一定存在。
namespace doris::snii::query {

// gram_boolean_query() 消费的 posting 数据源。生产环境由下方的
// LogicalIndexPostingSource 适配 LogicalIndexReader；测试则注入一个基于 map
// 的假实现，从而在不构造真实索引文件的前提下覆盖 AND/OR/ALL/NONE 的求值逻辑。
class GramPostingSource {
public:
    virtual ~GramPostingSource() = default;
    // 查询一个 gram 的文档频率（df）。found=false 表示该 gram 不在词典中，
    // 此时任何包含它的 AND 节点都会求值为 NONE（空集），且不会再读取任何
    // posting 列表。
    virtual Status df(std::string_view gram, bool* found, uint64_t* df) = 0;
    // 将一个 gram 的完整 docid 集合（不含位置/词频）解码进 out。只会在 df()
    // 已确认该 gram 存在之后才会被调用。
    virtual Status postings(std::string_view gram, roaring::Roaring* out) = 0;
};

// 基于 LogicalIndexReader 的生产环境 GramPostingSource 实现：df() 只是一次
// 普通的词典查找，postings() 在此基础上还要解码 docid-only 的 posting。
class LogicalIndexPostingSource final : public GramPostingSource {
public:
    explicit LogicalIndexPostingSource(const reader::LogicalIndexReader& idx) : _idx(idx) {}
    Status df(std::string_view gram, bool* found, uint64_t* df) override;
    Status postings(std::string_view gram, roaring::Roaring* out) override;

private:
    const reader::LogicalIndexReader& _idx;
};

// 在 src 上对 q 求值：ALL -> [0, num_docs)；NONE -> 空集；AND 先对每个直属
// gram 叶子查询 df（任一叶子缺失即整体短路为空，不读取任何 posting），再按
// df 升序对剩余叶子求交，交集一旦为空立即提前返回，随后以同样的方式与每个
// 子查询求交（既无叶子也无子查询的 AND 退化为 ALL）；OR 则是对每个叶子的
// postings 与每个子查询的结果求并。递归深度由 GramQuery::parse 产出的树
// 决定（构造时已设有上限）。
Status gram_boolean_query(GramPostingSource& src, const segment_v2::gram::GramQuery& q,
                          uint32_t num_docs, roaring::Roaring* out);

} // namespace doris::snii::query
