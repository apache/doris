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

#include "storage/index/snii/query/gram_boolean_query.h"

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"

namespace doris::snii::query {

// R8/R24（Unity Build）：文件级辅助实现放在这个本文件专属的具名命名空间里，
// 而不是裸的匿名命名空间，这样 Unity Build 下就不会与其他文件的符号冲突。
namespace gram_boolean_query_detail {

// 与 snii_index_reader.cpp 匿名命名空间里的 RoaringDocIdSink 行为完全一致
// （非空批次走 addMany，非空区间走 addRange(first, last_exclusive)，
// dedups()==true 使多 gram 的 OR/AND 可以把 posting 直接流式写入同一个
// 位图）；那个类无法跨翻译单元复用，因此这里另外复制一份。
class RoaringSink final : public DocIdSink {
public:
    explicit RoaringSink(roaring::Roaring* bitmap) : _bitmap(bitmap) {}

    Status append_sorted(std::span<const uint32_t> docids) override {
        if (!docids.empty()) {
            _bitmap->addMany(docids.size(), docids.data());
        }
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        if (last_exclusive > first) {
            _bitmap->addRange(first, last_exclusive);
        }
        return Status::OK();
    }

    bool dedups() const override { return true; }

private:
    roaring::Roaring* _bitmap;
};

Status eval(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
            roaring::Roaring* out);

// AND：先对 gram 查 df，缺失的 gram 让整体直接短路为空，不读取任何
// posting；剩余的 gram 叶子按 df 升序求交（最省成本的驱动项排最前面，
// 交集一旦为空立即提前返回），随后以同样的「求交+提前返回」方式处理每个
// 子查询。既无 gram 叶子也无子查询的 AND 视为 ALL。
Status eval_and(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
                roaring::Roaring* out) {
    std::vector<std::pair<uint64_t, const std::string*>> order;
    order.reserve(q.grams.size());
    for (const auto& gram : q.grams) {
        bool found = false;
        uint64_t df = 0;
        RETURN_IF_ERROR(src.df(gram, &found, &df));
        if (!found) {
            // 索引只产生超集候选：缺失的 gram 让整个 AND 分支确定为空，直接返回，
            // 不再读取任何 posting。
            return Status::OK();
        }
        order.emplace_back(df, &gram);
    }
    std::ranges::sort(order,
                      [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    bool has_acc = false;
    roaring::Roaring acc;
    for (const auto& ordered_gram : order) {
        roaring::Roaring cur;
        RETURN_IF_ERROR(src.postings(*ordered_gram.second, &cur));
        if (!has_acc) {
            acc = std::move(cur);
            has_acc = true;
        } else {
            acc &= cur;
        }
        if (acc.isEmpty()) {
            return Status::OK();
        }
    }
    for (const auto& sub : q.subs) {
        roaring::Roaring cur;
        RETURN_IF_ERROR(eval(src, sub, num_docs, &cur));
        if (!has_acc) {
            acc = std::move(cur);
            has_acc = true;
        } else {
            acc &= cur;
        }
        if (acc.isEmpty()) {
            return Status::OK();
        }
    }
    if (!has_acc) {
        // 既无 gram 叶子也无子查询：AND 退化为 ALL。
        out->addRange(0, num_docs);
        return Status::OK();
    }
    *out |= acc;
    return Status::OK();
}

// OR：对每个直属 gram 叶子的 postings 与每个子查询的求值结果求并。
Status eval_or(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
               roaring::Roaring* out) {
    for (const auto& gram : q.grams) {
        RETURN_IF_ERROR(src.postings(gram, out));
    }
    for (const auto& sub : q.subs) {
        roaring::Roaring cur;
        RETURN_IF_ERROR(eval(src, sub, num_docs, &cur));
        *out |= cur;
    }
    return Status::OK();
}

Status eval(GramPostingSource& src, const segment_v2::gram::GramQuery& q, uint32_t num_docs,
            roaring::Roaring* out) {
    using Op = segment_v2::gram::GramQuery::Op;
    switch (q.op) {
    case Op::ALL:
        out->addRange(0, num_docs);
        return Status::OK();
    case Op::NONE:
        return Status::OK();
    case Op::AND:
        return eval_and(src, q, num_docs, out);
    case Op::OR:
        return eval_or(src, q, num_docs, out);
    }
    return Status::OK();
}

} // namespace gram_boolean_query_detail

Status LogicalIndexPostingSource::df(std::string_view gram, bool* found, uint64_t* df) {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(_idx.lookup(gram, found, &entry, &frq_base, &prx_base));
    *df = *found ? entry.df : 0;
    return Status::OK();
}

Status LogicalIndexPostingSource::postings(std::string_view gram, roaring::Roaring* out) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(_idx.lookup(gram, &found, &entry, &frq_base, &prx_base));
    if (!found) {
        return Status::OK();
    }
    gram_boolean_query_detail::RoaringSink sink(out);
    return internal::read_docid_posting(_idx, entry, frq_base, prx_base, &sink);
}

Status gram_boolean_query(GramPostingSource& src, const segment_v2::gram::GramQuery& q,
                          uint32_t num_docs, roaring::Roaring* out) {
    return gram_boolean_query_detail::eval(src, q, num_docs, out);
}

} // namespace doris::snii::query
