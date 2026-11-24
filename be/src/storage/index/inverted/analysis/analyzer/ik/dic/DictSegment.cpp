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

#include "storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.h"

#include <algorithm>
#include <limits>
#include <string>

#include "common/check.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/FlatDict.h"

namespace doris::segment_v2 {

// Here rather than in a FlatDict.cpp of its own: the trie is what it is built from, and a new
// source would move the Storage target's unity batches.
FlatDict::FlatDict(const DictSegment& root) {
    // `order` lists the segments in the order they are numbered; a segment's children are
    // appended when it is numbered, so they get the next free numbers, all in a row.
    std::vector<const DictSegment*> order {&root};
    std::vector<const DictSegment*> children;
    for (size_t i = 0; i < order.size(); ++i) {
        const DictSegment& segment = *order[i];
        children.clear();
        segment.forEachChild([&children](const DictSegment& child) { children.push_back(&child); });
        std::ranges::sort(children, [](const DictSegment* lhs, const DictSegment* rhs) {
            return lhs->keyChar() < rhs->keyChar();
        });
        // Every number fits child_begin, and every child count the 31 bits it has.
        DORIS_CHECK(order.size() + children.size() <= std::numeric_limits<uint32_t>::max() / 2);
        Node node;
        node.key = segment.keyChar();
        node.child_begin = static_cast<uint32_t>(order.size());
        node.child_count = static_cast<uint32_t>(children.size());
        node.word_end = segment.isWordEnd() ? 1 : 0;
        nodes_.push_back(node);
        order.insert(order.end(), children.begin(), children.end());
    }
    nodes_.shrink_to_fit();

    root_children_.assign(kDirectKeys, kNoNode);
    const Node& root_node = nodes_[kNoNode];
    for (uint32_t c = root_node.child_begin; c < root_node.child_begin + root_node.child_count;
         ++c) {
        if (nodes_[c].key >= 0 && nodes_[c].key < kDirectKeys) {
            root_children_[nodes_[c].key] = c;
        }
    }
}

DictSegment::DictSegment(int32_t key_char) : key_char_(key_char) {
    children_array_.reserve(ARRAY_LENGTH_LIMIT);
    live_count_.fetch_add(1, std::memory_order_relaxed);
}

DictSegment::~DictSegment() {
    live_count_.fetch_sub(1, std::memory_order_relaxed);
}

bool DictSegment::hasNextNode() const {
    return store_size_ > 0;
}

const DictSegment* DictSegment::findChild(int32_t key_char) const {
    if (store_size_ <= ARRAY_LENGTH_LIMIT) {
        for (size_t i = 0; i < store_size_; ++i) {
            if (children_array_[i]->key_char_ == key_char) {
                return children_array_[i].get();
            }
        }
        return nullptr;
    }
    const auto it = children_map_.find(key_char);
    return it == children_map_.end() ? nullptr : it->second.get();
}

void DictSegment::fillSegment(const char* text) {
    if (!text) return;

    std::string cleanText = text;
    if (!cleanText.empty() && static_cast<unsigned char>(cleanText[0]) == 0xEF &&
        static_cast<unsigned char>(cleanText[1]) == 0xBB &&
        static_cast<unsigned char>(cleanText[2]) == 0xBF) {
        cleanText.erase(0, 3);
    }
    if (!cleanText.empty() && cleanText.back() == '\r') {
        cleanText.pop_back();
    }

    if (cleanText.empty()) return;

    CharacterUtil::RuneStrArray runes;
    if (!CharacterUtil::decodeString(cleanText.c_str(), cleanText.length(), runes)) {
        return;
    }

    DictSegment* current = this;
    for (const auto& rune : runes) {
        auto child = current->lookforSegment(rune.rune, true);
        if (!child) return;
        current = child;
    }
    current->is_word_end_ = true;
}

DictSegment* DictSegment::lookforSegment(int32_t keyChar, bool create) {
    DictSegment* child = nullptr;

    if (store_size_ <= ARRAY_LENGTH_LIMIT) {
        for (size_t i = 0; i < store_size_ && i < children_array_.size(); i++) {
            if (children_array_[i]->key_char_ == keyChar) {
                child = children_array_[i].get();
                break;
            }
        }
        if (!child && create) {
            auto new_segment = std::make_unique<DictSegment>(keyChar);
            if (store_size_ < ARRAY_LENGTH_LIMIT) {
                if (store_size_ >= children_array_.size()) {
                    children_array_.resize(store_size_ + 1);
                }
                size_t insertPos = 0;
                while (insertPos < store_size_ && children_array_[insertPos]->key_char_ < keyChar) {
                    insertPos++;
                }
                for (size_t i = store_size_; i > insertPos; i--) {
                    children_array_[i] = std::move(children_array_[i - 1]);
                }
                child = new_segment.get();
                children_array_[insertPos] = std::move(new_segment);
                store_size_++;
            } else {
                for (size_t i = 0; i < store_size_; i++) {
                    children_map_[children_array_[i]->key_char_] = std::move(children_array_[i]);
                }
                child = new_segment.get();
                children_map_[keyChar] = std::move(new_segment);
                store_size_++;
                children_array_.clear();
                children_array_.shrink_to_fit();
            }
        }
    } else {
        auto it = children_map_.find(keyChar);
        if (it != children_map_.end()) {
            child = it->second.get();
        } else if (create) {
            auto new_segment = std::make_unique<DictSegment>(keyChar);
            child = new_segment.get();
            children_map_[keyChar] = std::move(new_segment);
            store_size_++;
        }
    }

    return child;
}

} // namespace doris::segment_v2
