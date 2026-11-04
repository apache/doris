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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/logging.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Hit.h"

namespace doris::segment_v2 {

class DictSegment;

// A dictionary as the analysis reads it: a read-only copy of a DictSegment trie in one array,
// built once the dictionary file is loaded, after which the tree is freed. A lookup is an array
// probe or a scan of adjacent nodes instead of a hash lookup behind a pointer per character.
//
// Node 0 is the root and the other nodes are numbered breadth first, so the children of a node are
// consecutive nodes, in increasing key; the root's children are also found through a table indexed
// by the first-plane character. Say the words are 中, 中国, 中国人 and 人: the root's children are
// node 1 (中) and node 2 (人), node 1's only child is node 3 (国) and node 3's is node 4 (人); nodes
// 1 to 4 end a word, nodes 2 and 4 have no child, and the table maps 中 to 1 and 人 to 2.
class FlatDict {
public:
    // The root, which is nobody's child, also stands for no node.
    static constexpr uint32_t kNoNode = 0;

    // Copies the trie under `root`; defined in DictSegment.cpp.
    explicit FlatDict(const DictSegment& root);
    FlatDict(FlatDict&&) noexcept = default;
    FlatDict& operator=(FlatDict&&) noexcept = default;
    FlatDict(const FlatDict&) = delete;
    FlatDict& operator=(const FlatDict&) = delete;

    // The child of `node` for `key`, or kNoNode. A node of up to kLinearChildren children is
    // scanned, one of more is searched by bisection.
    uint32_t child(uint32_t node, int32_t key) const {
        if (node == kNoNode && key >= 0 && key < kDirectKeys) {
            return root_children_[key];
        }
        const Node& parent = nodes_[node];
        const Node* const first = nodes_.data() + parent.child_begin;
        const Node* const last = first + parent.child_count;
        if (parent.child_count <= kLinearChildren) {
            for (const Node* it = first; it != last; ++it) {
                if (it->key == key) {
                    return static_cast<uint32_t>(it - nodes_.data());
                }
            }
            return kNoNode;
        }
        const Node* const it = std::lower_bound(
                first, last, key,
                [](const Node& entry, int32_t value) { return entry.key < value; });
        return it != last && it->key == key ? static_cast<uint32_t>(it - nodes_.data()) : kNoNode;
    }

    bool isWordEnd(uint32_t node) const { return nodes_[node].word_end != 0; }
    bool hasChildren(uint32_t node) const { return nodes_[node].child_count != 0; }

    // Walks `length` characters from `unicode_offset` (all that are left when `length` is 0 or
    // more than are left) down from `node`, as the pointer trie's match did. `hit` ends unmatched
    // at the first character without a child, with that character's end; otherwise the last
    // character's node decides: it is a match if a word ends there and a prefix, which the next
    // character continues from, if it has children. Say the words are 中国 and 中国人 and the text
    // is 中国人: from the root, 中 is a prefix only; from its node, 国 is a match and a prefix;
    // from that one, 人 is a match only.
    void match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
               size_t length, uint32_t node, Hit& hit) const {
        DCHECK_LT(unicode_offset, typed_runes.size());
        if (length == 0 || unicode_offset + length > typed_runes.size()) {
            length = typed_runes.size() - unicode_offset;
        }
        while (true) {
            const CharacterUtil::TypedRune& rune = typed_runes[unicode_offset];
            hit.setUnmatch();
            hit.setByteEnd(rune.offset + rune.getByteLength());
            hit.setCharEnd(unicode_offset);
            node = child(node, rune.getChar());
            if (node == kNoNode) {
                return;
            }
            if (length == 1) {
                if (isWordEnd(node)) {
                    hit.setMatch();
                }
                if (hasChildren(node)) {
                    hit.setPrefix();
                    hit.setMatchedNode(this, node);
                }
                return;
            }
            ++unicode_offset;
            --length;
        }
    }

    // match() from the root, with the hit starting at `unicode_offset`.
    Hit match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
              size_t length) const {
        Hit hit;
        hit.setByteBegin(typed_runes[unicode_offset].offset);
        hit.setCharBegin(unicode_offset);
        match(typed_runes, unicode_offset, length, kNoNode, hit);
        return hit;
    }

    // Whether the characters match() walks are a word: match(...).isMatch() without the hit.
    bool isWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                size_t length) const {
        DCHECK_LT(unicode_offset, typed_runes.size());
        if (length == 0 || unicode_offset + length > typed_runes.size()) {
            length = typed_runes.size() - unicode_offset;
        }
        uint32_t node = kNoNode;
        for (size_t i = unicode_offset; i < unicode_offset + length; ++i) {
            node = child(node, typed_runes[i].getChar());
            if (node == kNoNode) {
                return false;
            }
        }
        return isWordEnd(node);
    }

private:
    // The characters below this have a slot of their own in the root's table.
    static constexpr int32_t kDirectKeys = 0x10000;
    static constexpr uint32_t kLinearChildren = 8;

    struct Node {
        int32_t key;
        uint32_t child_begin;
        uint32_t child_count : 31;
        uint32_t word_end : 1;
    };

    std::vector<uint32_t> root_children_;
    std::vector<Node> nodes_;
};

} // namespace doris::segment_v2
