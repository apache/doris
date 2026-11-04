// Derived from cppjieba's Trie.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <boost/container/small_vector.hpp>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

inline constexpr size_t MAX_WORD_LENGTH = 512;

// A dictionary word and its weight. Upstream's part-of-speech tag is gone: only tagging read it.
struct DictUnit {
    Unicode word;
    double weight = 0.0;
};

// The dictionary words that start at one rune of a range. Upstream's runestr (a copy of the rune,
// which no cut path reads) and nextPos (never set) are gone.
struct Dag {
    // [offset, nexts.first]: the end of each word, inclusive, and its unit; the rune alone comes
    // first, with a null unit when it is no word, and the longer words follow in increasing end.
    // The first 16 live in the Dag, as in upstream's LocalVector, whose inline buffer holds 16:
    // with a std::vector, the heap allocation for every rune made the two cuts together 14-27%
    // slower than upstream on Chinese and Japanese text.
    boost::container::small_vector<std::pair<size_t, const DictUnit*>, 16> nexts;
    const DictUnit* p_info = nullptr;
    double weight = 0.0;
};

using TrieKey = Rune;

struct TrieNode {
    using NextMap = std::unordered_map<TrieKey, std::unique_ptr<TrieNode>>;
    // Null until a key goes through this node.
    std::unique_ptr<NextMap> next;
    // The unit of the word that ends here, or null when no word ends here.
    const DictUnit* pt_value = nullptr;
};

// The nodes are owned by unique_ptrs where upstream deleted them by hand.
class Trie {
public:
    Trie(const std::vector<Unicode>& keys, const std::vector<const DictUnit*>& value_pointers)
            : _root(std::make_unique<TrieNode>()) {
        create_trie(keys, value_pointers);
    }

    // Fills `res` with a Dag for each rune of [begin, end): the words of at most `max_word_len`
    // runes that start there. `res` is empty when it comes in.
    void find(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
              std::vector<Dag>& res, size_t max_word_len = MAX_WORD_LENGTH) const {
        DCHECK(_root != nullptr);
        const auto size = static_cast<size_t>(end - begin);
        res.resize(size);

        const TrieNode* pt_node = nullptr;
        for (size_t i = 0; i < size; i++) {
            pt_node = nullptr;
            if (_root->next != nullptr) {
                const auto citer = _root->next->find((begin + i)->rune);
                if (citer != _root->next->end()) {
                    pt_node = citer->second.get();
                }
            }
            if (pt_node != nullptr) {
                res[i].nexts.emplace_back(i, pt_node->pt_value);
            } else {
                res[i].nexts.emplace_back(i, nullptr);
            }

            for (size_t j = i + 1; j < size && (j - i + 1) <= max_word_len; j++) {
                if (pt_node == nullptr || pt_node->next == nullptr) {
                    break;
                }
                const auto citer = pt_node->next->find((begin + j)->rune);
                if (pt_node->next->end() == citer) {
                    break;
                }
                pt_node = citer->second.get();
                if (nullptr != pt_node->pt_value) {
                    res[i].nexts.emplace_back(j, pt_node->pt_value);
                }
            }
        }
    }

private:
    // A key that is inserted again takes the later unit: a user word replaces the main
    // dictionary's unit of the same word.
    void insert_node(const Unicode& key, const DictUnit* pt_value) {
        // The unit of a word that did not decode has an empty word, and no node.
        if (key.begin() == key.end()) {
            return;
        }

        TrieNode* pt_node = _root.get();
        for (const TrieKey rune : key) {
            if (nullptr == pt_node->next) {
                pt_node->next = std::make_unique<TrieNode::NextMap>();
            }
            const auto km_iter = pt_node->next->find(rune);
            if (pt_node->next->end() == km_iter) {
                pt_node = pt_node->next->emplace(rune, std::make_unique<TrieNode>())
                                  .first->second.get();
            } else {
                pt_node = km_iter->second.get();
            }
        }
        DCHECK(pt_node != nullptr);
        pt_node->pt_value = pt_value;
    }

    // Upstream returned early on empty keys or values; the only caller passes one value per key
    // and at least one key, and empty lists would insert nothing anyway.
    void create_trie(const std::vector<Unicode>& keys,
                     const std::vector<const DictUnit*>& value_pointers) {
        DORIS_CHECK(keys.size() == value_pointers.size());
        for (size_t i = 0; i < keys.size(); i++) {
            insert_node(keys[i], value_pointers[i]);
        }
    }

    std::unique_ptr<TrieNode> _root;
};

} // namespace doris::segment_v2::inverted_index::jieba
