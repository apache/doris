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

#include <unordered_map>

#include "unicode.h"

namespace doris {
using namespace std;

const size_t MAX_WORD_LENGTH = 512;

using TrieKey = Rune;

struct Dag {
    vector<uint32_t> nexts;
}; // struct Dag

class TrieNode {
public:
    using NextMap = unordered_map<TrieKey, TrieNode*>;
    TrieNode() = default;

    NextMap* next {};
    const Unicode* ptValue {};
};

class Trie {
public:
    Trie(const vector<Unicode>& keys) : root_(new TrieNode) { create_trie(keys); }
    ~Trie() { delete_node(root_); }

    void insert_node(const Unicode& key) const {
        if (key.begin() == key.end()) {
            return;
        }

        TrieNode::NextMap::const_iterator kmIter;
        TrieNode* ptNode = root_;
        for (Unicode::const_iterator iter = key.begin(); iter != key.end(); ++iter) {
            if (nullptr == ptNode->next) {
                ptNode->next = new TrieNode::NextMap;
            }
            kmIter = ptNode->next->find(*iter);
            if (ptNode->next->end() == kmIter) {
                TrieNode* nextNode = new TrieNode;
                ptNode->next->insert(make_pair(*iter, nextNode));
                ptNode = nextNode;
            } else {
                ptNode = kmIter->second;
            }
        }
        ptNode->ptValue = &key;
    }

    [[nodiscard]] const Unicode* find(RuneStrArray::const_iterator begin,
                                      RuneStrArray::const_iterator end) const {
        const TrieNode* ptNode = root_;
        TrieNode::NextMap::const_iterator citer;
        const Unicode* longestWord = nullptr;

        for (RuneStrArray::const_iterator it = begin; it != end; ++it) {
            if (nullptr == ptNode->next) {
                break;
            }
            citer = ptNode->next->find(it->rune);
            if (ptNode->next->end() == citer) {
                break;
            }
            ptNode = citer->second;
            if (ptNode->ptValue) {
                longestWord = ptNode->ptValue;
            }
        }

        return longestWord;
    }

    void find(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
              vector<struct Dag>& res, size_t max_word_len = MAX_WORD_LENGTH) const {
        assert(root_ != nullptr);

        const TrieNode* ptNode = nullptr;
        TrieNode::NextMap::const_iterator citer;
        if (root_->next == nullptr) {
            return;
        }

        RuneStrArray::const_iterator iter = begin;
        for (; iter != end; ++iter) {
            if (root_->next->end() != (citer = root_->next->find(iter->rune))) {
                ptNode = citer->second;
            } else {
                ptNode = nullptr;
            }

            if (ptNode != nullptr) {
                size_t distance = std::distance(begin, iter);
                res[distance].nexts.emplace_back(distance);
            } else {
                continue;
            }

            RuneStrArray::const_iterator innerIter = iter + 1;
            for (; innerIter != end && std::distance(iter, innerIter) <= max_word_len;
                 ++innerIter) {
                if (ptNode->next == nullptr ||
                    ptNode->next->end() == (citer = ptNode->next->find(innerIter->rune))) {
                    break;
                }
                ptNode = citer->second;
                if (ptNode->ptValue != nullptr) {
                    size_t distance = std::distance(begin, innerIter);
                    res[std::distance(begin, iter)].nexts.emplace_back(distance);
                }
            }
        }
    }

    const Unicode* find_next(RuneStrArray::const_iterator& it,
                             const RuneStrArray::const_iterator& end) const {
        const TrieNode* ptNode = root_;
        TrieNode::NextMap::const_iterator citer;

        for (; it != end; ++it) {
            if (nullptr == ptNode->next) {
                break;
            }
            citer = ptNode->next->find(it->rune);
            if (ptNode->next->end() == citer) {
                break;
            }
            ptNode = citer->second;
            if (ptNode->ptValue) {
                return ptNode->ptValue;
            }
        }

        return nullptr;
    }

    [[nodiscard]] std::vector<const Unicode*> find_all(RuneStrArray::const_iterator begin,
                                                       RuneStrArray::const_iterator end) const {
        std::vector<const Unicode*> matchedWords;

        if (begin == end) {
            return matchedWords;
        }

        const TrieNode* ptNode = root_;
        TrieNode::NextMap::const_iterator citer;

        for (RuneStrArray::const_iterator it = begin; it != end; ++it) {
            if (nullptr == ptNode->next) {
                break;
            }
            citer = ptNode->next->find(it->rune);
            if (ptNode->next->end() == citer) {
                break;
            }
            ptNode = citer->second;
            if (ptNode->ptValue) {
                matchedWords.push_back(ptNode->ptValue);
            }
        }

        if (begin != end && !matchedWords.empty()) {
            std::vector<const Unicode*> subsequentMatches = find_all(begin + 1, end);
            matchedWords.insert(matchedWords.end(), subsequentMatches.begin(),
                                subsequentMatches.end());
        }

        return matchedWords;
    }

    void delete_node(const Unicode& key) const {
        if (key.begin() == key.end()) {
            return;
        }
        TrieNode::NextMap::const_iterator kmIter;
        TrieNode* ptNode = root_;
        for (Unicode::const_iterator iter = key.begin(); iter != key.end(); ++iter) {
            if (nullptr == ptNode->next) {
                return;
            }
            kmIter = ptNode->next->find(*iter);
            if (ptNode->next->end() == kmIter) {
                break;
            }
            ptNode->next->erase(*iter);
            ptNode = kmIter->second;
            delete ptNode;
            break;
        }
    }

private:
    void create_trie(const vector<Unicode>& keys) const {
        if (keys.empty()) {
            return;
        }

        for (size_t i = 0; i < keys.size(); i++) {
            insert_node(keys[i]);
        }
    }

    void delete_node(TrieNode* node) {
        if (nullptr == node) {
            return;
        }
        if (nullptr != node->next) {
            for (TrieNode::NextMap::iterator iter = node->next->begin(); iter != node->next->end();
                 ++iter) {
                delete_node(iter->second);
            }
            delete node->next;
        }
        delete node;
    }

public:
    TrieNode* root_;
}; // class Trie

inline bool make_node_info(Unicode& node_info, const string& word) {
    if (!decode_runes_in_string(word.data(), word.length(), node_info)) {
        LOG(ERROR) << "Decode " << word << " failed.";
        return false;
    }
    return true;
}

} // namespace doris
