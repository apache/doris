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

#include "smart_forest.h"

#include <iostream>

#include "common/logging.h"
#include "smart_get_word.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

std::vector<Rune> SmartForest::utf8_to_runes(const std::string& utf8_str) {
    std::vector<Rune> runes;
    runes.reserve(utf8_str.length());

    int32_t byte_pos = 0;
    const char* str_ptr = utf8_str.c_str();
    int32_t str_length = static_cast<int32_t>(utf8_str.length());

    while (byte_pos < str_length) {
        UChar32 cp;
        int32_t byte_start = byte_pos;

        U8_NEXT(str_ptr, byte_pos, str_length, cp);

        if (cp >= 0) {
            runes.emplace_back(byte_start, byte_pos, cp);
        }
    }

    return runes;
}

SmartForest* SmartForest::add(std::unique_ptr<SmartForest> branch_node) {
    if (!branch_node) {
        return nullptr;
    }

    UChar32 c = branch_node->getC();
    SmartForest* result_ptr = nullptr;

    auto it = branches.find(c);
    if (it != branches.end()) {
        SmartForest* existing = it->second.get();
        uint8_t new_status = branch_node->getStatus();
        uint8_t existing_status = existing->getStatus();

        switch (new_status) {
        case CONTINUE:
            if (existing_status == WORD_END) {
                existing->setStatus(WORD_CONTINUE);
            }
            break;
        case WORD_END:
            if (existing_status != WORD_END) {
                existing->setStatus(WORD_CONTINUE);
            }
            existing->setParam(branch_node->getParam());
            break;
        default:
            break;
        }

        result_ptr = existing;
    } else {
        result_ptr = branch_node.get();
        branches[c] = std::move(branch_node);
    }

    return result_ptr;
}

int SmartForest::getIndex(UChar32 c) {
    return branches.find(c) != branches.end() ? static_cast<int>(c) : -1;
}

bool SmartForest::contains(UChar32 c) {
    return branches.find(c) != branches.end();
}

int SmartForest::compareTo(UChar32 c) const {
    if (c_ < c) return -1;
    if (c_ > c) return 1;
    return 0;
}

bool SmartForest::equals(UChar32 c) const {
    return c_ == c;
}

void SmartForest::add(const std::string& keyWord, const ParamType& param) {
    std::vector<Rune> runes = utf8_to_runes(keyWord);
    add(runes, param);
}

void SmartForest::add(const std::vector<Rune>& runes, const ParamType& param) {
    SmartForest* tempBranch = this;

    for (size_t i = 0; i < runes.size(); i++) {
        UChar32 cp = runes[i].cp;

        if (i == runes.size() - 1) {
            auto new_node = std::make_unique<SmartForest>(cp, WORD_END, param);
            tempBranch->add(std::move(new_node));
        } else {
            auto new_node = std::make_unique<SmartForest>(cp, CONTINUE, ParamType {});
            tempBranch->add(std::move(new_node));
        }

        tempBranch = tempBranch->getBranch(cp);

        if (!tempBranch) {
            break;
        }
    }
}

SmartForest* SmartForest::getBranch(UChar32 c) {
    auto it = branches.find(c);
    return it != branches.end() ? it->second.get() : nullptr;
}

SmartForest* SmartForest::getBranch(const std::string& keyWord) {
    std::vector<Rune> runes = utf8_to_runes(keyWord);
    return getBranch(runes);
}

SmartForest* SmartForest::getBranch(const std::vector<Rune>& runes) {
    SmartForest* tempBranch = this;

    for (const auto& rune : runes) {
        if (!tempBranch) {
            return nullptr;
        }
        tempBranch = tempBranch->getBranch(rune.cp);
    }

    return tempBranch;
}

std::unique_ptr<SmartGetWord> SmartForest::getWord(const std::string& str) {
    return std::make_unique<SmartGetWord>(this, str);
}

std::unique_ptr<SmartGetWord> SmartForest::getWord(const std::vector<Rune>& runes) {
    return std::make_unique<SmartGetWord>(this, runes);
}

void SmartForest::remove(const std::string& word) {
    SmartForest* node = getBranch(word);
    if (node) {
        node->setStatus(CONTINUE);
        node->setParam(ParamType {});
    }
}

void SmartForest::clear() {
    branches.clear();
    branch = nullptr;
}

void SmartForest::print(int depth) const {
    std::string indent(depth * 2, ' ');
    char utf8_buffer[4];
    int32_t utf8_len = 0;
    U8_APPEND_UNSAFE(utf8_buffer, utf8_len, c_);
    std::string char_str(utf8_buffer, utf8_len);

    std::cout << indent << "Node: '" << char_str << "' status=" << static_cast<int>(status_)
              << " param_size=" << param_.size() << std::endl;

    for (const auto& [c, child_branch] : branches) {
        if (child_branch) {
            child_branch->print(depth + 1);
        }
    }
}

} // namespace doris::segment_v2::inverted_index
