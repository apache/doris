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

#include "DictSegment.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

DictSegment::DictSegment(int32_t key_char) : key_char_(key_char) {
    children_array_.reserve(ARRAY_LENGTH_LIMIT);
}

bool DictSegment::hasNextNode() const {
    return store_size_ > 0;
}

Hit DictSegment::match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                       size_t length) {
    Hit search_hit;
    search_hit.setByteBegin(typed_runes[unicode_offset].offset);
    search_hit.setCharBegin(unicode_offset);
    match(typed_runes, unicode_offset, length, search_hit);
    return search_hit;
}

void DictSegment::match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                        size_t length, Hit& search_hit) {
    search_hit.setUnmatch();

    if (!typed_runes.empty() && unicode_offset < typed_runes.size()) {
        search_hit.setByteEnd(typed_runes[unicode_offset].offset +
                              typed_runes[unicode_offset].getByteLength());
        search_hit.setCharEnd(unicode_offset);
    }

    length = (length == 0 || unicode_offset + length > typed_runes.size())
                     ? typed_runes.size() - unicode_offset
                     : length;

    int32_t ch = typed_runes[unicode_offset].getChar();
    DictSegment* ds = nullptr;
    if (store_size_ <= ARRAY_LENGTH_LIMIT) {
        for (size_t i = 0; i < store_size_ && i < children_array_.size(); i++) {
            if (children_array_[i]->key_char_ == ch) {
                ds = children_array_[i].get();
                break;
            }
        }
    } else {
        auto it = children_map_.find(ch);
        if (it != children_map_.end()) {
            ds = it->second.get();
        } else {
            return;
        }
    }

    if (ds) {
        if (length == 1) {
            if (ds->node_state_ == 1) {
                search_hit.setMatch();
            }
            if (ds->hasNextNode()) {
                search_hit.setPrefix();
                search_hit.setMatchedDictSegment(ds);
            }
        } else if (length > 1) {
            ds->match(typed_runes, unicode_offset + 1, length - 1, search_hit);
        }
    }
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
    current->node_state_ = 1;
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

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
