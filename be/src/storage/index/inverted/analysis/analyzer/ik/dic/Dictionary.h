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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/FlatDict.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Hit.h"

namespace doris::segment_v2 {

class DictSegment;

// The main, quantifier and stop word dictionaries, loaded once per process. Each file is loaded
// into a DictSegment trie, copied into a FlatDict and freed, so a loaded Dictionary keeps only the
// flat copies. A segmenter is given the loaded Dictionary when it is made and reads it through
// that reference: the per-character path never goes through getSingleton().
class Dictionary {
private:
    // Published only after every dictionary has loaded, so a failed load leaves nothing behind and
    // the next initial() tries again. With std::call_once the first attempt was final even when it
    // failed: an IK analyzer created while the dictionary path was wrong, for example a path fixed
    // later in be.conf, kept failing until the BE restarted, and a unit test that loaded from a bad
    // path broke every IK test that gtest happened to run after it.
    static std::atomic<Dictionary*> singleton_;
    static std::mutex init_mutex_;

    std::unique_ptr<Configuration> config_;
    bool load_ext_dict_;
    // Built in this order by the constructor, after config_ and load_ext_dict_.
    FlatDict main_dict_;
    FlatDict quantifier_dict_;
    FlatDict stop_words_;

    class Cleanup {
    public:
        ~Cleanup() { delete Dictionary::singleton_.exchange(nullptr); }
    };
    static Cleanup cleanup_;

    // Dictionary paths
    static const std::string PATH_DIC_MAIN;
    static const std::string PATH_DIC_QUANTIFIER;
    static const std::string PATH_DIC_STOP;

    // Loads every dictionary; throws when one of the three files cannot be read, while a missing
    // extension file is only logged.
    explicit Dictionary(const Configuration& cfg, bool use_ext_dict = false);

    FlatDict loadMainDict() const;
    FlatDict loadStopWordDict() const;
    FlatDict loadQuantifierDict() const;

    static void loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
                             const std::string& dict_name);

    Dictionary(const Dictionary&) = delete;
    Dictionary& operator=(const Dictionary&) = delete;

public:
    // Only for tests, and only while no IK stream is alive.
    static void destroy() {
        std::lock_guard<std::mutex> lock(init_mutex_);
        delete singleton_.exchange(nullptr);
    }
    ~Dictionary() = default;

    // Loads the dictionaries unless they are loaded already; the first successful configuration
    // wins. Throws INVERTED_INDEX_ANALYZER_ERROR when loading fails, and the next call tries
    // again.
    static void initial(const Configuration& cfg, bool useExtDict = false);

    static Dictionary* getSingleton() {
        Dictionary* dictionary = singleton_.load(std::memory_order_acquire);
        if (dictionary == nullptr) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, "Dictionary not initialized");
        }
        return dictionary;
    }

    Configuration* getConfiguration() const { return config_.get(); }

    // A hit of the `length` characters from `unicode_offset`; a match or a prefix spans them,
    // which makes its char end unicode_offset + length, one past the last of them: 中 matched
    // alone at index 5 ends at 6. The CJK segmenter gives that end to the lexeme of the character
    // and the quantifier segmenter continues the hit from it, and both decide tokens.
    Hit matchInMainDict(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                        size_t length) const {
        return spanHit(main_dict_.match(typed_runes, unicode_offset, length), typed_runes,
                       unicode_offset, length);
    }
    Hit matchInQuantifierDict(const CharacterUtil::TypedRuneArray& typed_runes,
                              size_t unicode_offset, size_t length) const {
        return spanHit(quantifier_dict_.match(typed_runes, unicode_offset, length), typed_runes,
                       unicode_offset, length);
    }
    // Continues a prefix `hit` with the character at `current_index`, after which the hit's char
    // end is `current_index` itself.
    void matchWithHit(const CharacterUtil::TypedRuneArray& typed_runes, size_t current_index,
                      Hit& hit) const {
        DCHECK(hit.getMatchedDict() != nullptr);
        hit.getMatchedDict()->match(typed_runes, current_index, 1, hit.getMatchedNode(), hit);
    }
    bool isStopWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                    size_t length) const {
        return stop_words_.isWord(typed_runes, unicode_offset, length);
    }

private:
    static Hit spanHit(Hit result, const CharacterUtil::TypedRuneArray& typed_runes,
                       size_t unicode_offset, size_t length) {
        if (!result.isUnmatch()) {
            result.setByteBegin(typed_runes[unicode_offset].offset);
            result.setCharBegin(unicode_offset);
            result.setByteEnd(typed_runes[unicode_offset + length - 1].getNextBytePosition());
            result.setCharEnd(unicode_offset + length);
        }
        return result;
    }
};

inline std::atomic<Dictionary*> Dictionary::singleton_ {nullptr};
inline std::mutex Dictionary::init_mutex_;

inline const std::string Dictionary::PATH_DIC_MAIN = "main.dic";
inline const std::string Dictionary::PATH_DIC_QUANTIFIER = "quantifier.dic";
inline const std::string Dictionary::PATH_DIC_STOP = "stopword.dic";
inline Dictionary::Cleanup Dictionary::cleanup_;
} // namespace doris::segment_v2
