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

#include <CLucene.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "CLucene/LuceneThreads.h"
#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Hit.h"

namespace doris::segment_v2 {
class Dictionary {
private:
    // Published only after every dictionary has loaded, so a failed load leaves nothing behind and
    // the next initial() tries again. With std::call_once the first attempt was final even when it
    // failed: an IK analyzer created while the dictionary path was wrong, for example a path fixed
    // later in be.conf, kept failing until the BE restarted, and a unit test that loaded from a bad
    // path broke every IK test that gtest happened to run after it.
    static std::atomic<Dictionary*> singleton_;
    static std::mutex init_mutex_;

    // Dictionary segment mappings
    std::unique_ptr<DictSegment> main_dict_;
    std::unique_ptr<DictSegment> quantifier_dict_;
    std::unique_ptr<DictSegment> stop_words_;
    std::unique_ptr<Configuration> config_;
    bool load_ext_dict_;
    class Cleanup {
    public:
        ~Cleanup() { delete Dictionary::singleton_.exchange(nullptr); }
    };
    static Cleanup cleanup_;

    // Dictionary paths
    static const std::string PATH_DIC_MAIN;
    static const std::string PATH_DIC_QUANTIFIER;
    static const std::string PATH_DIC_STOP;

    explicit Dictionary(const Configuration& cfg, bool use_ext_dict = false);

    void loadMainDict();
    void loadStopWordDict();
    void loadQuantifierDict();

    void loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
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
    // wins. Throws CL_ERR_IllegalState when loading fails, and the next call tries again.
    static void initial(const Configuration& cfg, bool useExtDict = false);

    static Dictionary* getSingleton() {
        Dictionary* dictionary = singleton_.load(std::memory_order_acquire);
        if (dictionary == nullptr) {
            _CLTHROWA(CL_ERR_IllegalState, "Dictionary not initialized");
        }
        return dictionary;
    }

    Configuration* getConfiguration() const { return config_.get(); }

    Hit matchInMainDict(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                        size_t length);
    Hit matchInQuantifierDict(const CharacterUtil::TypedRuneArray& typed_runes,
                              size_t unicode_offset, size_t length);
    void matchWithHit(const CharacterUtil::TypedRuneArray& typed_runes, size_t current_index,
                      Hit& hit);
    bool isStopWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                    size_t length);

    void printStats() const;
};

inline std::atomic<Dictionary*> Dictionary::singleton_ {nullptr};
inline std::mutex Dictionary::init_mutex_;

inline const std::string Dictionary::PATH_DIC_MAIN = "main.dic";
inline const std::string Dictionary::PATH_DIC_QUANTIFIER = "quantifier.dic";
inline const std::string Dictionary::PATH_DIC_STOP = "stopword.dic";
inline Dictionary::Cleanup Dictionary::cleanup_;
} // namespace doris::segment_v2
