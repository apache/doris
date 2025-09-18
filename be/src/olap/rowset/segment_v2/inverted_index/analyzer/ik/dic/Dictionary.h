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

#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "CLucene/LuceneThreads.h"
#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "DictSegment.h"
#include "Hit.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/cfg/Configuration.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/core/CharacterUtil.h"

namespace doris::segment_v2 {
class Dictionary {
private:
    static Dictionary* singleton_;
    static std::once_flag init_flag_;
    static bool init_success_;

    // Dictionary segment mappings
    std::unique_ptr<DictSegment> main_dict_;
    std::unique_ptr<DictSegment> quantifier_dict_;
    std::unique_ptr<DictSegment> stop_words_;
    std::unique_ptr<Configuration> config_;
    bool load_ext_dict_;
    class Cleanup {
    public:
        ~Cleanup() {
            if (Dictionary::singleton_) {
                delete Dictionary::singleton_;
                Dictionary::singleton_ = nullptr;
                Dictionary::init_success_ = false;
            }
        }
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
    static void destroy() {
        if (singleton_) {
            delete singleton_;
            singleton_ = nullptr;
            init_success_ = false;
        }
    }
    ~Dictionary() {}

    static void initial(const Configuration& cfg, bool useExtDict = false) {
        getSingleton(cfg, useExtDict);
    }

    static Dictionary* getSingleton() {
        if (!singleton_) {
            _CLTHROWA(CL_ERR_IllegalState, "Dictionary not initialized");
        }
        // Just log warning if initialization failed, but still return the object
        // This allows clients to use the object for reloading
        if (!init_success_) {
            LOG(WARNING) << "Dictionary initialization failed previously, the object may not work "
                            "properly";
        }
        return singleton_;
    }

    static Dictionary* getSingleton(const Configuration& cfg, bool useExtDict = false) {
        std::call_once(init_flag_, [&]() {
            try {
                singleton_ = new Dictionary(cfg, useExtDict);
                // Try to load dictionaries
                try {
                    singleton_->loadMainDict();
                    singleton_->loadQuantifierDict();
                    singleton_->loadStopWordDict();
                    // Set success flag if all operations succeed
                    init_success_ = true;
                } catch (const CLuceneError& e) {
                    // Dictionary loading failed, but object was created
                    init_success_ = false;
                    LOG(ERROR) << "Dictionary loading failed: " << e.what();
                    // Keep the object for possible reload
                }
            } catch (std::bad_alloc& e) {
                // Memory allocation failure
                LOG(ERROR) << "Failed to allocate memory for Dictionary: " << e.what();
                // Let exception propagate to indicate critical failure
                throw;
            } catch (...) {
                // Handle other exceptions during object creation
                LOG(ERROR) << "Unknown error during Dictionary object creation";
                // Let exception propagate to indicate critical failure
                throw;
            }
        });

        // Check initialization result
        if (!singleton_ || !init_success_) {
            _CLTHROWA(CL_ERR_IllegalState, "Dictionary initialization failed");
        }

        return singleton_;
    }

    Configuration* getConfiguration() const { return config_.get(); }

    static void reload();

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

inline Dictionary* Dictionary::singleton_ = nullptr;
inline std::once_flag Dictionary::init_flag_;
inline bool Dictionary::init_success_ = false;

inline const std::string Dictionary::PATH_DIC_MAIN = "main.dic";
inline const std::string Dictionary::PATH_DIC_QUANTIFIER = "quantifier.dic";
inline const std::string Dictionary::PATH_DIC_STOP = "stopword.dic";
inline Dictionary::Cleanup Dictionary::cleanup_;
} // namespace doris::segment_v2
