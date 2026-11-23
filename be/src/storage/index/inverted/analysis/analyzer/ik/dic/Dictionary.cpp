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

#include "storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.h"

#include <fstream>

#include "storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.h"

namespace doris::segment_v2 {

Dictionary::Dictionary(const Configuration& cfg, bool use_ext_dict)
        : config_(std::make_unique<Configuration>(cfg)),
          load_ext_dict_(use_ext_dict),
          main_dict_(loadMainDict()),
          quantifier_dict_(loadQuantifierDict()),
          stop_words_(loadStopWordDict()) {}

void Dictionary::loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
                              const std::string& dict_name) {
    std::ifstream in(file_path);
    if (!in.good()) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "{} dictionary file not found: {}", dict_name, file_path);
    }

    std::string line;
    while (in.good() && !in.eof()) {
        std::getline(in, line);
        if (line.empty() || line[0] == '#') {
            continue;
        }
        dict->fillSegment(line.c_str());
    }
}

FlatDict Dictionary::loadMainDict() const {
    try {
        DictSegment main_dict(0);
        loadDictFile(&main_dict, config_->getDictPath() + "/" + config_->getMainDictFile(), true,
                     "Main Dict");

        // Load extension dictionaries - can fail
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtDictFiles()) {
                try {
                    loadDictFile(&main_dict, config_->getDictPath() + "/" + extDict, false,
                                 "Extra Dict");
                } catch (const Exception& e) {
                    // Extension dictionary loading failure is logged but doesn't affect main functionality
                    LOG(WARNING) << "Failed to load extension dictionary " << extDict << ": "
                                 << e.what();
                }
            }
        }
        return FlatDict(main_dict);
    } catch (const Exception& e) {
        LOG(ERROR) << "Failed to load main dictionary: " << e.what();
        throw;
    }
}

FlatDict Dictionary::loadStopWordDict() const {
    try {
        DictSegment stop_words(0);
        loadDictFile(&stop_words, config_->getDictPath() + "/" + config_->getStopWordDictFile(),
                     false, "Stopword");

        // Load extension stopword dictionaries
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtStopWordDictFiles()) {
                try {
                    loadDictFile(&stop_words, config_->getDictPath() + "/" + extDict, false,
                                 "Extra Stopword");
                } catch (const Exception& e) {
                    // Extension stopword loading failure is just logged
                    LOG(WARNING) << "Failed to load extension stop word dictionary " << extDict
                                 << ": " << e.what();
                }
            }
        }
        return FlatDict(stop_words);
    } catch (const std::exception& e) {
        // Catch any other unexpected exceptions
        LOG(ERROR) << "Unexpected error loading stopword dictionary: " << e.what();
        throw;
    }
}

FlatDict Dictionary::loadQuantifierDict() const {
    try {
        DictSegment quantifier_dict(0);
        loadDictFile(&quantifier_dict,
                     config_->getDictPath() + "/" + config_->getQuantifierDictFile(), true,
                     "Quantifier");
        return FlatDict(quantifier_dict);
    } catch (const Exception& e) {
        LOG(ERROR) << "Failed to load quantifier dictionary: " << e.what();
        throw;
    }
}

void Dictionary::initial(const Configuration& cfg, bool useExtDict) {
    if (singleton_.load(std::memory_order_acquire) != nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(init_mutex_);
    if (singleton_.load(std::memory_order_relaxed) != nullptr) {
        return;
    }
    std::unique_ptr<Dictionary> dictionary;
    try {
        dictionary.reset(new Dictionary(cfg, useExtDict));
    } catch (const Exception& e) {
        LOG(ERROR) << "Dictionary loading failed: " << e.what();
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Dictionary initialization failed");
    }
    singleton_.store(dictionary.release(), std::memory_order_release);
}

} // namespace doris::segment_v2
