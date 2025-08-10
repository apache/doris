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

#include "Dictionary.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

Dictionary::Dictionary(const Configuration& cfg, bool use_ext_dict)
        : main_dict_(std::make_unique<DictSegment>(0)),
          quantifier_dict_(std::make_unique<DictSegment>(0)),
          stop_words_(std::make_unique<DictSegment>(0)),
          config_(std::make_unique<Configuration>(cfg)),
          load_ext_dict_(use_ext_dict) {}

void Dictionary::loadDictFile(DictSegment* dict, const std::string& file_path, bool critical,
                              const std::string& dict_name) {
    std::ifstream in(file_path);
    if (!in.good()) {
        _CLTHROWA(CL_ERR_IO, (dict_name + " dictionary file not found: " + file_path).c_str());
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

void Dictionary::loadMainDict() {
    try {
        loadDictFile(main_dict_.get(), config_->getDictPath() + "/" + config_->getMainDictFile(),
                     true, "Main Dict");

        // Load extension dictionaries - can fail
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtDictFiles()) {
                try {
                    loadDictFile(main_dict_.get(), config_->getDictPath() + "/" + extDict, false,
                                 "Extra Dict");
                } catch (const CLuceneError& e) {
                    // Extension dictionary loading failure is logged but doesn't affect main functionality
                    LOG(WARNING) << "Failed to load extension dictionary " << extDict << ": "
                                 << e.what();
                }
            }
        }
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "Failed to load main dictionary: " << e.what();
        throw;
    }
}

void Dictionary::loadStopWordDict() {
    try {
        loadDictFile(stop_words_.get(),
                     config_->getDictPath() + "/" + config_->getStopWordDictFile(), false,
                     "Stopword");

        // Load extension stopword dictionaries
        if (load_ext_dict_) {
            for (const auto& extDict : config_->getExtStopWordDictFiles()) {
                try {
                    loadDictFile(stop_words_.get(), config_->getDictPath() + "/" + extDict, false,
                                 "Extra Stopword");
                } catch (const CLuceneError& e) {
                    // Extension stopword loading failure is just logged
                    LOG(WARNING) << "Failed to load extension stop word dictionary " << extDict
                                 << ": " << e.what();
                }
            }
        }
    } catch (const std::exception& e) {
        // Catch any other unexpected exceptions
        LOG(ERROR) << "Unexpected error loading stopword dictionary: " << e.what();
        throw;
    }
}

void Dictionary::loadQuantifierDict() {
    try {
        loadDictFile(quantifier_dict_.get(),
                     config_->getDictPath() + "/" + config_->getQuantifierDictFile(), true,
                     "Quantifier");
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "Failed to load quantifier dictionary: " << e.what();
        throw;
    }
}

void Dictionary::reload() {
    if (!singleton_) {
        // Singleton doesn't exist, can't reload
        _CLTHROWA(CL_ERR_IllegalState, "Dictionary not initialized, cannot reload");
    }

    try {
        // Try to reload all dictionaries
        singleton_->loadMainDict();
        singleton_->loadStopWordDict();
        singleton_->loadQuantifierDict();

        init_success_ = true;
        LOG(INFO) << "Dictionary reloaded successfully";
    } catch (const CLuceneError& e) {
        LOG(ERROR) << "Failed to reload dictionary: " << e.what();

        throw;
    }
}

Hit Dictionary::matchInMainDict(const CharacterUtil::TypedRuneArray& typed_runes,
                                size_t unicode_offset, size_t length) {
    Hit result = main_dict_->match(typed_runes, unicode_offset, length);

    if (!result.isUnmatch()) {
        result.setByteBegin(typed_runes[unicode_offset].offset);
        result.setCharBegin(unicode_offset);
        result.setByteEnd(typed_runes[unicode_offset + length - 1].getNextBytePosition());
        result.setCharEnd(unicode_offset + length);
    }
    return result;
}

Hit Dictionary::matchInQuantifierDict(const CharacterUtil::TypedRuneArray& typed_runes,
                                      size_t unicode_offset, size_t length) {
    Hit result = quantifier_dict_->match(typed_runes, unicode_offset, length);

    if (!result.isUnmatch()) {
        result.setByteBegin(typed_runes[unicode_offset].offset);
        result.setCharBegin(unicode_offset);
        result.setByteEnd(typed_runes[unicode_offset + length - 1].getNextBytePosition());
        result.setCharEnd(unicode_offset + length);
    }
    return result;
}

void Dictionary::matchWithHit(const CharacterUtil::TypedRuneArray& typed_runes,
                              size_t current_index, Hit& hit) {
    if (auto* matchedSegment = hit.getMatchedDictSegment()) {
        matchedSegment->match(typed_runes, current_index, 1, hit);
        return;
    }
    hit.setUnmatch();
}

bool Dictionary::isStopWord(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
                            size_t length) {
    if (typed_runes.empty() || unicode_offset >= typed_runes.size()) {
        return false;
    }

    Hit result = stop_words_->match(typed_runes, unicode_offset, length);

    return result.isMatch();
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
