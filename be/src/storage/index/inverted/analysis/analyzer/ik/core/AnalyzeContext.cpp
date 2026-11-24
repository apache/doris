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

#include "storage/index/inverted/analysis/analyzer/ik/core/AnalyzeContext.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>

#include "common/exception.h"

namespace doris::segment_v2 {

AnalyzeContext::AnalyzeContext(Arena& arena, std::shared_ptr<Configuration> config,
                               const Dictionary& dictionary)
        : segment_buff_(),
          typed_runes_(),
          buffer_offset_(0),
          cursor_(0),
          available_(0),
          last_useless_char_num_(0),
          buffer_locker_(0),
          org_lexemes_(arena),
          path_map_(),
          config_(std::move(config)),
          dictionary_(dictionary) {
    segment_buff_.resize(BUFF_SIZE);
    typed_runes_.reserve(BUFF_SIZE);
}

AnalyzeContext::~AnalyzeContext() {
    // Ensure all resources are released during destruction
    for (auto& [_, path] : path_map_) {
        delete path;
    }
    path_map_.clear();
}

void AnalyzeContext::reset() {
    buffer_offset_ = 0;
    segment_buff_.clear();
    buffer_locker_ = 0;
    org_lexemes_.clear();
    available_ = 0;
    buffer_offset_ = 0;
    cursor_ = 0;
    last_useless_char_num_ = 0;
    typed_runes_.clear();

    // Clean up all LexemePath objects in path_map_ to avoid memory leaks
    for (auto& [_, path] : path_map_) {
        delete path;
    }
    path_map_.clear();

    results_.clear();
    results_head_ = 0;
}

size_t AnalyzeContext::fillBuffer(inverted_index::TextCursor& cursor) {
    try {
        int32_t readCount = 0;
        if (buffer_offset_ == 0) {
            readCount = std::max(
                    0, cursor.read_copy(segment_buff_.data(), static_cast<int32_t>(BUFF_SIZE)));
            CharacterUtil::decodeStringToRunes(segment_buff_.data(), readCount, typed_runes_,
                                               config_->isEnableLowercase());
        } else {
            size_t offset = available_ - typed_runes_[cursor_].getNextBytePosition();
            if (offset > 0) {
                std::memmove(segment_buff_.data(),
                             segment_buff_.data() + typed_runes_[cursor_].getNextBytePosition(),
                             offset);
                readCount = std::max(0, cursor.read_copy(segment_buff_.data() + offset,
                                                         static_cast<int32_t>(BUFF_SIZE - offset)));
                readCount += offset;
            } else {
                readCount = std::max(
                        0, cursor.read_copy(segment_buff_.data(), static_cast<int32_t>(BUFF_SIZE)));
            }
            CharacterUtil::decodeStringToRunes(segment_buff_.data(), readCount, typed_runes_,
                                               config_->isEnableLowercase());
        }
        // Ensure readCount is set to 0 in case of
        // an exceptional situation where typed_runes_ is empty.
        if (typed_runes_.size() == 0 && readCount != 0) {
            std::string error_msg = "IK Analyzer: typed_runes_ is empty, but readCount is not 0: ";
            error_msg += std::to_string(readCount) + " " + std::to_string(typed_runes_.size());
            LOG(ERROR) << error_msg;
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, error_msg);
        }

        available_ = readCount;
        cursor_ = 0;
        return readCount;
    } catch (const std::exception& e) {
        // Unified exception handling for all standard exceptions
        std::string error_msg = "IK Analyzer exception during buffer filling: ";
        error_msg += e.what();
        LOG(ERROR) << error_msg;
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, error_msg);
    } catch (...) {
        // Handle unknown exceptions
        std::string error_msg = "IK Analyzer: Unknown error occurred during buffer filling";
        LOG(ERROR) << error_msg;
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR, error_msg);
    }
}

void AnalyzeContext::addLexeme(Lexeme& lexeme) {
    org_lexemes_.addLexeme(lexeme);
}

void AnalyzeContext::addLexemePath(LexemePath* path) {
    if (path) {
        auto begin = path->getPathBegin();
        path_map_.emplace(begin, path);
    }
}

void AnalyzeContext::compound(Lexeme& lexeme) {
    if (!config_->isUseSmart()) {
        return;
    }
    if (hasResult()) {
        if (Lexeme::Type::Arabic == lexeme.getType()) {
            const Lexeme& nextLexeme = results_[results_head_];
            bool appendOk = false;
            if (Lexeme::Type::CNum == nextLexeme.getType()) {
                appendOk = lexeme.append(nextLexeme, Lexeme::Type::CNum);
            } else if (Lexeme::Type::Count == nextLexeme.getType()) {
                appendOk = lexeme.append(nextLexeme, Lexeme::Type::CQuan);
            }
            if (appendOk) {
                ++results_head_;
            }
        }

        if (Lexeme::Type::CNum == lexeme.getType() && hasResult()) {
            const Lexeme& nextLexeme = results_[results_head_];
            bool appendOk = false;
            if (Lexeme::Type::Count == nextLexeme.getType()) {
                appendOk = lexeme.append(nextLexeme, Lexeme::Type::CQuan);
            }
            if (appendOk) {
                ++results_head_;
            }
        }
    }
}

void AnalyzeContext::markBufferOffset() {
    buffer_offset_ += typed_runes_[cursor_].getNextBytePosition();
}

bool AnalyzeContext::getNextLexeme(Lexeme& lexeme) {
    while (hasResult()) {
        Lexeme result = results_[results_head_++];
        compound(result);
        if (!dictionary_.isStopWord(typed_runes_, result.getCharBegin(), result.getCharLength())) {
            lexeme = result;
            return true;
        }
    }
    return false;
}

void AnalyzeContext::outputToResult() {
    // The lexemes of the previous window are all read before the next window is analyzed.
    DCHECK(!hasResult());
    results_.clear();
    results_head_ = 0;
    for (size_t index = 0; index <= cursor_;) {
        if (typed_runes_[index].char_type == CharacterUtil::CharType::USELESS) {
            index++;
            last_useless_char_num_++;
            continue;
        }
        last_useless_char_num_ = 0;
        auto byte_pos = typed_runes_[index].getBytePosition();
        auto pathIter = path_map_.find(byte_pos);
        if (pathIter != path_map_.end()) {
            auto& path = pathIter->second;
            while (auto lexeme = path->pollFirst()) {
                results_.push_back(*lexeme);
                index = lexeme->getCharEnd() + 1;
                auto next_lexeme = path->peekFirst();
                if (next_lexeme) {
                    for (; index < next_lexeme->getCharBegin(); index++) {
                        outputSingleCJK(index);
                    }
                }
            }
        } else {
            outputSingleCJK(index);
            index++;
        }
    }
    for (auto& [_, path] : path_map_) {
        delete path;
    }
    path_map_.clear();
}

void AnalyzeContext::outputSingleCJK(size_t index) {
    if (typed_runes_[index].char_type == CharacterUtil::CharType::CHINESE ||
        typed_runes_[index].char_type == CharacterUtil::CharType::OTHER_CJK) {
        results_.emplace_back(buffer_offset_, typed_runes_[index].offset, typed_runes_[index].len,
                              typed_runes_[index].char_type == CharacterUtil::CharType::CHINESE
                                      ? Lexeme::Type::CNChar
                                      : Lexeme::Type::OtherCJK,
                              index, index);
    }
}

} // namespace doris::segment_v2
