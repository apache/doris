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

#include "itoken_extractor.h"

#include <fstream>

#include "io/fs/path.h"
#include "util/simd/vstring_function.h"
#include "util/string_util.h"
#include "util/trie.h"
#include "vec/common/string_utils/string_utils.h"

namespace doris {

bool NgramTokenExtractor::next_in_string(const char* data, size_t length, size_t* __restrict pos,
                                         size_t* __restrict token_start,
                                         size_t* __restrict token_length) const {
    *token_start = *pos;
    *token_length = 0;
    size_t code_points = 0;
    for (; code_points < n && *token_start + *token_length < length; ++code_points) {
        size_t sz = get_utf8_byte_length(static_cast<uint8_t>(data[*token_start + *token_length]));
        *token_length += sz;
    }
    *pos += get_utf8_byte_length(static_cast<uint8_t>(data[*pos]));
    return code_points == n;
}

bool NgramTokenExtractor::next_in_string_like(const char* data, size_t length, size_t* pos,
                                              std::string& token) const {
    token.clear();

    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = *pos; i < length;) {
        if (escaped && (data[i] == '%' || data[i] == '_' || data[i] == '\\')) {
            token += data[i];
            ++code_points;
            escaped = false;
            ++i;
        } else if (!escaped && (data[i] == '%' || data[i] == '_')) {
            /// This token is too small, go to the next.
            token.clear();
            code_points = 0;
            escaped = false;
            *pos = ++i;
        } else if (!escaped && data[i] == '\\') {
            escaped = true;
            ++i;
        } else {
            const size_t sz = get_utf8_byte_length(static_cast<uint8_t>(data[i]));
            for (size_t j = 0; j < sz; ++j) {
                token += data[i + j];
            }
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n) {
            *pos += get_utf8_byte_length(static_cast<uint8_t>(data[*pos]));
            return true;
        }
    }

    return false;
}

bool AlphaNumTokenExtractor::next_in_string(const char* data, size_t length, size_t* __restrict pos,
                                            size_t* __restrict token_start,
                                            size_t* __restrict token_length) const {
    *token_start = *pos;
    *token_length = 0;
    while (*pos < length) {
        if (!is_alpha_numeric_ascii(data[*pos])) {
            /// Finish current token if any
            if (*token_length > 0) {
                return true;
            }
            *token_start = ++*pos;
        } else {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++*pos;
            ++*token_length;
        }
    }
    return *token_length > 0;
}

bool AlphaNumTokenExtractor::next_in_string_like(const char* data, size_t length, size_t* pos,
                                                 std::string& token) const {
    token.clear();
    bool bad_token = false; // % or _ before token
    bool escaped = false;
    while (*pos < length) {
        if (!escaped && (data[*pos] == '%' || data[*pos] == '_')) {
            token.clear();
            bad_token = true;
            ++*pos;
        } else if (!escaped && data[*pos] == '\\') {
            escaped = true;
            ++*pos;
        } else if (is_ascii(data[*pos]) && !is_alpha_numeric_ascii(data[*pos])) {
            if (!bad_token && !token.empty()) {
                return true;
            }
            token.clear();
            bad_token = false;
            escaped = false;
            ++*pos;
        } else {
            const size_t sz = get_utf8_byte_length(static_cast<uint8_t>(data[*pos]));
            for (size_t j = 0; j < sz; ++j) {
                token += data[*pos];
                ++*pos;
            }
            escaped = false;
        }
    }
    return !bad_token && !token.empty();
}

ChineseTokenDict::ChineseTokenDict(const std::string& dict_path) {
    static_node_infos = load_dict(io::Path(dict_path) / config::inverted_index_dict_file_name);
    trie = new Trie(static_node_infos);
}

std::vector<Unicode> ChineseTokenDict::load_dict(const std::string& file_path) {
    std::vector<Unicode> result;

    std::ifstream ifs(file_path.c_str());
    if (!ifs.is_open()) {
        LOG(FATAL) << "chinese dictionary file:" << file_path << " open error:" << strerror(errno);
    }
    std::string line;
    std::vector<std::string> buf;

    Unicode node_info;
    while (getline(ifs, line)) {
        buf = split(line, " ");
        DCHECK(buf.size() == 3) << "split result illegal, line:" << line;
        make_node_info(node_info, buf[0]);
        result.push_back(node_info);
    }
    return result;
}

template <InvertedIndexParserMode parser_mode>
bool ChineseTokenExtractor<parser_mode>::next_in_string(const char* data, size_t length,
                                                        size_t* __restrict pos,
                                                        size_t* __restrict token_start,
                                                        size_t* __restrict token_length) const {
    if (_runes.empty() || *pos == 0) {
        reset();
        if (!decode_runes_in_string(data, length, &_runes)) {
            reset();
            return false;
        }
        _currentIter = _runes.begin();
        _endIter = _runes.end();
    }

    std::string_view word;
    if constexpr (parser_mode == InvertedIndexParserMode::FINE_GRAIN) {
        if (cut_all(data, word)) {
            *token_start = word.data() - data;
            *token_length = word.size();
            *pos = *token_start + *token_length;
            return true;
        }
    } else {
        if (cut(data, word)) {
            *token_start = word.data() - data;
            *token_length = word.size();
            *pos = *token_start + *token_length;
            return true;
        }
    }
    return false;
}

template <InvertedIndexParserMode parser_mode>
bool ChineseTokenExtractor<parser_mode>::cut(const char* sentence, std::string_view& word) const {
    if (_currentIter >= _endIter) {
        return false;
    }
    const Unicode* longestWord = _dict->trie->find(_currentIter, _endIter);
    if (longestWord) {
        word = get_string_view_from_runes(sentence, _currentIter,
                                          _currentIter + longestWord->size() - 1);
        _currentIter += longestWord->size();
    } else {
        // Handle ASCII alpha-numeric characters
        auto currentIter = _currentIter;
        while (currentIter != _endIter && is_alpha_numeric_ascii(sentence[currentIter->offset])) {
            currentIter++;
        }

        if (currentIter != _currentIter) {
            word = get_string_view_from_runes(sentence, _currentIter, currentIter - 1);
            _currentIter = currentIter;
        } else {
            // If it's neither a Chinese word nor an ASCII word, just move one rune forward.
            word = get_string_view_from_runes(sentence, _currentIter, _currentIter);
            _currentIter++;
        }
    }
    return true;
}

template <InvertedIndexParserMode parser_mode>
bool ChineseTokenExtractor<parser_mode>::cut_all(const char* sentence,
                                                 std::string_view& word) const {
    if (_currentIter >= _endIter && _matchedWordIndex >= _matchedWords.size()) {
        return false;
    }

    // If there are no matched words in cache or we've returned all cached words,
    // try to find new matches from the current position
    if (_matchedWords.empty()) {
        find(_currentIter, _endIter, _matchedWords);
        _matchedWordIndex = 0; // Reset the index
    }

    // Return the next matched word from the cache
    WordRange nextWord = _matchedWords[_matchedWordIndex++];
    word = get_string_view_from_runes(sentence, nextWord.left, nextWord.right);
    _currentIter = nextWord.right + 1;

    return true;
}

template <InvertedIndexParserMode parser_mode>
void ChineseTokenExtractor<parser_mode>::find(RuneStrArray::const_iterator begin,
                                              RuneStrArray::const_iterator end,
                                              std::vector<WordRange>& res) const {
    assert(_dict->trie);
    std::vector<struct Dag> dags(end - begin);
    _dict->trie->find(begin, end, dags);
    size_t maxIdx = 0;

    for (size_t i = 0, uIdx = 0; i < dags.size() && begin + i != end; ++i, ++uIdx) {
        if (dags[i].nexts.empty()) {
            // Check for consecutive ASCII alpha-numeric characters
            auto currentIter = begin + uIdx;
            while (currentIter != end && currentIter->rune < 0x80 &&
                   is_alpha_numeric_ascii(currentIter->rune)) {
                currentIter++;
            }

            if (currentIter != begin + uIdx) {
                res.emplace_back(WordRange(begin + uIdx, currentIter - 1));
                i += std::distance(begin + uIdx, currentIter) - 1; // Update the i
                uIdx = i;                                          // Update the uIdx
            } else {
                res.emplace_back(WordRange(begin + uIdx, begin + uIdx));
            }
            continue;
        }

        for (size_t j = 0; j < dags[i].nexts.size(); ++j) {
            auto nextoffset = dags[i].nexts[j];
            size_t wordLen = nextoffset - i + 1;

            if (wordLen >= 2 || (dags[i].nexts.size() == 1 && maxIdx <= uIdx)) {
                res.emplace_back(WordRange(begin + i, begin + nextoffset));
            }
            if (wordLen + uIdx > maxIdx) {
                maxIdx = uIdx + wordLen;
            }
        }
    }
}

template class ChineseTokenExtractor<InvertedIndexParserMode::FINE_GRAIN>;
template class ChineseTokenExtractor<InvertedIndexParserMode::COARSE_GRAIN>;

} // namespace doris
