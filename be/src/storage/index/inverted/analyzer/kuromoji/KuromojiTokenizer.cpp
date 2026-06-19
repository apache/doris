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

#include "storage/index/inverted/analyzer/kuromoji/KuromojiTokenizer.h"

#include <algorithm>
#include <string_view>

#include "storage/index/inverted/analyzer/kuromoji/kuromoji_normalize.h"

namespace doris::segment_v2 {

namespace {
// Number of bytes in the UTF-8 sequence whose lead byte is `c`.
inline int utf8_len(unsigned char c) {
    if (c < 0x80) {
        return 1;
    }
    if ((c >> 5) == 0x6) {
        return 2;
    }
    if ((c >> 4) == 0xE) {
        return 3;
    }
    if ((c >> 3) == 0x1E) {
        return 4;
    }
    return 1; // invalid lead byte: treat as a single byte
}

inline bool is_ascii_space(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

// Returns the idx-th comma-separated field of an IPADIC feature string
// (0=POS1 ... 6=base form, 7=reading, 8=pronunciation), or empty.
std::string_view feature_field(std::string_view feat, int idx) {
    int cur = 0;
    std::size_t start = 0;
    for (std::size_t i = 0; i <= feat.size(); ++i) {
        if (i == feat.size() || feat[i] == ',') {
            if (cur == idx) {
                return feat.substr(start, i - start);
            }
            ++cur;
            start = i + 1;
        }
    }
    return {};
}

// Part-of-speech (POS1) classes dropped for full-text search. A coarse subset of
// Lucene/OpenSearch's ja stoptags: particles, auxiliary verbs, conjunctions,
// symbols, fillers. (Full stoptags fidelity is a later refinement.)
bool is_stop_pos(std::string_view pos1) {
    return pos1 == "\xE5\x8A\xA9\xE8\xA9\x9E" ||             // 助詞 (particle)
           pos1 == "\xE5\x8A\xA9\xE5\x8B\x95\xE8\xA9\x9E" || // 助動詞 (auxiliary verb)
           pos1 == "\xE6\x8E\xA5\xE7\xB6\x9A\xE8\xA9\x9E" || // 接続詞 (conjunction)
           pos1 == "\xE8\xA8\x98\xE5\x8F\xB7" ||             // 記号 (symbol)
           pos1 == "\xE3\x83\x95\xE3\x82\xA3\xE3\x83\xA9\xE3\x83\xBC"; // フィラー (filler)
}

void ascii_lower(std::string& s) {
    for (char& c : s) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
}
} // namespace

KuromojiTokenizer::KuromojiTokenizer(KuromojiMode mode, bool lower_case, bool own_reader,
                                     const kuromoji::KuromojiDictionary* dict)
        : mode_(mode), dict_(dict) {
    this->lowercase = lower_case;
    this->ownReader = own_reader;
}

void KuromojiTokenizer::reset(lucene::util::Reader* reader) {
    this->input = reader;
    buffer_index_ = 0;
    data_length_ = 0;
    tokens_text_.clear();

    // Read the entire input. readCopy returns the count read, or <= 0 at EOF.
    std::string text;
    char buf[4096];
    int32_t n = 0;
    while ((n = reader->readCopy(buf, 0, static_cast<int32_t>(sizeof(buf)))) > 0) {
        text.append(buf, n);
    }

    if (dict_ != nullptr) {
        // Viterbi morphological segmentation, then OpenSearch-default-style filtering:
        // drop stop part-of-speech (particles/auxiliaries/...), emit the dictionary
        // base form for conjugated words, and lowercase embedded ASCII.
        kuromoji::KuromojiViterbi viterbi(*dict_);
        std::vector<kuromoji::KuromojiMorpheme> morphemes;
        viterbi.segment(text, &morphemes);
        tokens_text_.reserve(morphemes.size());
        for (const auto& m : morphemes) {
            const std::string_view feat =
                    m.known ? dict_->feature(dict_->word(m.word_id))
                            : dict_->unknown_feature(dict_->unknown_word(m.word_id));
            if (is_stop_pos(feature_field(feat, 0))) {
                continue; // part-of-speech stop filtering
            }
            const std::string_view base = feature_field(feat, 6);
            std::string term = (base.empty() || base == "*")
                                       ? text.substr(m.byte_start, m.byte_len)
                                       : std::string(base);
            term = kuromoji::cjk_width_normalize(term); // full-width ASCII -> ASCII before lowercase
            if (this->lowercase) {
                ascii_lower(term);
            }
            if (!term.empty()) {
                tokens_text_.push_back(std::move(term));
            }
        }
    } else {
        // Fallback (no dictionary wired in yet): CJK per-codepoint unigram split,
        // skipping ASCII whitespace.
        for (size_t i = 0; i < text.size();) {
            int len = utf8_len(static_cast<unsigned char>(text[i]));
            if (i + static_cast<size_t>(len) > text.size()) {
                len = 1; // truncated tail: emit a single byte
            }
            if (!(len == 1 && is_ascii_space(static_cast<unsigned char>(text[i])))) {
                tokens_text_.emplace_back(text.substr(i, len));
            }
            i += len;
        }
    }
    data_length_ = static_cast<int32_t>(tokens_text_.size());
}

Token* KuromojiTokenizer::next(Token* token) {
    if (buffer_index_ >= data_length_) {
        return nullptr;
    }
    std::string& token_text = tokens_text_[buffer_index_++];
    // reset() already segmented and normalized the terms; hand them out one at a
    // time, capped at the CLucene maximum term length.
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    token->setNoCopy(token_text.data(), 0, static_cast<int32_t>(size));
    return token;
}

} // namespace doris::segment_v2
