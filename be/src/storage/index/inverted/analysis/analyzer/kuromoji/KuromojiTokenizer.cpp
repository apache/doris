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

#include "storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.h"

#include <algorithm>
#include <string>
#include <string_view>

#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter.h"

namespace doris::segment_v2 {

namespace {
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
    return pos1 == "\xE5\x8A\xA9\xE8\xA9\x9E" ||                       // Particle
           pos1 == "\xE5\x8A\xA9\xE5\x8B\x95\xE8\xA9\x9E" ||           // Auxiliary verb
           pos1 == "\xE6\x8E\xA5\xE7\xB6\x9A\xE8\xA9\x9E" ||           // Conjunction
           pos1 == "\xE8\xA8\x98\xE5\x8F\xB7" ||                       // Symbol
           pos1 == "\xE3\x83\x95\xE3\x82\xA3\xE3\x83\xA9\xE3\x83\xBC"; // Filler
}

// Decode the first UTF-8 code point of text[start, start+len). Validates
// continuation bytes and falls back to the lead byte on malformed input, so it
// classifies the same boundaries the Viterbi decoder (decode_utf8) produced.
char32_t first_codepoint(std::string_view text, uint32_t start, uint32_t len) {
    if (len == 0 || start >= text.size()) {
        return 0;
    }
    const auto b0 = static_cast<unsigned char>(text[start]);
    const std::size_t avail = std::min<std::size_t>(len, text.size() - start);
    auto cont = [&](uint32_t i) {
        return (static_cast<unsigned char>(text[start + i]) & 0xC0U) == 0x80U;
    };
    auto low6 = [&](uint32_t i) { return static_cast<unsigned char>(text[start + i]) & 0x3FU; };
    if (b0 < 0x80) {
        return b0;
    }
    if ((b0 >> 5) == 0x6 && avail >= 2 && cont(1)) {
        return static_cast<char32_t>(((b0 & 0x1FU) << 6) | low6(1));
    }
    if ((b0 >> 4) == 0xE && avail >= 3 && cont(1) && cont(2)) {
        return static_cast<char32_t>(((b0 & 0x0FU) << 12) | (low6(1) << 6) | low6(2));
    }
    if ((b0 >> 3) == 0x1E && avail >= 4 && cont(1) && cont(2) && cont(3)) {
        return static_cast<char32_t>(((b0 & 0x07U) << 18) | (low6(1) << 12) | (low6(2) << 6) |
                                     low6(3));
    }
    return b0;
}
} // namespace

KuromojiTokenizer::KuromojiTokenizer(KuromojiMode mode, bool lowercase,
                                     const inverted_index::kuromoji::KuromojiDictionary* dict)
        : inverted_index::PerTokenTokenizer(TermLifetime::kUntilReset),
          mode_(mode),
          lowercase_(lowercase),
          dict_(dict) {}

void KuromojiTokenizer::on_reset(std::string_view text) {
    buffer_index_ = 0;
    tokens_text_.clear();
    inverted_index::release_oversized_scratch(tokens_text_);

    if (dict_ == nullptr) {
        throw doris::Exception(doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                               "kuromoji tokenizer requires a loaded dictionary");
    }

    // Viterbi morphological segmentation, then OpenSearch-default-style filtering:
    // drop stop part-of-speech (particles/auxiliaries/...), emit the dictionary
    // base form for conjugated words, and lowercase embedded ASCII.
    inverted_index::kuromoji::KuromojiViterbi viterbi(*dict_, mode_);
    std::vector<inverted_index::kuromoji::KuromojiMorpheme> morphemes;
    viterbi.segment(text, &morphemes);
    tokens_text_.reserve(morphemes.size());
    for (const auto& m : morphemes) {
        if (!m.known) {
            const auto cat = dict_->char_category(first_codepoint(text, m.byte_start, m.byte_len));
            if (cat == inverted_index::kuromoji::CharCategory::CAT_SPACE ||
                cat == inverted_index::kuromoji::CharCategory::CAT_SYMBOL) {
                continue;
            }
        }
        const std::string_view feat =
                m.known ? dict_->feature(dict_->word(m.word_id))
                        : dict_->unknown_feature(dict_->unknown_word(m.word_id));
        if (is_stop_pos(feature_field(feat, 0))) {
            continue; // part-of-speech stop filtering
        }
        const std::string_view base = feature_field(feat, 6);
        std::string term = (base.empty() || base == "*")
                                   ? std::string(text.substr(m.byte_start, m.byte_len))
                                   : std::string(base);
        term = inverted_index::kuromoji::cjk_width_normalize(
                term); // full-width ASCII -> ASCII before lowercase
        if (lowercase_) {
            inverted_index::unicode_lower(term); // Unicode-aware, matching LowerCaseFilter
        }
        if (!term.empty()) {
            tokens_text_.push_back(std::move(term));
        }
    }
}

bool KuromojiTokenizer::next_token(inverted_index::TokenRef& token) {
    if (buffer_index_ >= tokens_text_.size()) {
        return false;
    }
    const std::string& token_text = tokens_text_[buffer_index_++];
    // on_reset() already segmented and normalized the terms; hand them out one at a time, capped
    // at MAX_WORD_LEN bytes.
    token = {.data = token_text.data(),
             .size = static_cast<uint32_t>(std::min(
                     token_text.size(), static_cast<size_t>(inverted_index::MAX_WORD_LEN))),
             .position_increment = 1};
    return true;
}

size_t KuromojiTokenizer::reserved_bytes() const {
    size_t bytes = tokens_text_.capacity() * sizeof(std::string);
    for (const std::string& text : tokens_text_) {
        bytes += text.capacity();
    }
    return bytes;
}

} // namespace doris::segment_v2
