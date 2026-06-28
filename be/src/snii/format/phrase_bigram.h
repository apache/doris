#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace snii::format {

inline constexpr std::string_view kPhraseBigramTermMarker =
        "\x1F"
        "SNII_PHRASE_BIGRAM"
        "\x1F";

inline void append_phrase_bigram_varint32(uint32_t value, std::string* out) {
    while (value >= 0x80) {
        out->push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out->push_back(static_cast<char>(value));
}

inline std::string make_phrase_bigram_term(std::string_view left, std::string_view right) {
    std::string out;
    out.reserve(kPhraseBigramTermMarker.size() + 5 + left.size() + right.size());
    out.append(kPhraseBigramTermMarker);
    append_phrase_bigram_varint32(static_cast<uint32_t>(left.size()), &out);
    out.append(left);
    out.append(right);
    return out;
}

inline std::string make_phrase_bigram_sentinel_term() {
    std::string out(kPhraseBigramTermMarker);
    out.push_back('\0');
    return out;
}

inline bool is_phrase_bigram_term(std::string_view term) {
    return term.starts_with(kPhraseBigramTermMarker);
}

inline bool is_ascii_alpha_phrase_bigram_char(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z');
}

inline bool is_phrase_bigram_indexable_term(std::string_view term) {
    constexpr size_t kMinPhraseBigramTermLength = 2;
    constexpr size_t kMaxPhraseBigramTermLength = 32;
    if (term.size() < kMinPhraseBigramTermLength || term.size() > kMaxPhraseBigramTermLength) {
        return false;
    }
    for (const char c : term) {
        if (!is_ascii_alpha_phrase_bigram_char(c)) {
            return false;
        }
    }
    return true;
}

} // namespace snii::format
