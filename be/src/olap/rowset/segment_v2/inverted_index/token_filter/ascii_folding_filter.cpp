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

#include "ascii_folding_filter.h"

#include <optional>
#include <string_view>

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

ASCIIFoldingFilter::ASCIIFoldingFilter(const TokenStreamPtr& in, bool preserve_original)
        : DorisTokenFilter(in), _preserve_original(preserve_original), _output(512, 0) {}

Token* ASCIIFoldingFilter::next(Token* t) {
    if (_state != std::nullopt) {
        assert(_preserve_original);
        set(t, std::string_view(_state->data(), _state->size()), 0);
        _state = std::nullopt;
        return t;
    }
    if (_in->next(t)) {
        const char* buffer = t->termBuffer<char>();
        auto length = static_cast<int32_t>(t->termLength<char>());
        for (int32_t i = 0; i < length;) {
            UChar32 c = U_UNASSIGNED;
            U8_NEXT(buffer, i, length, c);
            if (c < 0) {
                continue;
            }
            if (c >= 0x0080) {
                fold_to_ascii(buffer, length);
                set_text(t, std::string_view(_output.data(), _output_pos));
                break;
            }
        }
        return t;
    }
    return nullptr;
}

void ASCIIFoldingFilter::reset() {
    DorisTokenFilter::reset();
    _state = std::nullopt;
}

void ASCIIFoldingFilter::fold_to_ascii(const char* in, int32_t length) {
    // Worst-case scenario: 3-byte UTF-8 → 4-byte ASCII (expansion ratio ~1.33x)
    // Conservative estimate: 2x input length is sufficient to cover all cases
    int32_t max_size_needed = 2 * length;
    if (_output.size() < max_size_needed) {
        _output.resize(max_size_needed);
    }

    _output_pos = fold_to_ascii(in, 0, _output.data(), 0, length);
    if (_preserve_original && need_to_preserve(in, length)) {
        _state = std::string_view(in, length);
    }
}

bool ASCIIFoldingFilter::need_to_preserve(const char* in, int32_t input_length) {
    if (input_length != _output_pos) {
        return true;
    }
    for (int32_t i = 0; i < input_length; i++) {
        if (in[i] != _output[i]) {
            return true;
        }
    }
    return false;
}

int32_t ASCIIFoldingFilter::fold_to_ascii(const char* in, int32_t input_pos, char* out,
                                          int32_t output_pos, int32_t length) {
    int32_t end = input_pos + length;
    for (int32_t pos = input_pos; pos < end;) {
        const int32_t prev_pos = pos;
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(in, pos, end, c);
        if (c < 0) {
            continue;
        }

        // Quick test: if it's not in range then just keep current character
        if (c < 0x0080) {
            out[output_pos++] = static_cast<char>(c);
        } else {
            switch (c) {
            case 0x00C0: // À  [LATIN CAPITAL LETTER A WITH GRAVE]
            case 0x00C1: // Á  [LATIN CAPITAL LETTER A WITH ACUTE]
            case 0x00C2: // Â  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX]
            case 0x00C3: // Ã  [LATIN CAPITAL LETTER A WITH TILDE]
            case 0x00C4: // Ä  [LATIN CAPITAL LETTER A WITH DIAERESIS]
            case 0x00C5: // Å  [LATIN CAPITAL LETTER A WITH RING ABOVE]
            case 0x0100: // Ā  [LATIN CAPITAL LETTER A WITH MACRON]
            case 0x0102: // Ă  [LATIN CAPITAL LETTER A WITH BREVE]
            case 0x0104: // Ą  [LATIN CAPITAL LETTER A WITH OGONEK]
            case 0x018F: // Ə  http://en.wikipedia.org/wiki/Schwa  [LATIN CAPITAL LETTER SCHWA]
            case 0x01CD: // Ǎ  [LATIN CAPITAL LETTER A WITH CARON]
            case 0x01DE: // Ǟ  [LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON]
            case 0x01E0: // Ǡ  [LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON]
            case 0x01FA: // Ǻ  [LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE]
            case 0x0200: // Ȁ  [LATIN CAPITAL LETTER A WITH DOUBLE GRAVE]
            case 0x0202: // Ȃ  [LATIN CAPITAL LETTER A WITH INVERTED BREVE]
            case 0x0226: // Ȧ  [LATIN CAPITAL LETTER A WITH DOT ABOVE]
            case 0x023A: // Ⱥ  [LATIN CAPITAL LETTER A WITH STROKE]
            case 0x1D00: // ᴀ  [LATIN LETTER SMALL CAPITAL A]
            case 0x1E00: // Ḁ  [LATIN CAPITAL LETTER A WITH RING BELOW]
            case 0x1EA0: // Ạ  [LATIN CAPITAL LETTER A WITH DOT BELOW]
            case 0x1EA2: // Ả  [LATIN CAPITAL LETTER A WITH HOOK ABOVE]
            case 0x1EA4: // Ấ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE]
            case 0x1EA6: // Ầ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE]
            case 0x1EA8: // Ẩ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1EAA: // Ẫ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE]
            case 0x1EAC: // Ậ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
            case 0x1EAE: // Ắ  [LATIN CAPITAL LETTER A WITH BREVE AND ACUTE]
            case 0x1EB0: // Ằ  [LATIN CAPITAL LETTER A WITH BREVE AND GRAVE]
            case 0x1EB2: // Ẳ  [LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE]
            case 0x1EB4: // Ẵ  [LATIN CAPITAL LETTER A WITH BREVE AND TILDE]
            case 0x1EB6: // Ặ  [LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW]
            case 0x24B6: // Ⓐ  [CIRCLED LATIN CAPITAL LETTER A]
            case 0xFF21: // Ａ  [FULLWIDTH LATIN CAPITAL LETTER A]
                out[output_pos++] = 'A';
                break;
            case 0x00E0: // à  [LATIN SMALL LETTER A WITH GRAVE]
            case 0x00E1: // á  [LATIN SMALL LETTER A WITH ACUTE]
            case 0x00E2: // â  [LATIN SMALL LETTER A WITH CIRCUMFLEX]
            case 0x00E3: // ã  [LATIN SMALL LETTER A WITH TILDE]
            case 0x00E4: // ä  [LATIN SMALL LETTER A WITH DIAERESIS]
            case 0x00E5: // å  [LATIN SMALL LETTER A WITH RING ABOVE]
            case 0x0101: // ā  [LATIN SMALL LETTER A WITH MACRON]
            case 0x0103: // ă  [LATIN SMALL LETTER A WITH BREVE]
            case 0x0105: // ą  [LATIN SMALL LETTER A WITH OGONEK]
            case 0x01CE: // ǎ  [LATIN SMALL LETTER A WITH CARON]
            case 0x01DF: // ǟ  [LATIN SMALL LETTER A WITH DIAERESIS AND MACRON]
            case 0x01E1: // ǡ  [LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON]
            case 0x01FB: // ǻ  [LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE]
            case 0x0201: // ȁ  [LATIN SMALL LETTER A WITH DOUBLE GRAVE]
            case 0x0203: // ȃ  [LATIN SMALL LETTER A WITH INVERTED BREVE]
            case 0x0227: // ȧ  [LATIN SMALL LETTER A WITH DOT ABOVE]
            case 0x0250: // ɐ  [LATIN SMALL LETTER TURNED A]
            case 0x0259: // ə  [LATIN SMALL LETTER SCHWA]
            case 0x025A: // ɚ  [LATIN SMALL LETTER SCHWA WITH HOOK]
            case 0x1D8F: // ᶏ  [LATIN SMALL LETTER A WITH RETROFLEX HOOK]
            case 0x1D95: // ᶕ  [LATIN SMALL LETTER SCHWA WITH RETROFLEX HOOK]
            case 0x1E01: // ạ  [LATIN SMALL LETTER A WITH RING BELOW]
            case 0x1E9A: // ả  [LATIN SMALL LETTER A WITH RIGHT HALF RING]
            case 0x1EA1: // ạ  [LATIN SMALL LETTER A WITH DOT BELOW]
            case 0x1EA3: // ả  [LATIN SMALL LETTER A WITH HOOK ABOVE]
            case 0x1EA5: // ấ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE]
            case 0x1EA7: // ầ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE]
            case 0x1EA9: // ẩ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1EAB: // ẫ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE]
            case 0x1EAD: // ậ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
            case 0x1EAF: // ắ  [LATIN SMALL LETTER A WITH BREVE AND ACUTE]
            case 0x1EB1: // ằ  [LATIN SMALL LETTER A WITH BREVE AND GRAVE]
            case 0x1EB3: // ẳ  [LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE]
            case 0x1EB5: // ẵ  [LATIN SMALL LETTER A WITH BREVE AND TILDE]
            case 0x1EB7: // ặ  [LATIN SMALL LETTER A WITH BREVE AND DOT BELOW]
            case 0x2090: // ₐ  [LATIN SUBSCRIPT SMALL LETTER A]
            case 0x2094: // ₔ  [LATIN SUBSCRIPT SMALL LETTER SCHWA]
            case 0x24D0: // ⓐ  [CIRCLED LATIN SMALL LETTER A]
            case 0x2C65: // ⱥ  [LATIN SMALL LETTER A WITH STROKE]
            case 0x2C6F: // Ɐ  [LATIN CAPITAL LETTER TURNED A]
            case 0xFF41: // ａ  [FULLWIDTH LATIN SMALL LETTER A]
                out[output_pos++] = 'a';
                break;
            case 0xA732: // Ꜳ  [LATIN CAPITAL LETTER AA]
                out[output_pos++] = 'A';
                out[output_pos++] = 'A';
                break;
            case 0x00C6: // Æ  [LATIN CAPITAL LETTER AE]
            case 0x01E2: // Ǣ  [LATIN CAPITAL LETTER AE WITH MACRON]
            case 0x01FC: // Ǽ  [LATIN CAPITAL LETTER AE WITH ACUTE]
            case 0x1D01: // ᴁ  [LATIN LETTER SMALL CAPITAL AE]
                out[output_pos++] = 'A';
                out[output_pos++] = 'E';
                break;
            case 0xA734: // Ꜵ  [LATIN CAPITAL LETTER AO]
                out[output_pos++] = 'A';
                out[output_pos++] = 'O';
                break;
            case 0xA736: // Ꜷ  [LATIN CAPITAL LETTER AU]
                out[output_pos++] = 'A';
                out[output_pos++] = 'U';
                break;
            case 0xA738: // Ꜹ  [LATIN CAPITAL LETTER AV]
            case 0xA73A: // Ꜻ  [LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR]
                out[output_pos++] = 'A';
                out[output_pos++] = 'V';
                break;
            case 0xA73C: // Ꜽ  [LATIN CAPITAL LETTER AY]
                out[output_pos++] = 'A';
                out[output_pos++] = 'Y';
                break;
            case 0x249C: // ⒜  [PARENTHESIZED LATIN SMALL LETTER A]
                out[output_pos++] = '(';
                out[output_pos++] = 'a';
                out[output_pos++] = ')';
                break;
            case 0xA733: // ꜳ  [LATIN SMALL LETTER AA]
                out[output_pos++] = 'a';
                out[output_pos++] = 'a';
                break;
            case 0x00E6: // æ  [LATIN SMALL LETTER AE]
            case 0x01E3: // ǣ  [LATIN SMALL LETTER AE WITH MACRON]
            case 0x01FD: // ǽ  [LATIN SMALL LETTER AE WITH ACUTE]
            case 0x1D02: // ᴂ  [LATIN SMALL LETTER TURNED AE]
                out[output_pos++] = 'a';
                out[output_pos++] = 'e';
                break;
            case 0xA735: // ꜵ  [LATIN SMALL LETTER AO]
                out[output_pos++] = 'a';
                out[output_pos++] = 'o';
                break;
            case 0xA737: // ꜷ  [LATIN SMALL LETTER AU]
                out[output_pos++] = 'a';
                out[output_pos++] = 'u';
                break;
            case 0xA739: // ꜹ  [LATIN SMALL LETTER AV]
            case 0xA73B: // ꜻ  [LATIN SMALL LETTER AV WITH HORIZONTAL BAR]
                out[output_pos++] = 'a';
                out[output_pos++] = 'v';
                break;
            case 0xA73D: // ꜽ  [LATIN SMALL LETTER AY]
                out[output_pos++] = 'a';
                out[output_pos++] = 'y';
                break;
            case 0x0181: // Ɓ  [LATIN CAPITAL LETTER B WITH HOOK]
            case 0x0182: // Ƃ  [LATIN CAPITAL LETTER B WITH TOPBAR]
            case 0x0243: // Ƀ  [LATIN CAPITAL LETTER B WITH STROKE]
            case 0x0299: // ʙ  [LATIN LETTER SMALL CAPITAL B]
            case 0x1D03: // ᴃ  [LATIN LETTER SMALL CAPITAL BARRED B]
            case 0x1E02: // Ḃ  [LATIN CAPITAL LETTER B WITH DOT ABOVE]
            case 0x1E04: // Ḅ  [LATIN CAPITAL LETTER B WITH DOT BELOW]
            case 0x1E06: // Ḇ  [LATIN CAPITAL LETTER B WITH LINE BELOW]
            case 0x24B7: // Ⓑ  [CIRCLED LATIN CAPITAL LETTER B]
            case 0xFF22: // Ｂ  [FULLWIDTH LATIN CAPITAL LETTER B]
                out[output_pos++] = 'B';
                break;
            case 0x0180: // ƀ  [LATIN SMALL LETTER B WITH STROKE]
            case 0x0183: // ƃ  [LATIN SMALL LETTER B WITH TOPBAR]
            case 0x0253: // ɓ  [LATIN SMALL LETTER B WITH HOOK]
            case 0x1D6C: // ᵬ  [LATIN SMALL LETTER B WITH MIDDLE TILDE]
            case 0x1D80: // ᶀ  [LATIN SMALL LETTER B WITH PALATAL HOOK]
            case 0x1E03: // ḃ  [LATIN SMALL LETTER B WITH DOT ABOVE]
            case 0x1E05: // ḅ  [LATIN SMALL LETTER B WITH DOT BELOW]
            case 0x1E07: // ḇ  [LATIN SMALL LETTER B WITH LINE BELOW]
            case 0x24D1: // ⓑ  [CIRCLED LATIN SMALL LETTER B]
            case 0xFF42: // ｂ  [FULLWIDTH LATIN SMALL LETTER B]
                out[output_pos++] = 'b';
                break;
            case 0x249D: // ⒝  [PARENTHESIZED LATIN SMALL LETTER B]
                out[output_pos++] = '(';
                out[output_pos++] = 'b';
                out[output_pos++] = ')';
                break;
            case 0x00C7: // Ç  [LATIN CAPITAL LETTER C WITH CEDILLA]
            case 0x0106: // Ć  [LATIN CAPITAL LETTER C WITH ACUTE]
            case 0x0108: // Ĉ  [LATIN CAPITAL LETTER C WITH CIRCUMFLEX]
            case 0x010A: // Ċ  [LATIN CAPITAL LETTER C WITH DOT ABOVE]
            case 0x010C: // Č  [LATIN CAPITAL LETTER C WITH CARON]
            case 0x0187: // Ƈ  [LATIN CAPITAL LETTER C WITH HOOK]
            case 0x023B: // Ȼ  [LATIN CAPITAL LETTER C WITH STROKE]
            case 0x0297: // ʗ  [LATIN LETTER STRETCHED C]
            case 0x1D04: // ᴄ  [LATIN LETTER SMALL CAPITAL C]
            case 0x1E08: // Ḉ  [LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE]
            case 0x24B8: // Ⓒ  [CIRCLED LATIN CAPITAL LETTER C]
            case 0xFF23: // Ｃ  [FULLWIDTH LATIN CAPITAL LETTER C]
                out[output_pos++] = 'C';
                break;
            case 0x00E7: // ç  [LATIN SMALL LETTER C WITH CEDILLA]
            case 0x0107: // ć  [LATIN SMALL LETTER C WITH ACUTE]
            case 0x0109: // ĉ  [LATIN SMALL LETTER C WITH CIRCUMFLEX]
            case 0x010B: // ċ  [LATIN SMALL LETTER C WITH DOT ABOVE]
            case 0x010D: // č  [LATIN SMALL LETTER C WITH CARON]
            case 0x0188: // ƈ  [LATIN SMALL LETTER C WITH HOOK]
            case 0x023C: // ȼ  [LATIN SMALL LETTER C WITH STROKE]
            case 0x0255: // ɕ  [LATIN SMALL LETTER C WITH CURL]
            case 0x1E09: // ḉ  [LATIN SMALL LETTER C WITH CEDILLA AND ACUTE]
            case 0x2184: // ↄ  [LATIN SMALL LETTER REVERSED C]
            case 0x24D2: // ⓒ  [CIRCLED LATIN SMALL LETTER C]
            case 0xA73E: // Ꜿ  [LATIN CAPITAL LETTER REVERSED C WITH DOT]
            case 0xA73F: // ꜿ  [LATIN SMALL LETTER REVERSED C WITH DOT]
            case 0xFF43: // ｃ  [FULLWIDTH LATIN SMALL LETTER C]
                out[output_pos++] = 'c';
                break;
            case 0x249E: // ⒞  [PARENTHESIZED LATIN SMALL LETTER C]
                out[output_pos++] = '(';
                out[output_pos++] = 'c';
                out[output_pos++] = ')';
                break;
            case 0x00D0: // Ð  [LATIN CAPITAL LETTER ETH]
            case 0x010E: // Ď  [LATIN CAPITAL LETTER D WITH CARON]
            case 0x0110: // Đ  [LATIN CAPITAL LETTER D WITH STROKE]
            case 0x0189: // Ɖ  [LATIN CAPITAL LETTER AFRICAN D]
            case 0x018A: // Ɗ  [LATIN CAPITAL LETTER D WITH HOOK]
            case 0x018B: // Ƌ  [LATIN CAPITAL LETTER D WITH TOPBAR]
            case 0x1D05: // ᴅ  [LATIN LETTER SMALL CAPITAL D]
            case 0x1D06: // ᴆ  [LATIN LETTER SMALL CAPITAL ETH]
            case 0x1E0A: // Ḋ  [LATIN CAPITAL LETTER D WITH DOT ABOVE]
            case 0x1E0C: // Ḍ  [LATIN CAPITAL LETTER D WITH DOT BELOW]
            case 0x1E0E: // Ḏ  [LATIN CAPITAL LETTER D WITH LINE BELOW]
            case 0x1E10: // Ḑ  [LATIN CAPITAL LETTER D WITH CEDILLA]
            case 0x1E12: // Ḓ  [LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW]
            case 0x24B9: // Ⓓ  [CIRCLED LATIN CAPITAL LETTER D]
            case 0xA779: // Ꝺ  [LATIN CAPITAL LETTER INSULAR D]
            case 0xFF24: // Ｄ  [FULLWIDTH LATIN CAPITAL LETTER D]
                out[output_pos++] = 'D';
                break;
            case 0x00F0: // ð  [LATIN SMALL LETTER ETH]
            case 0x010F: // ď  [LATIN SMALL LETTER D WITH CARON]
            case 0x0111: // đ  [LATIN SMALL LETTER D WITH STROKE]
            case 0x018C: // ƌ  [LATIN SMALL LETTER D WITH TOPBAR]
            case 0x0221: // ȡ  [LATIN SMALL LETTER D WITH CURL]
            case 0x0256: // ɖ  [LATIN SMALL LETTER D WITH TAIL]
            case 0x0257: // ɗ  [LATIN SMALL LETTER D WITH HOOK]
            case 0x1D6D: // ᵭ  [LATIN SMALL LETTER D WITH MIDDLE TILDE]
            case 0x1D81: // ᶁ  [LATIN SMALL LETTER D WITH PALATAL HOOK]
            case 0x1D91: // ᶑ  [LATIN SMALL LETTER D WITH HOOK AND TAIL]
            case 0x1E0B: // ḋ  [LATIN SMALL LETTER D WITH DOT ABOVE]
            case 0x1E0D: // ḍ  [LATIN SMALL LETTER D WITH DOT BELOW]
            case 0x1E0F: // ḏ  [LATIN SMALL LETTER D WITH LINE BELOW]
            case 0x1E11: // ḑ  [LATIN SMALL LETTER D WITH CEDILLA]
            case 0x1E13: // ḓ  [LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW]
            case 0x24D3: // ⓓ  [CIRCLED LATIN SMALL LETTER D]
            case 0xA77A: // ꝺ  [LATIN SMALL LETTER INSULAR D]
            case 0xFF44: // ｄ  [FULLWIDTH LATIN SMALL LETTER D]
                out[output_pos++] = 'd';
                break;
            case 0x01C4: // Ǆ  [LATIN CAPITAL LETTER DZ WITH CARON]
            case 0x01F1: // Ǳ  [LATIN CAPITAL LETTER DZ]
                out[output_pos++] = 'D';
                out[output_pos++] = 'Z';
                break;
            case 0x01C5: // ǅ  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
            case 0x01F2: // ǲ  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
                out[output_pos++] = 'D';
                out[output_pos++] = 'z';
                break;
            case 0x249F: // ⒟  [PARENTHESIZED LATIN SMALL LETTER D]
                out[output_pos++] = '(';
                out[output_pos++] = 'd';
                out[output_pos++] = ')';
                break;
            case 0x0238: // ȸ  [LATIN SMALL LETTER DB DIGRAPH]
                out[output_pos++] = 'd';
                out[output_pos++] = 'b';
                break;
            case 0x01C6: // ǆ  [LATIN SMALL LETTER DZ WITH CARON]
            case 0x01F3: // ǳ  [LATIN SMALL LETTER DZ]
            case 0x02A3: // ʣ  [LATIN SMALL LETTER DZ DIGRAPH]
            case 0x02A5: // ʥ  [LATIN SMALL LETTER DZ DIGRAPH WITH CURL]
                out[output_pos++] = 'd';
                out[output_pos++] = 'z';
                break;
            case 0x00C8: // È  [LATIN CAPITAL LETTER E WITH GRAVE]
            case 0x00C9: // É  [LATIN CAPITAL LETTER E WITH ACUTE]
            case 0x00CA: // Ê  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX]
            case 0x00CB: // Ë  [LATIN CAPITAL LETTER E WITH DIAERESIS]
            case 0x0112: // Ē  [LATIN CAPITAL LETTER E WITH MACRON]
            case 0x0114: // Ĕ  [LATIN CAPITAL LETTER E WITH BREVE]
            case 0x0116: // Ė  [LATIN CAPITAL LETTER E WITH DOT ABOVE]
            case 0x0118: // Ę  [LATIN CAPITAL LETTER E WITH OGONEK]
            case 0x011A: // Ě  [LATIN CAPITAL LETTER E WITH CARON]
            case 0x018E: // Ǝ  [LATIN CAPITAL LETTER REVERSED E]
            case 0x0190: // Ɛ  [LATIN CAPITAL LETTER OPEN E]
            case 0x0204: // Ȅ  [LATIN CAPITAL LETTER E WITH DOUBLE GRAVE]
            case 0x0206: // Ȇ  [LATIN CAPITAL LETTER E WITH INVERTED BREVE]
            case 0x0228: // Ȩ  [LATIN CAPITAL LETTER E WITH CEDILLA]
            case 0x0246: // Ɇ  [LATIN CAPITAL LETTER E WITH STROKE]
            case 0x1D07: // ᴇ  [LATIN LETTER SMALL CAPITAL E]
            case 0x1E14: // Ḕ  [LATIN CAPITAL LETTER E WITH MACRON AND GRAVE]
            case 0x1E16: // Ḗ  [LATIN CAPITAL LETTER E WITH MACRON AND ACUTE]
            case 0x1E18: // Ḙ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW]
            case 0x1E1A: // Ḛ  [LATIN CAPITAL LETTER E WITH TILDE BELOW]
            case 0x1E1C: // Ḝ  [LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE]
            case 0x1EB8: // Ẹ  [LATIN CAPITAL LETTER E WITH DOT BELOW]
            case 0x1EBA: // Ẻ  [LATIN CAPITAL LETTER E WITH HOOK ABOVE]
            case 0x1EBC: // Ẽ  [LATIN CAPITAL LETTER E WITH TILDE]
            case 0x1EBE: // Ế  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE]
            case 0x1EC0: // Ề  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE]
            case 0x1EC2: // Ể  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1EC4: // Ễ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE]
            case 0x1EC6: // Ệ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
            case 0x24BA: // Ⓔ  [CIRCLED LATIN CAPITAL LETTER E]
            case 0x2C7B: // ⱻ  [LATIN LETTER SMALL CAPITAL TURNED E]
            case 0xFF25: // Ｅ  [FULLWIDTH LATIN CAPITAL LETTER E]
                out[output_pos++] = 'E';
                break;
            case 0x00E8: // è  [LATIN SMALL LETTER E WITH GRAVE]
            case 0x00E9: // é  [LATIN SMALL LETTER E WITH ACUTE]
            case 0x00EA: // ê  [LATIN SMALL LETTER E WITH CIRCUMFLEX]
            case 0x00EB: // ë  [LATIN SMALL LETTER E WITH DIAERESIS]
            case 0x0113: // ē  [LATIN SMALL LETTER E WITH MACRON]
            case 0x0115: // ĕ  [LATIN SMALL LETTER E WITH BREVE]
            case 0x0117: // ė  [LATIN SMALL LETTER E WITH DOT ABOVE]
            case 0x0119: // ę  [LATIN SMALL LETTER E WITH OGONEK]
            case 0x011B: // ě  [LATIN SMALL LETTER E WITH CARON]
            case 0x01DD: // ǝ  [LATIN SMALL LETTER TURNED E]
            case 0x0205: // ȅ  [LATIN SMALL LETTER E WITH DOUBLE GRAVE]
            case 0x0207: // ȇ  [LATIN SMALL LETTER E WITH INVERTED BREVE]
            case 0x0229: // ȩ  [LATIN SMALL LETTER E WITH CEDILLA]
            case 0x0247: // ɇ  [LATIN SMALL LETTER E WITH STROKE]
            case 0x0258: // ɘ  [LATIN SMALL LETTER REVERSED E]
            case 0x025B: // ɛ  [LATIN SMALL LETTER OPEN E]
            case 0x025C: // ɜ  [LATIN SMALL LETTER REVERSED OPEN E]
            case 0x025D: // ɝ  [LATIN SMALL LETTER REVERSED OPEN E WITH HOOK]
            case 0x025E: // ɞ  [LATIN SMALL LETTER CLOSED REVERSED OPEN E]
            case 0x029A: // ʚ  [LATIN SMALL LETTER CLOSED OPEN E]
            case 0x1D08: // ᴈ  [LATIN SMALL LETTER TURNED OPEN E]
            case 0x1D92: // ᶒ  [LATIN SMALL LETTER E WITH RETROFLEX HOOK]
            case 0x1D93: // ᶓ  [LATIN SMALL LETTER OPEN E WITH RETROFLEX HOOK]
            case 0x1D94: // ᶔ  [LATIN SMALL LETTER REVERSED OPEN E WITH RETROFLEX HOOK]
            case 0x1E15: // ḕ  [LATIN SMALL LETTER E WITH MACRON AND GRAVE]
            case 0x1E17: // ḗ  [LATIN SMALL LETTER E WITH MACRON AND ACUTE]
            case 0x1E19: // ḙ  [LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW]
            case 0x1E1B: // ḛ  [LATIN SMALL LETTER E WITH TILDE BELOW]
            case 0x1E1D: // ḝ  [LATIN SMALL LETTER E WITH CEDILLA AND BREVE]
            case 0x1EB9: // ẹ  [LATIN SMALL LETTER E WITH DOT BELOW]
            case 0x1EBB: // ẻ  [LATIN SMALL LETTER E WITH HOOK ABOVE]
            case 0x1EBD: // ẽ  [LATIN SMALL LETTER E WITH TILDE]
            case 0x1EBF: // ế  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE]
            case 0x1EC1: // ề  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE]
            case 0x1EC3: // ể  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1EC5: // ễ  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE]
            case 0x1EC7: // ệ  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
            case 0x2091: // ₑ  [LATIN SUBSCRIPT SMALL LETTER E]
            case 0x24D4: // ⓔ  [CIRCLED LATIN SMALL LETTER E]
            case 0x2C78: // ⱸ  [LATIN SMALL LETTER E WITH NOTCH]
            case 0xFF45: // ｅ  [FULLWIDTH LATIN SMALL LETTER E]
                out[output_pos++] = 'e';
                break;
            case 0x24A0: // ⒠  [PARENTHESIZED LATIN SMALL LETTER E]
                out[output_pos++] = '(';
                out[output_pos++] = 'e';
                out[output_pos++] = ')';
                break;
            case 0x0191: // Ƒ  [LATIN CAPITAL LETTER F WITH HOOK]
            case 0x1E1E: // Ḟ  [LATIN CAPITAL LETTER F WITH DOT ABOVE]
            case 0x24BB: // Ⓕ  [CIRCLED LATIN CAPITAL LETTER F]
            case 0xA730: // ꜰ  [LATIN LETTER SMALL CAPITAL F]
            case 0xA77B: // Ꝼ  [LATIN CAPITAL LETTER INSULAR F]
            case 0xA7FB: // ꟻ  [LATIN EPIGRAPHIC LETTER REVERSED F]
            case 0xFF26: // Ｆ  [FULLWIDTH LATIN CAPITAL LETTER F]
                out[output_pos++] = 'F';
                break;
            case 0x0192: // ƒ  [LATIN SMALL LETTER F WITH HOOK]
            case 0x1D6E: // ᵮ  [LATIN SMALL LETTER F WITH MIDDLE TILDE]
            case 0x1D82: // ᶂ  [LATIN SMALL LETTER F WITH PALATAL HOOK]
            case 0x1E1F: // ḟ  [LATIN SMALL LETTER F WITH DOT ABOVE]
            case 0x1E9B: // ẛ  [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
            case 0x24D5: // ⓕ  [CIRCLED LATIN SMALL LETTER F]
            case 0xA77C: // ꝼ  [LATIN SMALL LETTER INSULAR F]
            case 0xFF46: // ｆ  [FULLWIDTH LATIN SMALL LETTER F]
                out[output_pos++] = 'f';
                break;
            case 0x24A1: // ⒡  [PARENTHESIZED LATIN SMALL LETTER F]
                out[output_pos++] = '(';
                out[output_pos++] = 'f';
                out[output_pos++] = ')';
                break;
            case 0xFB00: // ﬀ  [LATIN SMALL LIGATURE FF]
                out[output_pos++] = 'f';
                out[output_pos++] = 'f';
                break;
            case 0xFB03: // ﬃ  [LATIN SMALL LIGATURE FFI]
                out[output_pos++] = 'f';
                out[output_pos++] = 'f';
                out[output_pos++] = 'i';
                break;
            case 0xFB04: // ﬄ  [LATIN SMALL LIGATURE FFL]
                out[output_pos++] = 'f';
                out[output_pos++] = 'f';
                out[output_pos++] = 'l';
                break;
            case 0xFB01: // ﬁ  [LATIN SMALL LIGATURE FI]
                out[output_pos++] = 'f';
                out[output_pos++] = 'i';
                break;
            case 0xFB02: // ﬂ  [LATIN SMALL LIGATURE FL]
                out[output_pos++] = 'f';
                out[output_pos++] = 'l';
                break;
            case 0x011C: // Ĝ  [LATIN CAPITAL LETTER G WITH CIRCUMFLEX]
            case 0x011E: // Ğ  [LATIN CAPITAL LETTER G WITH BREVE]
            case 0x0120: // Ġ  [LATIN CAPITAL LETTER G WITH DOT ABOVE]
            case 0x0122: // Ģ  [LATIN CAPITAL LETTER G WITH CEDILLA]
            case 0x0193: // Ɠ  [LATIN CAPITAL LETTER G WITH HOOK]
            case 0x01E4: // Ǥ  [LATIN CAPITAL LETTER G WITH STROKE]
            case 0x01E5: // ǥ  [LATIN SMALL LETTER G WITH STROKE]
            case 0x01E6: // Ǧ  [LATIN CAPITAL LETTER G WITH CARON]
            case 0x01E7: // ǧ  [LATIN SMALL LETTER G WITH CARON]
            case 0x01F4: // Ǵ  [LATIN CAPITAL LETTER G WITH ACUTE]
            case 0x0262: // ɢ  [LATIN LETTER SMALL CAPITAL G]
            case 0x029B: // ʛ  [LATIN LETTER SMALL CAPITAL G WITH HOOK]
            case 0x1E20: // Ḡ  [LATIN CAPITAL LETTER G WITH MACRON]
            case 0x24BC: // Ⓖ  [CIRCLED LATIN CAPITAL LETTER G]
            case 0xA77D: // Ᵹ  [LATIN CAPITAL LETTER INSULAR G]
            case 0xA77E: // Ꝿ  [LATIN CAPITAL LETTER TURNED INSULAR G]
            case 0xFF27: // Ｇ  [FULLWIDTH LATIN CAPITAL LETTER G]
                out[output_pos++] = 'G';
                break;
            case 0x011D: // ĝ  [LATIN SMALL LETTER G WITH CIRCUMFLEX]
            case 0x011F: // ğ  [LATIN SMALL LETTER G WITH BREVE]
            case 0x0121: // ġ  [LATIN SMALL LETTER G WITH DOT ABOVE]
            case 0x0123: // ģ  [LATIN SMALL LETTER G WITH CEDILLA]
            case 0x01F5: // ǵ  [LATIN SMALL LETTER G WITH ACUTE]
            case 0x0260: // ɠ  [LATIN SMALL LETTER G WITH HOOK]
            case 0x0261: // ɡ  [LATIN SMALL LETTER SCRIPT G]
            case 0x1D77: // ᵷ  [LATIN SMALL LETTER TURNED G]
            case 0x1D79: // ᵹ  [LATIN SMALL LETTER INSULAR G]
            case 0x1D83: // ᶃ  [LATIN SMALL LETTER G WITH PALATAL HOOK]
            case 0x1E21: // ḡ  [LATIN SMALL LETTER G WITH MACRON]
            case 0x24D6: // ⓖ  [CIRCLED LATIN SMALL LETTER G]
            case 0xA77F: // ꝿ  [LATIN SMALL LETTER TURNED INSULAR G]
            case 0xFF47: // ｇ  [FULLWIDTH LATIN SMALL LETTER G]
                out[output_pos++] = 'g';
                break;
            case 0x24A2: // ⒢  [PARENTHESIZED LATIN SMALL LETTER G]
                out[output_pos++] = '(';
                out[output_pos++] = 'g';
                out[output_pos++] = ')';
                break;
            case 0x0124: // Ĥ  [LATIN CAPITAL LETTER H WITH CIRCUMFLEX]
            case 0x0126: // Ħ  [LATIN CAPITAL LETTER H WITH STROKE]
            case 0x021E: // Ȟ  [LATIN CAPITAL LETTER H WITH CARON]
            case 0x029C: // ʜ  [LATIN LETTER SMALL CAPITAL H]
            case 0x1E22: // Ḣ  [LATIN CAPITAL LETTER H WITH DOT ABOVE]
            case 0x1E24: // Ḥ  [LATIN CAPITAL LETTER H WITH DOT BELOW]
            case 0x1E26: // Ḧ  [LATIN CAPITAL LETTER H WITH DIAERESIS]
            case 0x1E28: // Ḩ  [LATIN CAPITAL LETTER H WITH CEDILLA]
            case 0x1E2A: // Ḫ  [LATIN CAPITAL LETTER H WITH BREVE BELOW]
            case 0x24BD: // Ⓗ  [CIRCLED LATIN CAPITAL LETTER H]
            case 0x2C67: // Ⱨ  [LATIN CAPITAL LETTER H WITH DESCENDER]
            case 0x2C75: // Ⱶ  [LATIN CAPITAL LETTER HALF H]
            case 0xFF28: // Ｈ  [FULLWIDTH LATIN CAPITAL LETTER H]
                out[output_pos++] = 'H';
                break;
            case 0x0125: // ĥ  [LATIN SMALL LETTER H WITH CIRCUMFLEX]
            case 0x0127: // ħ  [LATIN SMALL LETTER H WITH STROKE]
            case 0x021F: // ȟ  [LATIN SMALL LETTER H WITH CARON]
            case 0x0265: // ɥ  [LATIN SMALL LETTER TURNED H]
            case 0x0266: // ɦ  [LATIN SMALL LETTER H WITH HOOK]
            case 0x02AE: // ʮ  [LATIN SMALL LETTER TURNED H WITH FISHHOOK]
            case 0x02AF: // ʯ  [LATIN SMALL LETTER TURNED H WITH FISHHOOK AND TAIL]
            case 0x1E23: // ḣ  [LATIN SMALL LETTER H WITH DOT ABOVE]
            case 0x1E25: // ḥ  [LATIN SMALL LETTER H WITH DOT BELOW]
            case 0x1E27: // ḧ  [LATIN SMALL LETTER H WITH DIAERESIS]
            case 0x1E29: // ḩ  [LATIN SMALL LETTER H WITH CEDILLA]
            case 0x1E2B: // ḫ  [LATIN SMALL LETTER H WITH BREVE BELOW]
            case 0x1E96: // ẖ  [LATIN SMALL LETTER H WITH LINE BELOW]
            case 0x24D7: // ⓗ  [CIRCLED LATIN SMALL LETTER H]
            case 0x2C68: // ⱨ  [LATIN SMALL LETTER H WITH DESCENDER]
            case 0x2C76: // ⱶ  [LATIN SMALL LETTER HALF H]
            case 0xFF48: // ｈ  [FULLWIDTH LATIN SMALL LETTER H]
                out[output_pos++] = 'h';
                break;
            case 0x01F6: // Ƕ  http://en.wikipedia.org/wiki/Hwair  [LATIN CAPITAL LETTER HWAIR]
                out[output_pos++] = 'H';
                out[output_pos++] = 'V';
                break;
            case 0x24A3: // ⒣  [PARENTHESIZED LATIN SMALL LETTER H]
                out[output_pos++] = '(';
                out[output_pos++] = 'h';
                out[output_pos++] = ')';
                break;
            case 0x0195: // ƕ  [LATIN SMALL LETTER HV]
                out[output_pos++] = 'h';
                out[output_pos++] = 'v';
                break;
            case 0x00CC: // Ì  [LATIN CAPITAL LETTER I WITH GRAVE]
            case 0x00CD: // Í  [LATIN CAPITAL LETTER I WITH ACUTE]
            case 0x00CE: // Î  [LATIN CAPITAL LETTER I WITH CIRCUMFLEX]
            case 0x00CF: // Ï  [LATIN CAPITAL LETTER I WITH DIAERESIS]
            case 0x0128: // Ĩ  [LATIN CAPITAL LETTER I WITH TILDE]
            case 0x012A: // Ī  [LATIN CAPITAL LETTER I WITH MACRON]
            case 0x012C: // Ĭ  [LATIN CAPITAL LETTER I WITH BREVE]
            case 0x012E: // Į  [LATIN CAPITAL LETTER I WITH OGONEK]
            case 0x0130: // İ  [LATIN CAPITAL LETTER I WITH DOT ABOVE]
            case 0x0196: // Ɩ  [LATIN CAPITAL LETTER IOTA]
            case 0x0197: // Ɨ  [LATIN CAPITAL LETTER I WITH STROKE]
            case 0x01CF: // Ǐ  [LATIN CAPITAL LETTER I WITH CARON]
            case 0x0208: // Ȉ  [LATIN CAPITAL LETTER I WITH DOUBLE GRAVE]
            case 0x020A: // Ȋ  [LATIN CAPITAL LETTER I WITH INVERTED BREVE]
            case 0x026A: // ɪ  [LATIN LETTER SMALL CAPITAL I]
            case 0x1D7B: // ᵻ  [LATIN SMALL CAPITAL LETTER I WITH STROKE]
            case 0x1E2C: // Ḭ  [LATIN CAPITAL LETTER I WITH TILDE BELOW]
            case 0x1E2E: // Ḯ  [LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE]
            case 0x1EC8: // Ỉ  [LATIN CAPITAL LETTER I WITH HOOK ABOVE]
            case 0x1ECA: // Ị  [LATIN CAPITAL LETTER I WITH DOT BELOW]
            case 0x24BE: // Ⓘ  [CIRCLED LATIN CAPITAL LETTER I]
            case 0xA7FE: // ꟾ  [LATIN EPIGRAPHIC LETTER I LONGA]
            case 0xFF29: // Ｉ  [FULLWIDTH LATIN CAPITAL LETTER I]
                out[output_pos++] = 'I';
                break;
            case 0x00EC: // ì  [LATIN SMALL LETTER I WITH GRAVE]
            case 0x00ED: // í  [LATIN SMALL LETTER I WITH ACUTE]
            case 0x00EE: // î  [LATIN SMALL LETTER I WITH CIRCUMFLEX]
            case 0x00EF: // ï  [LATIN SMALL LETTER I WITH DIAERESIS]
            case 0x0129: // ĩ  [LATIN SMALL LETTER I WITH TILDE]
            case 0x012B: // ī  [LATIN SMALL LETTER I WITH MACRON]
            case 0x012D: // ĭ  [LATIN SMALL LETTER I WITH BREVE]
            case 0x012F: // į  [LATIN SMALL LETTER I WITH OGONEK]
            case 0x0131: // ı  [LATIN SMALL LETTER DOTLESS I]
            case 0x01D0: // ǐ  [LATIN SMALL LETTER I WITH CARON]
            case 0x0209: // ȉ  [LATIN SMALL LETTER I WITH DOUBLE GRAVE]
            case 0x020B: // ȋ  [LATIN SMALL LETTER I WITH INVERTED BREVE]
            case 0x0268: // ɨ  [LATIN SMALL LETTER I WITH STROKE]
            case 0x1D09: // ᴉ  [LATIN SMALL LETTER TURNED I]
            case 0x1D62: // ᵢ  [LATIN SUBSCRIPT SMALL LETTER I]
            case 0x1D7C: // ᵼ  [LATIN SMALL LETTER IOTA WITH STROKE]
            case 0x1D96: // ᶖ  [LATIN SMALL LETTER I WITH RETROFLEX HOOK]
            case 0x1E2D: // ḭ  [LATIN SMALL LETTER I WITH TILDE BELOW]
            case 0x1E2F: // ḯ  [LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE]
            case 0x1EC9: // ỉ  [LATIN SMALL LETTER I WITH HOOK ABOVE]
            case 0x1ECB: // ị  [LATIN SMALL LETTER I WITH DOT BELOW]
            case 0x2071: // ⁱ  [SUPERSCRIPT LATIN SMALL LETTER I]
            case 0x24D8: // ⓘ  [CIRCLED LATIN SMALL LETTER I]
            case 0xFF49: // ｉ  [FULLWIDTH LATIN SMALL LETTER I]
                out[output_pos++] = 'i';
                break;
            case 0x0132: // Ĳ  [LATIN CAPITAL LIGATURE IJ]
                out[output_pos++] = 'I';
                out[output_pos++] = 'J';
                break;
            case 0x24A4: // ⒤  [PARENTHESIZED LATIN SMALL LETTER I]
                out[output_pos++] = '(';
                out[output_pos++] = 'i';
                out[output_pos++] = ')';
                break;
            case 0x0133: // ĳ  [LATIN SMALL LIGATURE IJ]
                out[output_pos++] = 'i';
                out[output_pos++] = 'j';
                break;
            case 0x0134: // Ĵ  [LATIN CAPITAL LETTER J WITH CIRCUMFLEX]
            case 0x0248: // Ɉ  [LATIN CAPITAL LETTER J WITH STROKE]
            case 0x1D0A: // ᴊ  [LATIN LETTER SMALL CAPITAL J]
            case 0x24BF: // Ⓙ  [CIRCLED LATIN CAPITAL LETTER J]
            case 0xFF2A: // Ｊ  [FULLWIDTH LATIN CAPITAL LETTER J]
                out[output_pos++] = 'J';
                break;
            case 0x0135: // ĵ  [LATIN SMALL LETTER J WITH CIRCUMFLEX]
            case 0x01F0: // ǰ  [LATIN SMALL LETTER J WITH CARON]
            case 0x0237: // ȷ  [LATIN SMALL LETTER DOTLESS J]
            case 0x0249: // ɉ  [LATIN SMALL LETTER J WITH STROKE]
            case 0x025F: // ɟ  [LATIN SMALL LETTER DOTLESS J WITH STROKE]
            case 0x0284: // ʄ  [LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK]
            case 0x029D: // ʝ  [LATIN SMALL LETTER J WITH CROSSED-TAIL]
            case 0x24D9: // ⓙ  [CIRCLED LATIN SMALL LETTER J]
            case 0x2C7C: // ⱼ  [LATIN SUBSCRIPT SMALL LETTER J]
            case 0xFF4A: // ｊ  [FULLWIDTH LATIN SMALL LETTER J]
                out[output_pos++] = 'j';
                break;
            case 0x24A5: // ⒥  [PARENTHESIZED LATIN SMALL LETTER J]
                out[output_pos++] = '(';
                out[output_pos++] = 'j';
                out[output_pos++] = ')';
                break;
            case 0x0136: // Ķ  [LATIN CAPITAL LETTER K WITH CEDILLA]
            case 0x0198: // Ƙ  [LATIN CAPITAL LETTER K WITH HOOK]
            case 0x01E8: // Ǩ  [LATIN CAPITAL LETTER K WITH CARON]
            case 0x1D0B: // ᴋ  [LATIN LETTER SMALL CAPITAL K]
            case 0x1E30: // Ḱ  [LATIN CAPITAL LETTER K WITH ACUTE]
            case 0x1E32: // Ḳ  [LATIN CAPITAL LETTER K WITH DOT BELOW]
            case 0x1E34: // Ḵ  [LATIN CAPITAL LETTER K WITH LINE BELOW]
            case 0x24C0: // Ⓚ  [CIRCLED LATIN CAPITAL LETTER K]
            case 0x2C69: // Ⱪ  [LATIN CAPITAL LETTER K WITH DESCENDER]
            case 0xA740: // Ꝁ  [LATIN CAPITAL LETTER K WITH STROKE]
            case 0xA742: // Ꝃ  [LATIN CAPITAL LETTER K WITH DIAGONAL STROKE]
            case 0xA744: // Ꝅ  [LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE]
            case 0xFF2B: // Ｋ  [FULLWIDTH LATIN CAPITAL LETTER K]
                out[output_pos++] = 'K';
                break;
            case 0x0137: // ķ  [LATIN SMALL LETTER K WITH CEDILLA]
            case 0x0199: // ƙ  [LATIN SMALL LETTER K WITH HOOK]
            case 0x01E9: // ǩ  [LATIN SMALL LETTER K WITH CARON]
            case 0x029E: // ʞ  [LATIN SMALL LETTER TURNED K]
            case 0x1D84: // ᶄ  [LATIN SMALL LETTER K WITH PALATAL HOOK]
            case 0x1E31: // ḱ  [LATIN SMALL LETTER K WITH ACUTE]
            case 0x1E33: // ḳ  [LATIN SMALL LETTER K WITH DOT BELOW]
            case 0x1E35: // ḵ  [LATIN SMALL LETTER K WITH LINE BELOW]
            case 0x24DA: // ⓚ  [CIRCLED LATIN SMALL LETTER K]
            case 0x2C6A: // ⱪ  [LATIN SMALL LETTER K WITH DESCENDER]
            case 0xA741: // ꝁ  [LATIN SMALL LETTER K WITH STROKE]
            case 0xA743: // ꝃ  [LATIN SMALL LETTER K WITH DIAGONAL STROKE]
            case 0xA745: // ꝅ  [LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE]
            case 0xFF4B: // ｋ  [FULLWIDTH LATIN SMALL LETTER K]
                out[output_pos++] = 'k';
                break;
            case 0x24A6: // ⒦  [PARENTHESIZED LATIN SMALL LETTER K]
                out[output_pos++] = '(';
                out[output_pos++] = 'k';
                out[output_pos++] = ')';
                break;
            case 0x0139: // Ĺ  [LATIN CAPITAL LETTER L WITH ACUTE]
            case 0x013B: // Ļ  [LATIN CAPITAL LETTER L WITH CEDILLA]
            case 0x013D: // Ľ  [LATIN CAPITAL LETTER L WITH CARON]
            case 0x013F: // Ŀ  [LATIN CAPITAL LETTER L WITH MIDDLE DOT]
            case 0x0141: // Ł  [LATIN CAPITAL LETTER L WITH STROKE]
            case 0x023D: // Ƚ  [LATIN CAPITAL LETTER L WITH BAR]
            case 0x029F: // ʟ  [LATIN LETTER SMALL CAPITAL L]
            case 0x1D0C: // ᴌ  [LATIN LETTER SMALL CAPITAL L WITH STROKE]
            case 0x1E36: // Ḷ  [LATIN CAPITAL LETTER L WITH DOT BELOW]
            case 0x1E38: // Ḹ  [LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON]
            case 0x1E3A: // Ḻ  [LATIN CAPITAL LETTER L WITH LINE BELOW]
            case 0x1E3C: // Ḽ  [LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW]
            case 0x24C1: // Ⓛ  [CIRCLED LATIN CAPITAL LETTER L]
            case 0x2C60: // Ⱡ  [LATIN CAPITAL LETTER L WITH DOUBLE BAR]
            case 0x2C62: // Ɫ  [LATIN CAPITAL LETTER L WITH MIDDLE TILDE]
            case 0xA746: // Ꝇ  [LATIN CAPITAL LETTER BROKEN L]
            case 0xA748: // Ꝉ  [LATIN CAPITAL LETTER L WITH HIGH STROKE]
            case 0xA780: // Ꞁ  [LATIN CAPITAL LETTER TURNED L]
            case 0xFF2C: // Ｌ  [FULLWIDTH LATIN CAPITAL LETTER L]
                out[output_pos++] = 'L';
                break;
            case 0x013A: // ĺ  [LATIN SMALL LETTER L WITH ACUTE]
            case 0x013C: // ļ  [LATIN SMALL LETTER L WITH CEDILLA]
            case 0x013E: // ľ  [LATIN SMALL LETTER L WITH CARON]
            case 0x0140: // ŀ  [LATIN SMALL LETTER L WITH MIDDLE DOT]
            case 0x0142: // ł  [LATIN SMALL LETTER L WITH STROKE]
            case 0x019A: // ƚ  [LATIN SMALL LETTER L WITH BAR]
            case 0x0234: // ȴ  [LATIN SMALL LETTER L WITH CURL]
            case 0x026B: // ɫ  [LATIN SMALL LETTER L WITH MIDDLE TILDE]
            case 0x026C: // ɬ  [LATIN SMALL LETTER L WITH BELT]
            case 0x026D: // ɭ  [LATIN SMALL LETTER L WITH RETROFLEX HOOK]
            case 0x1D85: // ᶅ  [LATIN SMALL LETTER L WITH PALATAL HOOK]
            case 0x1E37: // ḷ  [LATIN SMALL LETTER L WITH DOT BELOW]
            case 0x1E39: // ḹ  [LATIN SMALL LETTER L WITH DOT BELOW AND MACRON]
            case 0x1E3B: // ḻ  [LATIN SMALL LETTER L WITH LINE BELOW]
            case 0x1E3D: // ḽ  [LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW]
            case 0x24DB: // ⓛ  [CIRCLED LATIN SMALL LETTER L]
            case 0x2C61: // ⱡ  [LATIN SMALL LETTER L WITH DOUBLE BAR]
            case 0xA747: // ꝇ  [LATIN SMALL LETTER BROKEN L]
            case 0xA749: // ꝉ  [LATIN SMALL LETTER L WITH HIGH STROKE]
            case 0xA781: // ꞁ  [LATIN SMALL LETTER TURNED L]
            case 0xFF4C: // ｌ  [FULLWIDTH LATIN SMALL LETTER L]
                out[output_pos++] = 'l';
                break;
            case 0x01C7: // Ǉ  [LATIN CAPITAL LETTER LJ]
                out[output_pos++] = 'L';
                out[output_pos++] = 'J';
                break;
            case 0x1EFA: // Ỻ  [LATIN CAPITAL LETTER MIDDLE-WELSH LL]
                out[output_pos++] = 'L';
                out[output_pos++] = 'L';
                break;
            case 0x01C8: // ǈ  [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
                out[output_pos++] = 'L';
                out[output_pos++] = 'j';
                break;
            case 0x24A7: // ⒧  [PARENTHESIZED LATIN SMALL LETTER L]
                out[output_pos++] = '(';
                out[output_pos++] = 'l';
                out[output_pos++] = ')';
                break;
            case 0x01C9: // ǉ  [LATIN SMALL LETTER LJ]
                out[output_pos++] = 'l';
                out[output_pos++] = 'j';
                break;
            case 0x1EFB: // ỻ  [LATIN SMALL LETTER MIDDLE-WELSH LL]
                out[output_pos++] = 'l';
                out[output_pos++] = 'l';
                break;
            case 0x02AA: // ʪ  [LATIN SMALL LETTER LS DIGRAPH]
                out[output_pos++] = 'l';
                out[output_pos++] = 's';
                break;
            case 0x02AB: // ʫ  [LATIN SMALL LETTER LZ DIGRAPH]
                out[output_pos++] = 'l';
                out[output_pos++] = 'z';
                break;
            case 0x019C: // Ɯ  [LATIN CAPITAL LETTER TURNED M]
            case 0x1D0D: // ᴍ  [LATIN LETTER SMALL CAPITAL M]
            case 0x1E3E: // Ḿ  [LATIN CAPITAL LETTER M WITH ACUTE]
            case 0x1E40: // Ṁ  [LATIN CAPITAL LETTER M WITH DOT ABOVE]
            case 0x1E42: // Ṃ  [LATIN CAPITAL LETTER M WITH DOT BELOW]
            case 0x24C2: // Ⓜ  [CIRCLED LATIN CAPITAL LETTER M]
            case 0x2C6E: // Ɱ  [LATIN CAPITAL LETTER M WITH HOOK]
            case 0xA7FD: // ꟽ  [LATIN EPIGRAPHIC LETTER INVERTED M]
            case 0xA7FF: // ꟿ  [LATIN EPIGRAPHIC LETTER ARCHAIC M]
            case 0xFF2D: // Ｍ  [FULLWIDTH LATIN CAPITAL LETTER M]
                out[output_pos++] = 'M';
                break;
            case 0x026F: // ɯ  [LATIN SMALL LETTER TURNED M]
            case 0x0270: // ɰ  [LATIN SMALL LETTER TURNED M WITH LONG LEG]
            case 0x0271: // ɱ  [LATIN SMALL LETTER M WITH HOOK]
            case 0x1D6F: // ᵯ  [LATIN SMALL LETTER M WITH MIDDLE TILDE]
            case 0x1D86: // ᶆ  [LATIN SMALL LETTER M WITH PALATAL HOOK]
            case 0x1E3F: // ḿ  [LATIN SMALL LETTER M WITH ACUTE]
            case 0x1E41: // ṁ  [LATIN SMALL LETTER M WITH DOT ABOVE]
            case 0x1E43: // ṃ  [LATIN SMALL LETTER M WITH DOT BELOW]
            case 0x24DC: // ⓜ  [CIRCLED LATIN SMALL LETTER M]
            case 0xFF4D: // ｍ  [FULLWIDTH LATIN SMALL LETTER M]
                out[output_pos++] = 'm';
                break;
            case 0x24A8: // ⒨  [PARENTHESIZED LATIN SMALL LETTER M]
                out[output_pos++] = '(';
                out[output_pos++] = 'm';
                out[output_pos++] = ')';
                break;
            case 0x00D1: // Ñ  [LATIN CAPITAL LETTER N WITH TILDE]
            case 0x0143: // Ń  [LATIN CAPITAL LETTER N WITH ACUTE]
            case 0x0145: // Ņ  [LATIN CAPITAL LETTER N WITH CEDILLA]
            case 0x0147: // Ň  [LATIN CAPITAL LETTER N WITH CARON]
            case 0x014A: // Ŋ  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN CAPITAL LETTER ENG]
            case 0x019D: // Ɲ  [LATIN CAPITAL LETTER N WITH LEFT HOOK]
            case 0x01F8: // Ǹ  [LATIN CAPITAL LETTER N WITH GRAVE]
            case 0x0220: // Ƞ  [LATIN CAPITAL LETTER N WITH LONG RIGHT LEG]
            case 0x0274: // ɴ  [LATIN LETTER SMALL CAPITAL N]
            case 0x1D0E: // ᴎ  [LATIN LETTER SMALL CAPITAL REVERSED N]
            case 0x1E44: // Ṅ  [LATIN CAPITAL LETTER N WITH DOT ABOVE]
            case 0x1E46: // Ṇ  [LATIN CAPITAL LETTER N WITH DOT BELOW]
            case 0x1E48: // Ṉ  [LATIN CAPITAL LETTER N WITH LINE BELOW]
            case 0x1E4A: // Ṋ  [LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW]
            case 0x24C3: // Ⓝ  [CIRCLED LATIN CAPITAL LETTER N]
            case 0xFF2E: // Ｎ  [FULLWIDTH LATIN CAPITAL LETTER N]
                out[output_pos++] = 'N';
                break;
            case 0x00F1: // ñ  [LATIN SMALL LETTER N WITH TILDE]
            case 0x0144: // ń  [LATIN SMALL LETTER N WITH ACUTE]
            case 0x0146: // ņ  [LATIN SMALL LETTER N WITH CEDILLA]
            case 0x0148: // ň  [LATIN SMALL LETTER N WITH CARON]
            case 0x0149: // ŉ  [LATIN SMALL LETTER N PRECEDED BY APOSTROPHE]
            case 0x014B: // ŋ  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN SMALL LETTER ENG]
            case 0x019E: // ƞ  [LATIN SMALL LETTER N WITH LONG RIGHT LEG]
            case 0x01F9: // ǹ  [LATIN SMALL LETTER N WITH GRAVE]
            case 0x0235: // ȵ  [LATIN SMALL LETTER N WITH CURL]
            case 0x0272: // ɲ  [LATIN SMALL LETTER N WITH LEFT HOOK]
            case 0x0273: // ɳ  [LATIN SMALL LETTER N WITH RETROFLEX HOOK]
            case 0x1D70: // ᵰ  [LATIN SMALL LETTER N WITH MIDDLE TILDE]
            case 0x1D87: // ᶇ  [LATIN SMALL LETTER N WITH PALATAL HOOK]
            case 0x1E45: // ṅ  [LATIN SMALL LETTER N WITH DOT ABOVE]
            case 0x1E47: // ṇ  [LATIN SMALL LETTER N WITH DOT BELOW]
            case 0x1E49: // ṉ  [LATIN SMALL LETTER N WITH LINE BELOW]
            case 0x1E4B: // ṋ  [LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW]
            case 0x207F: // ⁿ  [SUPERSCRIPT LATIN SMALL LETTER N]
            case 0x24DD: // ⓝ  [CIRCLED LATIN SMALL LETTER N]
            case 0xFF4E: // ｎ  [FULLWIDTH LATIN SMALL LETTER N]
                out[output_pos++] = 'n';
                break;
            case 0x01CA: // Ǌ  [LATIN CAPITAL LETTER NJ]
                out[output_pos++] = 'N';
                out[output_pos++] = 'J';
                break;
            case 0x01CB: // ǋ  [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
                out[output_pos++] = 'N';
                out[output_pos++] = 'j';
                break;
            case 0x24A9: // ⒩  [PARENTHESIZED LATIN SMALL LETTER N]
                out[output_pos++] = '(';
                out[output_pos++] = 'n';
                out[output_pos++] = ')';
                break;
            case 0x01CC: // ǌ  [LATIN SMALL LETTER NJ]
                out[output_pos++] = 'n';
                out[output_pos++] = 'j';
                break;
            case 0x00D2: // Ò  [LATIN CAPITAL LETTER O WITH GRAVE]
            case 0x00D3: // Ó  [LATIN CAPITAL LETTER O WITH ACUTE]
            case 0x00D4: // Ô  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX]
            case 0x00D5: // Õ  [LATIN CAPITAL LETTER O WITH TILDE]
            case 0x00D6: // Ö  [LATIN CAPITAL LETTER O WITH DIAERESIS]
            case 0x00D8: // Ø  [LATIN CAPITAL LETTER O WITH STROKE]
            case 0x014C: // Ō  [LATIN CAPITAL LETTER O WITH MACRON]
            case 0x014E: // Ŏ  [LATIN CAPITAL LETTER O WITH BREVE]
            case 0x0150: // Ő  [LATIN CAPITAL LETTER O WITH DOUBLE ACUTE]
            case 0x0186: // Ɔ  [LATIN CAPITAL LETTER OPEN O]
            case 0x019F: // Ɵ  [LATIN CAPITAL LETTER O WITH MIDDLE TILDE]
            case 0x01A0: // Ơ  [LATIN CAPITAL LETTER O WITH HORN]
            case 0x01D1: // Ǒ  [LATIN CAPITAL LETTER O WITH CARON]
            case 0x01EA: // Ǫ  [LATIN CAPITAL LETTER O WITH OGONEK]
            case 0x01EC: // Ǭ  [LATIN CAPITAL LETTER O WITH OGONEK AND MACRON]
            case 0x01FE: // Ǿ  [LATIN CAPITAL LETTER O WITH STROKE AND ACUTE]
            case 0x020C: // Ȍ  [LATIN CAPITAL LETTER O WITH DOUBLE GRAVE]
            case 0x020E: // Ȏ  [LATIN CAPITAL LETTER O WITH INVERTED BREVE]
            case 0x022A: // Ȫ  [LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON]
            case 0x022C: // Ȭ  [LATIN CAPITAL LETTER O WITH TILDE AND MACRON]
            case 0x022E: // Ȯ  [LATIN CAPITAL LETTER O WITH DOT ABOVE]
            case 0x0230: // Ȱ  [LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON]
            case 0x1D0F: // ᴏ  [LATIN LETTER SMALL CAPITAL O]
            case 0x1D10: // ᴐ  [LATIN LETTER SMALL CAPITAL OPEN O]
            case 0x1E4C: // Ṍ  [LATIN CAPITAL LETTER O WITH TILDE AND ACUTE]
            case 0x1E4E: // Ṏ  [LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS]
            case 0x1E50: // Ṑ  [LATIN CAPITAL LETTER O WITH MACRON AND GRAVE]
            case 0x1E52: // Ṓ  [LATIN CAPITAL LETTER O WITH MACRON AND ACUTE]
            case 0x1ECC: // Ọ  [LATIN CAPITAL LETTER O WITH DOT BELOW]
            case 0x1ECE: // Ỏ  [LATIN CAPITAL LETTER O WITH HOOK ABOVE]
            case 0x1ED0: // Ố  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE]
            case 0x1ED2: // Ồ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE]
            case 0x1ED4: // Ổ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1ED6: // Ỗ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE]
            case 0x1ED8: // Ộ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
            case 0x1EDA: // Ớ  [LATIN CAPITAL LETTER O WITH HORN AND ACUTE]
            case 0x1EDC: // Ờ  [LATIN CAPITAL LETTER O WITH HORN AND GRAVE]
            case 0x1EDE: // Ở  [LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE]
            case 0x1EE0: // Ỡ  [LATIN CAPITAL LETTER O WITH HORN AND TILDE]
            case 0x1EE2: // Ợ  [LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW]
            case 0x24C4: // Ⓞ  [CIRCLED LATIN CAPITAL LETTER O]
            case 0xA74A: // Ꝋ  [LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY]
            case 0xA74C: // Ꝍ  [LATIN CAPITAL LETTER O WITH LOOP]
            case 0xFF2F: // Ｏ  [FULLWIDTH LATIN CAPITAL LETTER O]
                out[output_pos++] = 'O';
                break;
            case 0x00F2: // ò  [LATIN SMALL LETTER O WITH GRAVE]
            case 0x00F3: // ó  [LATIN SMALL LETTER O WITH ACUTE]
            case 0x00F4: // ô  [LATIN SMALL LETTER O WITH CIRCUMFLEX]
            case 0x00F5: // õ  [LATIN SMALL LETTER O WITH TILDE]
            case 0x00F6: // ö  [LATIN SMALL LETTER O WITH DIAERESIS]
            case 0x00F8: // ø  [LATIN SMALL LETTER O WITH STROKE]
            case 0x014D: // ō  [LATIN SMALL LETTER O WITH MACRON]
            case 0x014F: // ŏ  [LATIN SMALL LETTER O WITH BREVE]
            case 0x0151: // ő  [LATIN SMALL LETTER O WITH DOUBLE ACUTE]
            case 0x01A1: // ơ  [LATIN SMALL LETTER O WITH HORN]
            case 0x01D2: // ǒ  [LATIN SMALL LETTER O WITH CARON]
            case 0x01EB: // ǫ  [LATIN SMALL LETTER O WITH OGONEK]
            case 0x01ED: // ǭ  [LATIN SMALL LETTER O WITH OGONEK AND MACRON]
            case 0x01FF: // ǿ  [LATIN SMALL LETTER O WITH STROKE AND ACUTE]
            case 0x020D: // ȍ  [LATIN SMALL LETTER O WITH DOUBLE GRAVE]
            case 0x020F: // ȏ  [LATIN SMALL LETTER O WITH INVERTED BREVE]
            case 0x022B: // ȫ  [LATIN SMALL LETTER O WITH DIAERESIS AND MACRON]
            case 0x022D: // ȭ  [LATIN SMALL LETTER O WITH TILDE AND MACRON]
            case 0x022F: // ȯ  [LATIN SMALL LETTER O WITH DOT ABOVE]
            case 0x0231: // ȱ  [LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON]
            case 0x0254: // ɔ  [LATIN SMALL LETTER OPEN O]
            case 0x0275: // ɵ  [LATIN SMALL LETTER BARRED O]
            case 0x1D16: // ᴖ  [LATIN SMALL LETTER TOP HALF O]
            case 0x1D17: // ᴗ  [LATIN SMALL LETTER BOTTOM HALF O]
            case 0x1D97: // ᶗ  [LATIN SMALL LETTER OPEN O WITH RETROFLEX HOOK]
            case 0x1E4D: // ṍ  [LATIN SMALL LETTER O WITH TILDE AND ACUTE]
            case 0x1E4F: // ṏ  [LATIN SMALL LETTER O WITH TILDE AND DIAERESIS]
            case 0x1E51: // ṑ  [LATIN SMALL LETTER O WITH MACRON AND GRAVE]
            case 0x1E53: // ṓ  [LATIN SMALL LETTER O WITH MACRON AND ACUTE]
            case 0x1ECD: // ọ  [LATIN SMALL LETTER O WITH DOT BELOW]
            case 0x1ECF: // ỏ  [LATIN SMALL LETTER O WITH HOOK ABOVE]
            case 0x1ED1: // ố  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE]
            case 0x1ED3: // ồ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE]
            case 0x1ED5: // ổ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
            case 0x1ED7: // ỗ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE]
            case 0x1ED9: // ộ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
            case 0x1EDB: // ớ  [LATIN SMALL LETTER O WITH HORN AND ACUTE]
            case 0x1EDD: // ờ  [LATIN SMALL LETTER O WITH HORN AND GRAVE]
            case 0x1EDF: // ở  [LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE]
            case 0x1EE1: // ỡ  [LATIN SMALL LETTER O WITH HORN AND TILDE]
            case 0x1EE3: // ợ  [LATIN SMALL LETTER O WITH HORN AND DOT BELOW]
            case 0x2092: // ₒ  [LATIN SUBSCRIPT SMALL LETTER O]
            case 0x24DE: // ⓞ  [CIRCLED LATIN SMALL LETTER O]
            case 0x2C7A: // ⱺ  [LATIN SMALL LETTER O WITH LOW RING INSIDE]
            case 0xA74B: // ꝋ  [LATIN SMALL LETTER O WITH LONG STROKE OVERLAY]
            case 0xA74D: // ꝍ  [LATIN SMALL LETTER O WITH LOOP]
            case 0xFF4F: // ｏ  [FULLWIDTH LATIN SMALL LETTER O]
                out[output_pos++] = 'o';
                break;
            case 0x0152: // Œ  [LATIN CAPITAL LIGATURE OE]
            case 0x0276: // ɶ  [LATIN LETTER SMALL CAPITAL OE]
                out[output_pos++] = 'O';
                out[output_pos++] = 'E';
                break;
            case 0xA74E: // Ꝏ  [LATIN CAPITAL LETTER OO]
                out[output_pos++] = 'O';
                out[output_pos++] = 'O';
                break;
            case 0x0222: // Ȣ  http://en.wikipedia.org/wiki/OU  [LATIN CAPITAL LETTER OU]
            case 0x1D15: // ᴕ  [LATIN LETTER SMALL CAPITAL OU]
                out[output_pos++] = 'O';
                out[output_pos++] = 'U';
                break;
            case 0x24AA: // ⒪  [PARENTHESIZED LATIN SMALL LETTER O]
                out[output_pos++] = '(';
                out[output_pos++] = 'o';
                out[output_pos++] = ')';
                break;
            case 0x0153: // œ  [LATIN SMALL LIGATURE OE]
            case 0x1D14: // ᴔ  [LATIN SMALL LETTER TURNED OE]
                out[output_pos++] = 'o';
                out[output_pos++] = 'e';
                break;
            case 0xA74F: // ꝏ  [LATIN SMALL LETTER OO]
                out[output_pos++] = 'o';
                out[output_pos++] = 'o';
                break;
            case 0x0223: // ȣ  http://en.wikipedia.org/wiki/OU  [LATIN SMALL LETTER OU]
                out[output_pos++] = 'o';
                out[output_pos++] = 'u';
                break;
            case 0x01A4: // Ƥ  [LATIN CAPITAL LETTER P WITH HOOK]
            case 0x1D18: // ᴘ  [LATIN LETTER SMALL CAPITAL P]
            case 0x1E54: // Ṕ  [LATIN CAPITAL LETTER P WITH ACUTE]
            case 0x1E56: // Ṗ  [LATIN CAPITAL LETTER P WITH DOT ABOVE]
            case 0x24C5: // Ⓟ  [CIRCLED LATIN CAPITAL LETTER P]
            case 0x2C63: // Ᵽ  [LATIN CAPITAL LETTER P WITH STROKE]
            case 0xA750: // Ꝑ  [LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER]
            case 0xA752: // Ꝓ  [LATIN CAPITAL LETTER P WITH FLOURISH]
            case 0xA754: // Ꝕ  [LATIN CAPITAL LETTER P WITH SQUIRREL TAIL]
            case 0xFF30: // Ｐ  [FULLWIDTH LATIN CAPITAL LETTER P]
                out[output_pos++] = 'P';
                break;
            case 0x01A5: // ƥ  [LATIN SMALL LETTER P WITH HOOK]
            case 0x1D71: // ᵱ  [LATIN SMALL LETTER P WITH MIDDLE TILDE]
            case 0x1D7D: // ᵽ  [LATIN SMALL LETTER P WITH STROKE]
            case 0x1D88: // ᶈ  [LATIN SMALL LETTER P WITH PALATAL HOOK]
            case 0x1E55: // ṕ  [LATIN SMALL LETTER P WITH ACUTE]
            case 0x1E57: // ṗ  [LATIN SMALL LETTER P WITH DOT ABOVE]
            case 0x24DF: // ⓟ  [CIRCLED LATIN SMALL LETTER P]
            case 0xA751: // ꝑ  [LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER]
            case 0xA753: // ꝓ  [LATIN SMALL LETTER P WITH FLOURISH]
            case 0xA755: // ꝕ  [LATIN SMALL LETTER P WITH SQUIRREL TAIL]
            case 0xA7FC: // ꟼ  [LATIN EPIGRAPHIC LETTER REVERSED P]
            case 0xFF50: // ｐ  [FULLWIDTH LATIN SMALL LETTER P]
                out[output_pos++] = 'p';
                break;
            case 0x24AB: // ⒫  [PARENTHESIZED LATIN SMALL LETTER P]
                out[output_pos++] = '(';
                out[output_pos++] = 'p';
                out[output_pos++] = ')';
                break;
            case 0x024A: // Ɋ  [LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL]
            case 0x24C6: // Ⓠ  [CIRCLED LATIN CAPITAL LETTER Q]
            case 0xA756: // Ꝗ  [LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER]
            case 0xA758: // Ꝙ  [LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE]
            case 0xFF31: // Ｑ  [FULLWIDTH LATIN CAPITAL LETTER Q]
                out[output_pos++] = 'Q';
                break;
            case 0x0138: // ĸ  http://en.wikipedia.org/wiki/Kra_(letter)  [LATIN SMALL LETTER KRA]
            case 0x024B: // ɋ  [LATIN SMALL LETTER Q WITH HOOK TAIL]
            case 0x02A0: // ʠ  [LATIN SMALL LETTER Q WITH HOOK]
            case 0x24E0: // ⓠ  [CIRCLED LATIN SMALL LETTER Q]
            case 0xA757: // ꝗ  [LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER]
            case 0xA759: // ꝙ  [LATIN SMALL LETTER Q WITH DIAGONAL STROKE]
            case 0xFF51: // ｑ  [FULLWIDTH LATIN SMALL LETTER Q]
                out[output_pos++] = 'q';
                break;
            case 0x24AC: // ⒬  [PARENTHESIZED LATIN SMALL LETTER Q]
                out[output_pos++] = '(';
                out[output_pos++] = 'q';
                out[output_pos++] = ')';
                break;
            case 0x0239: // ȹ  [LATIN SMALL LETTER QP DIGRAPH]
                out[output_pos++] = 'q';
                out[output_pos++] = 'p';
                break;
            case 0x0154: // Ŕ  [LATIN CAPITAL LETTER R WITH ACUTE]
            case 0x0156: // Ŗ  [LATIN CAPITAL LETTER R WITH CEDILLA]
            case 0x0158: // Ř  [LATIN CAPITAL LETTER R WITH CARON]
            case 0x0210: // Ȓ  [LATIN CAPITAL LETTER R WITH DOUBLE GRAVE]
            case 0x0212: // Ȓ  [LATIN CAPITAL LETTER R WITH INVERTED BREVE]
            case 0x024C: // Ɍ  [LATIN CAPITAL LETTER R WITH STROKE]
            case 0x0280: // ʀ  [LATIN LETTER SMALL CAPITAL R]
            case 0x0281: // ʁ  [LATIN LETTER SMALL CAPITAL INVERTED R]
            case 0x1D19: // ᴙ  [LATIN LETTER SMALL CAPITAL REVERSED R]
            case 0x1D1A: // ᴚ  [LATIN LETTER SMALL CAPITAL TURNED R]
            case 0x1E58: // Ṙ  [LATIN CAPITAL LETTER R WITH DOT ABOVE]
            case 0x1E5A: // Ṛ  [LATIN CAPITAL LETTER R WITH DOT BELOW]
            case 0x1E5C: // Ṝ  [LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON]
            case 0x1E5E: // Ṟ  [LATIN CAPITAL LETTER R WITH LINE BELOW]
            case 0x24C7: // Ⓡ  [CIRCLED LATIN CAPITAL LETTER R]
            case 0x2C64: // Ɽ  [LATIN CAPITAL LETTER R WITH TAIL]
            case 0xA75A: // Ꝛ  [LATIN CAPITAL LETTER R ROTUNDA]
            case 0xA782: // Ꞃ  [LATIN CAPITAL LETTER INSULAR R]
            case 0xFF32: // Ｒ  [FULLWIDTH LATIN CAPITAL LETTER R]
                out[output_pos++] = 'R';
                break;
            case 0x0155: // ŕ  [LATIN SMALL LETTER R WITH ACUTE]
            case 0x0157: // ŗ  [LATIN SMALL LETTER R WITH CEDILLA]
            case 0x0159: // ř  [LATIN SMALL LETTER R WITH CARON]
            case 0x0211: // ȑ  [LATIN SMALL LETTER R WITH DOUBLE GRAVE]
            case 0x0213: // ȓ  [LATIN SMALL LETTER R WITH INVERTED BREVE]
            case 0x024D: // ɍ  [LATIN SMALL LETTER R WITH STROKE]
            case 0x027C: // ɼ  [LATIN SMALL LETTER R WITH LONG LEG]
            case 0x027D: // ɽ  [LATIN SMALL LETTER R WITH TAIL]
            case 0x027E: // ɾ  [LATIN SMALL LETTER R WITH FISHHOOK]
            case 0x027F: // ɿ  [LATIN SMALL LETTER REVERSED R WITH FISHHOOK]
            case 0x1D63: // ᵣ  [LATIN SUBSCRIPT SMALL LETTER R]
            case 0x1D72: // ᵲ  [LATIN SMALL LETTER R WITH MIDDLE TILDE]
            case 0x1D73: // ᵳ  [LATIN SMALL LETTER R WITH FISHHOOK AND MIDDLE TILDE]
            case 0x1D89: // ᶉ  [LATIN SMALL LETTER R WITH PALATAL HOOK]
            case 0x1E59: // ṙ  [LATIN SMALL LETTER R WITH DOT ABOVE]
            case 0x1E5B: // ṛ  [LATIN SMALL LETTER R WITH DOT BELOW]
            case 0x1E5D: // ṝ  [LATIN SMALL LETTER R WITH DOT BELOW AND MACRON]
            case 0x1E5F: // ṟ  [LATIN SMALL LETTER R WITH LINE BELOW]
            case 0x24E1: // ⓡ  [CIRCLED LATIN SMALL LETTER R]
            case 0xA75B: // ꝛ  [LATIN SMALL LETTER R ROTUNDA]
            case 0xA783: // ꞃ  [LATIN SMALL LETTER INSULAR R]
            case 0xFF52: // ｒ  [FULLWIDTH LATIN SMALL LETTER R]
                out[output_pos++] = 'r';
                break;
            case 0x24AD: // ⒭  [PARENTHESIZED LATIN SMALL LETTER R]
                out[output_pos++] = '(';
                out[output_pos++] = 'r';
                out[output_pos++] = ')';
                break;
            case 0x015A: // Ś  [LATIN CAPITAL LETTER S WITH ACUTE]
            case 0x015C: // Ŝ  [LATIN CAPITAL LETTER S WITH CIRCUMFLEX]
            case 0x015E: // Ş  [LATIN CAPITAL LETTER S WITH CEDILLA]
            case 0x0160: // Š  [LATIN CAPITAL LETTER S WITH CARON]
            case 0x0218: // Ș  [LATIN CAPITAL LETTER S WITH COMMA BELOW]
            case 0x1E60: // Ṡ  [LATIN CAPITAL LETTER S WITH DOT ABOVE]
            case 0x1E62: // Ṣ  [LATIN CAPITAL LETTER S WITH DOT BELOW]
            case 0x1E64: // Ṥ  [LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE]
            case 0x1E66: // Ṧ  [LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE]
            case 0x1E68: // Ṩ  [LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE]
            case 0x24C8: // Ⓢ  [CIRCLED LATIN CAPITAL LETTER S]
            case 0xA731: // ꜱ  [LATIN LETTER SMALL CAPITAL S]
            case 0xA785: // ꞅ  [LATIN SMALL LETTER INSULAR S]
            case 0xFF33: // Ｓ  [FULLWIDTH LATIN CAPITAL LETTER S]
                out[output_pos++] = 'S';
                break;
            case 0x015B: // ś  [LATIN SMALL LETTER S WITH ACUTE]
            case 0x015D: // ŝ  [LATIN SMALL LETTER S WITH CIRCUMFLEX]
            case 0x015F: // ş  [LATIN SMALL LETTER S WITH CEDILLA]
            case 0x0161: // š  [LATIN SMALL LETTER S WITH CARON]
            case 0x017F: // ſ  http://en.wikipedia.org/wiki/Long_S  [LATIN SMALL LETTER LONG S]
            case 0x0219: // ș  [LATIN SMALL LETTER S WITH COMMA BELOW]
            case 0x023F: // ȿ  [LATIN SMALL LETTER S WITH SWASH TAIL]
            case 0x0282: // ʂ  [LATIN SMALL LETTER S WITH HOOK]
            case 0x1D74: // ᵴ  [LATIN SMALL LETTER S WITH MIDDLE TILDE]
            case 0x1D8A: // ᶊ  [LATIN SMALL LETTER S WITH PALATAL HOOK]
            case 0x1E61: // ṡ  [LATIN SMALL LETTER S WITH DOT ABOVE]
            case 0x1E63: // ṣ  [LATIN SMALL LETTER S WITH DOT BELOW]
            case 0x1E65: // ṥ  [LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE]
            case 0x1E67: // ṧ  [LATIN SMALL LETTER S WITH CARON AND DOT ABOVE]
            case 0x1E69: // ṩ  [LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE]
            case 0x1E9C: // ẜ  [LATIN SMALL LETTER LONG S WITH DIAGONAL STROKE]
            case 0x1E9D: // ẝ  [LATIN SMALL LETTER LONG S WITH HIGH STROKE]
            case 0x24E2: // ⓢ  [CIRCLED LATIN SMALL LETTER S]
            case 0xA784: // Ꞅ  [LATIN CAPITAL LETTER INSULAR S]
            case 0xFF53: // ｓ  [FULLWIDTH LATIN SMALL LETTER S]
                out[output_pos++] = 's';
                break;
            case 0x1E9E: // ẞ  [LATIN CAPITAL LETTER SHARP S]
                out[output_pos++] = 'S';
                out[output_pos++] = 'S';
                break;
            case 0x24AE: // ⒮  [PARENTHESIZED LATIN SMALL LETTER S]
                out[output_pos++] = '(';
                out[output_pos++] = 's';
                out[output_pos++] = ')';
                break;
            case 0x00DF: // ß  [LATIN SMALL LETTER SHARP S]
                out[output_pos++] = 's';
                out[output_pos++] = 's';
                break;
            case 0xFB06: // ﬆ  [LATIN SMALL LIGATURE ST]
                out[output_pos++] = 's';
                out[output_pos++] = 't';
                break;
            case 0x0162: // Ţ  [LATIN CAPITAL LETTER T WITH CEDILLA]
            case 0x0164: // Ť  [LATIN CAPITAL LETTER T WITH CARON]
            case 0x0166: // Ŧ  [LATIN CAPITAL LETTER T WITH STROKE]
            case 0x01AC: // Ƭ  [LATIN CAPITAL LETTER T WITH HOOK]
            case 0x01AE: // Ʈ  [LATIN CAPITAL LETTER T WITH RETROFLEX HOOK]
            case 0x021A: // Ț  [LATIN CAPITAL LETTER T WITH COMMA BELOW]
            case 0x023E: // Ⱦ  [LATIN CAPITAL LETTER T WITH DIAGONAL STROKE]
            case 0x1D1B: // ᴛ  [LATIN LETTER SMALL CAPITAL T]
            case 0x1E6A: // Ṫ  [LATIN CAPITAL LETTER T WITH DOT ABOVE]
            case 0x1E6C: // Ṭ  [LATIN CAPITAL LETTER T WITH DOT BELOW]
            case 0x1E6E: // Ṯ  [LATIN CAPITAL LETTER T WITH LINE BELOW]
            case 0x1E70: // Ṱ  [LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW]
            case 0x24C9: // Ⓣ  [CIRCLED LATIN CAPITAL LETTER T]
            case 0xA786: // Ꞇ  [LATIN CAPITAL LETTER INSULAR T]
            case 0xFF34: // Ｔ  [FULLWIDTH LATIN CAPITAL LETTER T]
                out[output_pos++] = 'T';
                break;
            case 0x0163: // ţ  [LATIN SMALL LETTER T WITH CEDILLA]
            case 0x0165: // ť  [LATIN SMALL LETTER T WITH CARON]
            case 0x0167: // ŧ  [LATIN SMALL LETTER T WITH STROKE]
            case 0x01AB: // ƫ  [LATIN SMALL LETTER T WITH PALATAL HOOK]
            case 0x01AD: // ƭ  [LATIN SMALL LETTER T WITH HOOK]
            case 0x021B: // ț  [LATIN SMALL LETTER T WITH COMMA BELOW]
            case 0x0236: // ȶ  [LATIN SMALL LETTER T WITH CURL]
            case 0x0287: // ʇ  [LATIN SMALL LETTER TURNED T]
            case 0x0288: // ʈ  [LATIN SMALL LETTER T WITH RETROFLEX HOOK]
            case 0x1D75: // ᵵ  [LATIN SMALL LETTER T WITH MIDDLE TILDE]
            case 0x1E6B: // ṫ  [LATIN SMALL LETTER T WITH DOT ABOVE]
            case 0x1E6D: // ṭ  [LATIN SMALL LETTER T WITH DOT BELOW]
            case 0x1E6F: // ṯ  [LATIN SMALL LETTER T WITH LINE BELOW]
            case 0x1E71: // ṱ  [LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW]
            case 0x1E97: // ẗ  [LATIN SMALL LETTER T WITH DIAERESIS]
            case 0x24E3: // ⓣ  [CIRCLED LATIN SMALL LETTER T]
            case 0x2C66: // ⱦ  [LATIN SMALL LETTER T WITH DIAGONAL STROKE]
            case 0xFF54: // ｔ  [FULLWIDTH LATIN SMALL LETTER T]
                out[output_pos++] = 't';
                break;
            case 0x00DE: // Þ  [LATIN CAPITAL LETTER THORN]
            case 0xA766: // Ꝧ  [LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER]
                out[output_pos++] = 'T';
                out[output_pos++] = 'H';
                break;
            case 0xA728: // Ꜩ  [LATIN CAPITAL LETTER TZ]
                out[output_pos++] = 'T';
                out[output_pos++] = 'Z';
                break;
            case 0x24AF: // ⒯  [PARENTHESIZED LATIN SMALL LETTER T]
                out[output_pos++] = '(';
                out[output_pos++] = 't';
                out[output_pos++] = ')';
                break;
            case 0x02A8: // ʨ  [LATIN SMALL LETTER TC DIGRAPH WITH CURL]
                out[output_pos++] = 't';
                out[output_pos++] = 'c';
                break;
            case 0x00FE: // þ  [LATIN SMALL LETTER THORN]
            case 0x1D7A: // ᵺ  [LATIN SMALL LETTER TH WITH STRIKETHROUGH]
            case 0xA767: // ꝧ  [LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER]
                out[output_pos++] = 't';
                out[output_pos++] = 'h';
                break;
            case 0x02A6: // ʦ  [LATIN SMALL LETTER TS DIGRAPH]
                out[output_pos++] = 't';
                out[output_pos++] = 's';
                break;
            case 0xA729: // ꜩ  [LATIN SMALL LETTER TZ]
                out[output_pos++] = 't';
                out[output_pos++] = 'z';
                break;
            case 0x00D9: // Ù  [LATIN CAPITAL LETTER U WITH GRAVE]
            case 0x00DA: // Ú  [LATIN CAPITAL LETTER U WITH ACUTE]
            case 0x00DB: // Û  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX]
            case 0x00DC: // Ü  [LATIN CAPITAL LETTER U WITH DIAERESIS]
            case 0x0168: // Ũ  [LATIN CAPITAL LETTER U WITH TILDE]
            case 0x016A: // Ū  [LATIN CAPITAL LETTER U WITH MACRON]
            case 0x016C: // Ŭ  [LATIN CAPITAL LETTER U WITH BREVE]
            case 0x016E: // Ů  [LATIN CAPITAL LETTER U WITH RING ABOVE]
            case 0x0170: // Ű  [LATIN CAPITAL LETTER U WITH DOUBLE ACUTE]
            case 0x0172: // Ų  [LATIN CAPITAL LETTER U WITH OGONEK]
            case 0x01AF: // Ư  [LATIN CAPITAL LETTER U WITH HORN]
            case 0x01D3: // Ǔ  [LATIN CAPITAL LETTER U WITH CARON]
            case 0x01D5: // Ǖ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON]
            case 0x01D7: // Ǘ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE]
            case 0x01D9: // Ǚ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON]
            case 0x01DB: // Ǜ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE]
            case 0x0214: // Ȕ  [LATIN CAPITAL LETTER U WITH DOUBLE GRAVE]
            case 0x0216: // Ȗ  [LATIN CAPITAL LETTER U WITH INVERTED BREVE]
            case 0x0244: // Ʉ  [LATIN CAPITAL LETTER U BAR]
            case 0x1D1C: // ᴜ  [LATIN LETTER SMALL CAPITAL U]
            case 0x1D7E: // ᵾ  [LATIN SMALL CAPITAL LETTER U WITH STROKE]
            case 0x1E72: // Ṳ  [LATIN CAPITAL LETTER U WITH DIAERESIS BELOW]
            case 0x1E74: // Ṵ  [LATIN CAPITAL LETTER U WITH TILDE BELOW]
            case 0x1E76: // Ṷ  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW]
            case 0x1E78: // Ṹ  [LATIN CAPITAL LETTER U WITH TILDE AND ACUTE]
            case 0x1E7A: // Ṻ  [LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS]
            case 0x1EE4: // Ụ  [LATIN CAPITAL LETTER U WITH DOT BELOW]
            case 0x1EE6: // Ủ  [LATIN CAPITAL LETTER U WITH HOOK ABOVE]
            case 0x1EE8: // Ứ  [LATIN CAPITAL LETTER U WITH HORN AND ACUTE]
            case 0x1EEA: // Ừ  [LATIN CAPITAL LETTER U WITH HORN AND GRAVE]
            case 0x1EEC: // Ử  [LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE]
            case 0x1EEE: // Ữ  [LATIN CAPITAL LETTER U WITH HORN AND TILDE]
            case 0x1EF0: // Ự  [LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW]
            case 0x24CA: // Ⓤ  [CIRCLED LATIN CAPITAL LETTER U]
            case 0xFF35: // Ｕ  [FULLWIDTH LATIN CAPITAL LETTER U]
                out[output_pos++] = 'U';
                break;
            case 0x00F9: // ù  [LATIN SMALL LETTER U WITH GRAVE]
            case 0x00FA: // ú  [LATIN SMALL LETTER U WITH ACUTE]
            case 0x00FB: // û  [LATIN SMALL LETTER U WITH CIRCUMFLEX]
            case 0x00FC: // ü  [LATIN SMALL LETTER U WITH DIAERESIS]
            case 0x0169: // ũ  [LATIN SMALL LETTER U WITH TILDE]
            case 0x016B: // ū  [LATIN SMALL LETTER U WITH MACRON]
            case 0x016D: // ŭ  [LATIN SMALL LETTER U WITH BREVE]
            case 0x016F: // ů  [LATIN SMALL LETTER U WITH RING ABOVE]
            case 0x0171: // ű  [LATIN SMALL LETTER U WITH DOUBLE ACUTE]
            case 0x0173: // ų  [LATIN SMALL LETTER U WITH OGONEK]
            case 0x01B0: // ư  [LATIN SMALL LETTER U WITH HORN]
            case 0x01D4: // ǔ  [LATIN SMALL LETTER U WITH CARON]
            case 0x01D6: // ǖ  [LATIN SMALL LETTER U WITH DIAERESIS AND MACRON]
            case 0x01D8: // ǘ  [LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE]
            case 0x01DA: // ǚ  [LATIN SMALL LETTER U WITH DIAERESIS AND CARON]
            case 0x01DC: // ǜ  [LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE]
            case 0x0215: // ȕ  [LATIN SMALL LETTER U WITH DOUBLE GRAVE]
            case 0x0217: // ȗ  [LATIN SMALL LETTER U WITH INVERTED BREVE]
            case 0x0289: // ʉ  [LATIN SMALL LETTER U BAR]
            case 0x1D64: // ᵤ  [LATIN SUBSCRIPT SMALL LETTER U]
            case 0x1D99: // ᶙ  [LATIN SMALL LETTER U WITH RETROFLEX HOOK]
            case 0x1E73: // ṳ  [LATIN SMALL LETTER U WITH DIAERESIS BELOW]
            case 0x1E75: // ṵ  [LATIN SMALL LETTER U WITH TILDE BELOW]
            case 0x1E77: // ṷ  [LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW]
            case 0x1E79: // ṹ  [LATIN SMALL LETTER U WITH TILDE AND ACUTE]
            case 0x1E7B: // ṻ  [LATIN SMALL LETTER U WITH MACRON AND DIAERESIS]
            case 0x1EE5: // ụ  [LATIN SMALL LETTER U WITH DOT BELOW]
            case 0x1EE7: // ủ  [LATIN SMALL LETTER U WITH HOOK ABOVE]
            case 0x1EE9: // ứ  [LATIN SMALL LETTER U WITH HORN AND ACUTE]
            case 0x1EEB: // ừ  [LATIN SMALL LETTER U WITH HORN AND GRAVE]
            case 0x1EED: // ử  [LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE]
            case 0x1EEF: // ữ  [LATIN SMALL LETTER U WITH HORN AND TILDE]
            case 0x1EF1: // ự  [LATIN SMALL LETTER U WITH HORN AND DOT BELOW]
            case 0x24E4: // ⓤ  [CIRCLED LATIN SMALL LETTER U]
            case 0xFF55: // ｕ  [FULLWIDTH LATIN SMALL LETTER U]
                out[output_pos++] = 'u';
                break;
            case 0x24B0: // ⒰  [PARENTHESIZED LATIN SMALL LETTER U]
                out[output_pos++] = '(';
                out[output_pos++] = 'u';
                out[output_pos++] = ')';
                break;
            case 0x1D6B: // ᵫ  [LATIN SMALL LETTER UE]
                out[output_pos++] = 'u';
                out[output_pos++] = 'e';
                break;
            case 0x01B2: // Ʋ  [LATIN CAPITAL LETTER V WITH HOOK]
            case 0x0245: // Ʌ  [LATIN CAPITAL LETTER TURNED V]
            case 0x1D20: // ᴠ  [LATIN LETTER SMALL CAPITAL V]
            case 0x1E7C: // Ṽ  [LATIN CAPITAL LETTER V WITH TILDE]
            case 0x1E7E: // Ṿ  [LATIN CAPITAL LETTER V WITH DOT BELOW]
            case 0x1EFC: // Ỽ  [LATIN CAPITAL LETTER MIDDLE-WELSH V]
            case 0x24CB: // Ⓥ  [CIRCLED LATIN CAPITAL LETTER V]
            case 0xA75E: // Ꝟ  [LATIN CAPITAL LETTER V WITH DIAGONAL STROKE]
            case 0xA768: // Ꝩ  [LATIN CAPITAL LETTER VEND]
            case 0xFF36: // Ｖ  [FULLWIDTH LATIN CAPITAL LETTER V]
                out[output_pos++] = 'V';
                break;
            case 0x028B: // ʋ  [LATIN SMALL LETTER V WITH HOOK]
            case 0x028C: // ʌ  [LATIN SMALL LETTER TURNED V]
            case 0x1D65: // ᵥ  [LATIN SUBSCRIPT SMALL LETTER V]
            case 0x1D8C: // ᶌ  [LATIN SMALL LETTER V WITH PALATAL HOOK]
            case 0x1E7D: // ṽ  [LATIN SMALL LETTER V WITH TILDE]
            case 0x1E7F: // ṿ  [LATIN SMALL LETTER V WITH DOT BELOW]
            case 0x24E5: // ⓥ  [CIRCLED LATIN SMALL LETTER V]
            case 0x2C71: // ⱱ  [LATIN SMALL LETTER V WITH RIGHT HOOK]
            case 0x2C74: // ⱴ  [LATIN SMALL LETTER V WITH CURL]
            case 0xA75F: // ꝟ  [LATIN SMALL LETTER V WITH DIAGONAL STROKE]
            case 0xFF56: // ｖ  [FULLWIDTH LATIN SMALL LETTER V]
                out[output_pos++] = 'v';
                break;
            case 0xA760: // Ꝡ  [LATIN CAPITAL LETTER VY]
                out[output_pos++] = 'V';
                out[output_pos++] = 'Y';
                break;
            case 0x24B1: // ⒱  [PARENTHESIZED LATIN SMALL LETTER V]
                out[output_pos++] = '(';
                out[output_pos++] = 'v';
                out[output_pos++] = ')';
                break;
            case 0xA761: // ꝡ  [LATIN SMALL LETTER VY]
                out[output_pos++] = 'v';
                out[output_pos++] = 'y';
                break;
            case 0x0174: // Ŵ  [LATIN CAPITAL LETTER W WITH CIRCUMFLEX]
            case 0x01F7: // Ƿ  http://en.wikipedia.org/wiki/Wynn  [LATIN CAPITAL LETTER WYNN]
            case 0x1D21: // ᴡ  [LATIN LETTER SMALL CAPITAL W]
            case 0x1E80: // Ẁ  [LATIN CAPITAL LETTER W WITH GRAVE]
            case 0x1E82: // Ẃ  [LATIN CAPITAL LETTER W WITH ACUTE]
            case 0x1E84: // Ẅ  [LATIN CAPITAL LETTER W WITH DIAERESIS]
            case 0x1E86: // Ẇ  [LATIN CAPITAL LETTER W WITH DOT ABOVE]
            case 0x1E88: // Ẉ  [LATIN CAPITAL LETTER W WITH DOT BELOW]
            case 0x24CC: // Ⓦ  [CIRCLED LATIN CAPITAL LETTER W]
            case 0x2C72: // Ⱳ  [LATIN CAPITAL LETTER W WITH HOOK]
            case 0xFF37: // Ｗ  [FULLWIDTH LATIN CAPITAL LETTER W]
                out[output_pos++] = 'W';
                break;
            case 0x0175: // ŵ  [LATIN SMALL LETTER W WITH CIRCUMFLEX]
            case 0x01BF: // ƿ  http://en.wikipedia.org/wiki/Wynn  [LATIN LETTER WYNN]
            case 0x028D: // ʍ  [LATIN SMALL LETTER TURNED W]
            case 0x1E81: // ẁ  [LATIN SMALL LETTER W WITH GRAVE]
            case 0x1E83: // ẃ  [LATIN SMALL LETTER W WITH ACUTE]
            case 0x1E85: // ẅ  [LATIN SMALL LETTER W WITH DIAERESIS]
            case 0x1E87: // ẇ  [LATIN SMALL LETTER W WITH DOT ABOVE]
            case 0x1E89: // ẉ  [LATIN SMALL LETTER W WITH DOT BELOW]
            case 0x1E98: // ẘ  [LATIN SMALL LETTER W WITH RING ABOVE]
            case 0x24E6: // ⓦ  [CIRCLED LATIN SMALL LETTER W]
            case 0x2C73: // ⱳ  [LATIN SMALL LETTER W WITH HOOK]
            case 0xFF57: // ｗ  [FULLWIDTH LATIN SMALL LETTER W]
                out[output_pos++] = 'w';
                break;
            case 0x24B2: // ⒲  [PARENTHESIZED LATIN SMALL LETTER W]
                out[output_pos++] = '(';
                out[output_pos++] = 'w';
                out[output_pos++] = ')';
                break;
            case 0x1E8A: // Ẋ  [LATIN CAPITAL LETTER X WITH DOT ABOVE]
            case 0x1E8C: // Ẍ  [LATIN CAPITAL LETTER X WITH DIAERESIS]
            case 0x24CD: // Ⓧ  [CIRCLED LATIN CAPITAL LETTER X]
            case 0xFF38: // Ｘ  [FULLWIDTH LATIN CAPITAL LETTER X]
                out[output_pos++] = 'X';
                break;
            case 0x1D8D: // ᶍ  [LATIN SMALL LETTER X WITH PALATAL HOOK]
            case 0x1E8B: // ẋ  [LATIN SMALL LETTER X WITH DOT ABOVE]
            case 0x1E8D: // ẍ  [LATIN SMALL LETTER X WITH DIAERESIS]
            case 0x2093: // ₓ  [LATIN SUBSCRIPT SMALL LETTER X]
            case 0x24E7: // ⓧ  [CIRCLED LATIN SMALL LETTER X]
            case 0xFF58: // ｘ  [FULLWIDTH LATIN SMALL LETTER X]
                out[output_pos++] = 'x';
                break;
            case 0x24B3: // ⒳  [PARENTHESIZED LATIN SMALL LETTER X]
                out[output_pos++] = '(';
                out[output_pos++] = 'x';
                out[output_pos++] = ')';
                break;
            case 0x00DD: // Ý  [LATIN CAPITAL LETTER Y WITH ACUTE]
            case 0x0176: // Ŷ  [LATIN CAPITAL LETTER Y WITH CIRCUMFLEX]
            case 0x0178: // Ÿ  [LATIN CAPITAL LETTER Y WITH DIAERESIS]
            case 0x01B3: // Ƴ  [LATIN CAPITAL LETTER Y WITH HOOK]
            case 0x0232: // Ȳ  [LATIN CAPITAL LETTER Y WITH MACRON]
            case 0x024E: // Ɏ  [LATIN CAPITAL LETTER Y WITH STROKE]
            case 0x028F: // ʏ  [LATIN LETTER SMALL CAPITAL Y]
            case 0x1E8E: // Ẏ  [LATIN CAPITAL LETTER Y WITH DOT ABOVE]
            case 0x1EF2: // Ỳ  [LATIN CAPITAL LETTER Y WITH GRAVE]
            case 0x1EF4: // Ỵ  [LATIN CAPITAL LETTER Y WITH DOT BELOW]
            case 0x1EF6: // Ỷ  [LATIN CAPITAL LETTER Y WITH HOOK ABOVE]
            case 0x1EF8: // Ỹ  [LATIN CAPITAL LETTER Y WITH TILDE]
            case 0x1EFE: // Ỿ  [LATIN CAPITAL LETTER Y WITH LOOP]
            case 0x24CE: // Ⓨ  [CIRCLED LATIN CAPITAL LETTER Y]
            case 0xFF39: // Ｙ  [FULLWIDTH LATIN CAPITAL LETTER Y]
                out[output_pos++] = 'Y';
                break;
            case 0x00FD: // ý  [LATIN SMALL LETTER Y WITH ACUTE]
            case 0x00FF: // ÿ  [LATIN SMALL LETTER Y WITH DIAERESIS]
            case 0x0177: // ŷ  [LATIN SMALL LETTER Y WITH CIRCUMFLEX]
            case 0x01B4: // ƴ  [LATIN SMALL LETTER Y WITH HOOK]
            case 0x0233: // ȳ  [LATIN SMALL LETTER Y WITH MACRON]
            case 0x024F: // ɏ  [LATIN SMALL LETTER Y WITH STROKE]
            case 0x028E: // ʎ  [LATIN SMALL LETTER TURNED Y]
            case 0x1E8F: // ẏ  [LATIN SMALL LETTER Y WITH DOT ABOVE]
            case 0x1E99: // ẙ  [LATIN SMALL LETTER Y WITH RING ABOVE]
            case 0x1EF3: // ỳ  [LATIN SMALL LETTER Y WITH GRAVE]
            case 0x1EF5: // ỵ  [LATIN SMALL LETTER Y WITH DOT BELOW]
            case 0x1EF7: // ỷ  [LATIN SMALL LETTER Y WITH HOOK ABOVE]
            case 0x1EF9: // ỹ  [LATIN SMALL LETTER Y WITH TILDE]
            case 0x1EFF: // ỿ  [LATIN SMALL LETTER Y WITH LOOP]
            case 0x24E8: // ⓨ  [CIRCLED LATIN SMALL LETTER Y]
            case 0xFF59: // ｙ  [FULLWIDTH LATIN SMALL LETTER Y]
                out[output_pos++] = 'y';
                break;
            case 0x24B4: // ⒴  [PARENTHESIZED LATIN SMALL LETTER Y]
                out[output_pos++] = '(';
                out[output_pos++] = 'y';
                out[output_pos++] = ')';
                break;
            case 0x0179: // Ź  [LATIN CAPITAL LETTER Z WITH ACUTE]
            case 0x017B: // Ż  [LATIN CAPITAL LETTER Z WITH DOT ABOVE]
            case 0x017D: // Ž  [LATIN CAPITAL LETTER Z WITH CARON]
            case 0x01B5: // Ƶ  [LATIN CAPITAL LETTER Z WITH STROKE]
            case 0x021C: // Ȝ  http://en.wikipedia.org/wiki/Yogh  [LATIN CAPITAL LETTER YOGH]
            case 0x0224: // Ȥ  [LATIN CAPITAL LETTER Z WITH HOOK]
            case 0x1D22: // ᴢ  [LATIN LETTER SMALL CAPITAL Z]
            case 0x1E90: // Ẑ  [LATIN CAPITAL LETTER Z WITH CIRCUMFLEX]
            case 0x1E92: // Ẓ  [LATIN CAPITAL LETTER Z WITH DOT BELOW]
            case 0x1E94: // Ẕ  [LATIN CAPITAL LETTER Z WITH LINE BELOW]
            case 0x24CF: // Ⓩ  [CIRCLED LATIN CAPITAL LETTER Z]
            case 0x2C6B: // Ⱬ  [LATIN CAPITAL LETTER Z WITH DESCENDER]
            case 0xA762: // Ꝣ  [LATIN CAPITAL LETTER VISIGOTHIC Z]
            case 0xFF3A: // Ｚ  [FULLWIDTH LATIN CAPITAL LETTER Z]
                out[output_pos++] = 'Z';
                break;
            case 0x017A: // ź  [LATIN SMALL LETTER Z WITH ACUTE]
            case 0x017C: // ż  [LATIN SMALL LETTER Z WITH DOT ABOVE]
            case 0x017E: // ž  [LATIN SMALL LETTER Z WITH CARON]
            case 0x01B6: // ƶ  [LATIN SMALL LETTER Z WITH STROKE]
            case 0x021D: // ȝ  http://en.wikipedia.org/wiki/Yogh  [LATIN SMALL LETTER YOGH]
            case 0x0225: // ȥ  [LATIN SMALL LETTER Z WITH HOOK]
            case 0x0240: // ɀ  [LATIN SMALL LETTER Z WITH SWASH TAIL]
            case 0x0290: // ʐ  [LATIN SMALL LETTER Z WITH RETROFLEX HOOK]
            case 0x0291: // ʑ  [LATIN SMALL LETTER Z WITH CURL]
            case 0x1D76: // ᵶ  [LATIN SMALL LETTER Z WITH MIDDLE TILDE]
            case 0x1D8E: // ᶎ  [LATIN SMALL LETTER Z WITH PALATAL HOOK]
            case 0x1E91: // ẑ  [LATIN SMALL LETTER Z WITH CIRCUMFLEX]
            case 0x1E93: // ẓ  [LATIN SMALL LETTER Z WITH DOT BELOW]
            case 0x1E95: // ẕ  [LATIN SMALL LETTER Z WITH LINE BELOW]
            case 0x24E9: // ⓩ  [CIRCLED LATIN SMALL LETTER Z]
            case 0x2C6C: // ⱬ  [LATIN SMALL LETTER Z WITH DESCENDER]
            case 0xA763: // ꝣ  [LATIN SMALL LETTER VISIGOTHIC Z]
            case 0xFF5A: // ｚ  [FULLWIDTH LATIN SMALL LETTER Z]
                out[output_pos++] = 'z';
                break;
            case 0x24B5: // ⒵  [PARENTHESIZED LATIN SMALL LETTER Z]
                out[output_pos++] = '(';
                out[output_pos++] = 'z';
                out[output_pos++] = ')';
                break;
            case 0x2070: // ⁰  [SUPERSCRIPT ZERO]
            case 0x2080: // ₀  [SUBSCRIPT ZERO]
            case 0x24EA: // ⓪  [CIRCLED DIGIT ZERO]
            case 0x24FF: // ⓿  [NEGATIVE CIRCLED DIGIT ZERO]
            case 0xFF10: // ０  [FULLWIDTH DIGIT ZERO]
                out[output_pos++] = '0';
                break;
            case 0x00B9: // ¹  [SUPERSCRIPT ONE]
            case 0x2081: // ₁  [SUBSCRIPT ONE]
            case 0x2460: // ①  [CIRCLED DIGIT ONE]
            case 0x24F5: // ⓵  [DOUBLE CIRCLED DIGIT ONE]
            case 0x2776: // ❶  [DINGBAT NEGATIVE CIRCLED DIGIT ONE]
            case 0x2780: // ➀  [DINGBAT CIRCLED SANS-SERIF DIGIT ONE]
            case 0x278A: // ➊  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT ONE]
            case 0xFF11: // １  [FULLWIDTH DIGIT ONE]
                out[output_pos++] = '1';
                break;
            case 0x2488: // ⒈  [DIGIT ONE FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '.';
                break;
            case 0x2474: // ⑴  [PARENTHESIZED DIGIT ONE]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = ')';
                break;
            case 0x00B2: // ²  [SUPERSCRIPT TWO]
            case 0x2082: // ₂  [SUBSCRIPT TWO]
            case 0x2461: // ②  [CIRCLED DIGIT TWO]
            case 0x24F6: // ⓶  [DOUBLE CIRCLED DIGIT TWO]
            case 0x2777: // ❷  [DINGBAT NEGATIVE CIRCLED DIGIT TWO]
            case 0x2781: // ➁  [DINGBAT CIRCLED SANS-SERIF DIGIT TWO]
            case 0x278B: // ➋  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT TWO]
            case 0xFF12: // ２  [FULLWIDTH DIGIT TWO]
                out[output_pos++] = '2';
                break;
            case 0x2489: // ⒉  [DIGIT TWO FULL STOP]
                out[output_pos++] = '2';
                out[output_pos++] = '.';
                break;
            case 0x2475: // ⑵  [PARENTHESIZED DIGIT TWO]
                out[output_pos++] = '(';
                out[output_pos++] = '2';
                out[output_pos++] = ')';
                break;
            case 0x00B3: // ³  [SUPERSCRIPT THREE]
            case 0x2083: // ₃  [SUBSCRIPT THREE]
            case 0x2462: // ③  [CIRCLED DIGIT THREE]
            case 0x24F7: // ⓷  [DOUBLE CIRCLED DIGIT THREE]
            case 0x2778: // ❸  [DINGBAT NEGATIVE CIRCLED DIGIT THREE]
            case 0x2782: // ➂  [DINGBAT CIRCLED SANS-SERIF DIGIT THREE]
            case 0x278C: // ➌  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT THREE]
            case 0xFF13: // ３  [FULLWIDTH DIGIT THREE]
                out[output_pos++] = '3';
                break;
            case 0x248A: // ⒊  [DIGIT THREE FULL STOP]
                out[output_pos++] = '3';
                out[output_pos++] = '.';
                break;
            case 0x2476: // ⑶  [PARENTHESIZED DIGIT THREE]
                out[output_pos++] = '(';
                out[output_pos++] = '3';
                out[output_pos++] = ')';
                break;
            case 0x2074: // ⁴  [SUPERSCRIPT FOUR]
            case 0x2084: // ₄  [SUBSCRIPT FOUR]
            case 0x2463: // ④  [CIRCLED DIGIT FOUR]
            case 0x24F8: // ⓸  [DOUBLE CIRCLED DIGIT FOUR]
            case 0x2779: // ❹  [DINGBAT NEGATIVE CIRCLED DIGIT FOUR]
            case 0x2783: // ➃  [DINGBAT CIRCLED SANS-SERIF DIGIT FOUR]
            case 0x278D: // ➍  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FOUR]
            case 0xFF14: // ４  [FULLWIDTH DIGIT FOUR]
                out[output_pos++] = '4';
                break;
            case 0x248B: // ⒋  [DIGIT FOUR FULL STOP]
                out[output_pos++] = '4';
                out[output_pos++] = '.';
                break;
            case 0x2477: // ⑷  [PARENTHESIZED DIGIT FOUR]
                out[output_pos++] = '(';
                out[output_pos++] = '4';
                out[output_pos++] = ')';
                break;
            case 0x2075: // ⁵  [SUPERSCRIPT FIVE]
            case 0x2085: // ₅  [SUBSCRIPT FIVE]
            case 0x2464: // ⑤  [CIRCLED DIGIT FIVE]
            case 0x24F9: // ⓹  [DOUBLE CIRCLED DIGIT FIVE]
            case 0x277A: // ❺  [DINGBAT NEGATIVE CIRCLED DIGIT FIVE]
            case 0x2784: // ➄  [DINGBAT CIRCLED SANS-SERIF DIGIT FIVE]
            case 0x278E: // ➎  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FIVE]
            case 0xFF15: // ５  [FULLWIDTH DIGIT FIVE]
                out[output_pos++] = '5';
                break;
            case 0x248C: // ⒌  [DIGIT FIVE FULL STOP]
                out[output_pos++] = '5';
                out[output_pos++] = '.';
                break;
            case 0x2478: // ⑸  [PARENTHESIZED DIGIT FIVE]
                out[output_pos++] = '(';
                out[output_pos++] = '5';
                out[output_pos++] = ')';
                break;
            case 0x2076: // ⁶  [SUPERSCRIPT SIX]
            case 0x2086: // ₆  [SUBSCRIPT SIX]
            case 0x2465: // ⑥  [CIRCLED DIGIT SIX]
            case 0x24FA: // ⓺  [DOUBLE CIRCLED DIGIT SIX]
            case 0x277B: // ❻  [DINGBAT NEGATIVE CIRCLED DIGIT SIX]
            case 0x2785: // ➅  [DINGBAT CIRCLED SANS-SERIF DIGIT SIX]
            case 0x278F: // ➏  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SIX]
            case 0xFF16: // ６  [FULLWIDTH DIGIT SIX]
                out[output_pos++] = '6';
                break;
            case 0x248D: // ⒍  [DIGIT SIX FULL STOP]
                out[output_pos++] = '6';
                out[output_pos++] = '.';
                break;
            case 0x2479: // ⑹  [PARENTHESIZED DIGIT SIX]
                out[output_pos++] = '(';
                out[output_pos++] = '6';
                out[output_pos++] = ')';
                break;
            case 0x2077: // ⁷  [SUPERSCRIPT SEVEN]
            case 0x2087: // ₇  [SUBSCRIPT SEVEN]
            case 0x2466: // ⑦  [CIRCLED DIGIT SEVEN]
            case 0x24FB: // ⓻  [DOUBLE CIRCLED DIGIT SEVEN]
            case 0x277C: // ❼  [DINGBAT NEGATIVE CIRCLED DIGIT SEVEN]
            case 0x2786: // ➆  [DINGBAT CIRCLED SANS-SERIF DIGIT SEVEN]
            case 0x2790: // ➐  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SEVEN]
            case 0xFF17: // ７  [FULLWIDTH DIGIT SEVEN]
                out[output_pos++] = '7';
                break;
            case 0x248E: // ⒎  [DIGIT SEVEN FULL STOP]
                out[output_pos++] = '7';
                out[output_pos++] = '.';
                break;
            case 0x247A: // ⑺  [PARENTHESIZED DIGIT SEVEN]
                out[output_pos++] = '(';
                out[output_pos++] = '7';
                out[output_pos++] = ')';
                break;
            case 0x2078: // ⁸  [SUPERSCRIPT EIGHT]
            case 0x2088: // ₈  [SUBSCRIPT EIGHT]
            case 0x2467: // ⑧  [CIRCLED DIGIT EIGHT]
            case 0x24FC: // ⓼  [DOUBLE CIRCLED DIGIT EIGHT]
            case 0x277D: // ❽  [DINGBAT NEGATIVE CIRCLED DIGIT EIGHT]
            case 0x2787: // ➇  [DINGBAT CIRCLED SANS-SERIF DIGIT EIGHT]
            case 0x2791: // ➑  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT EIGHT]
            case 0xFF18: // ８  [FULLWIDTH DIGIT EIGHT]
                out[output_pos++] = '8';
                break;
            case 0x248F: // ⒏  [DIGIT EIGHT FULL STOP]
                out[output_pos++] = '8';
                out[output_pos++] = '.';
                break;
            case 0x247B: // ⑻  [PARENTHESIZED DIGIT EIGHT]
                out[output_pos++] = '(';
                out[output_pos++] = '8';
                out[output_pos++] = ')';
                break;
            case 0x2079: // ⁹  [SUPERSCRIPT NINE]
            case 0x2089: // ₉  [SUBSCRIPT NINE]
            case 0x2468: // ⑨  [CIRCLED DIGIT NINE]
            case 0x24FD: // ⓽  [DOUBLE CIRCLED DIGIT NINE]
            case 0x277E: // ❾  [DINGBAT NEGATIVE CIRCLED DIGIT NINE]
            case 0x2788: // ➈  [DINGBAT CIRCLED SANS-SERIF DIGIT NINE]
            case 0x2792: // ➒  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT NINE]
            case 0xFF19: // ９  [FULLWIDTH DIGIT NINE]
                out[output_pos++] = '9';
                break;
            case 0x2490: // ⒐  [DIGIT NINE FULL STOP]
                out[output_pos++] = '9';
                out[output_pos++] = '.';
                break;
            case 0x247C: // ⑼  [PARENTHESIZED DIGIT NINE]
                out[output_pos++] = '(';
                out[output_pos++] = '9';
                out[output_pos++] = ')';
                break;
            case 0x2469: // ⑩  [CIRCLED NUMBER TEN]
            case 0x24FE: // ⓾  [DOUBLE CIRCLED NUMBER TEN]
            case 0x277F: // ❿  [DINGBAT NEGATIVE CIRCLED NUMBER TEN]
            case 0x2789: // ➉  [DINGBAT CIRCLED SANS-SERIF NUMBER TEN]
            case 0x2793: // ➓  [DINGBAT NEGATIVE CIRCLED SANS-SERIF NUMBER TEN]
                out[output_pos++] = '1';
                out[output_pos++] = '0';
                break;
            case 0x2491: // ⒑  [NUMBER TEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '0';
                out[output_pos++] = '.';
                break;
            case 0x247D: // ⑽  [PARENTHESIZED NUMBER TEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '0';
                out[output_pos++] = ')';
                break;
            case 0x246A: // ⑪  [CIRCLED NUMBER ELEVEN]
            case 0x24EB: // ⓫  [NEGATIVE CIRCLED NUMBER ELEVEN]
                out[output_pos++] = '1';
                out[output_pos++] = '1';
                break;
            case 0x2492: // ⒒  [NUMBER ELEVEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '1';
                out[output_pos++] = '.';
                break;
            case 0x247E: // ⑾  [PARENTHESIZED NUMBER ELEVEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '1';
                out[output_pos++] = ')';
                break;
            case 0x246B: // ⑫  [CIRCLED NUMBER TWELVE]
            case 0x24EC: // ⓬  [NEGATIVE CIRCLED NUMBER TWELVE]
                out[output_pos++] = '1';
                out[output_pos++] = '2';
                break;
            case 0x2493: // ⒓  [NUMBER TWELVE FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '2';
                out[output_pos++] = '.';
                break;
            case 0x247F: // ⑿  [PARENTHESIZED NUMBER TWELVE]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '2';
                out[output_pos++] = ')';
                break;
            case 0x246C: // ⑬  [CIRCLED NUMBER THIRTEEN]
            case 0x24ED: // ⓭  [NEGATIVE CIRCLED NUMBER THIRTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '3';
                break;
            case 0x2494: // ⒔  [NUMBER THIRTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '3';
                out[output_pos++] = '.';
                break;
            case 0x2480: // ⒀  [PARENTHESIZED NUMBER THIRTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '3';
                out[output_pos++] = ')';
                break;
            case 0x246D: // ⑭  [CIRCLED NUMBER FOURTEEN]
            case 0x24EE: // ⓮  [NEGATIVE CIRCLED NUMBER FOURTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '4';
                break;
            case 0x2495: // ⒕  [NUMBER FOURTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '4';
                out[output_pos++] = '.';
                break;
            case 0x2481: // ⒁  [PARENTHESIZED NUMBER FOURTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '4';
                out[output_pos++] = ')';
                break;
            case 0x246E: // ⑮  [CIRCLED NUMBER FIFTEEN]
            case 0x24EF: // ⓯  [NEGATIVE CIRCLED NUMBER FIFTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '5';
                break;
            case 0x2496: // ⒖  [NUMBER FIFTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '5';
                out[output_pos++] = '.';
                break;
            case 0x2482: // ⒂  [PARENTHESIZED NUMBER FIFTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '5';
                out[output_pos++] = ')';
                break;
            case 0x246F: // ⑯  [CIRCLED NUMBER SIXTEEN]
            case 0x24F0: // ⓰  [NEGATIVE CIRCLED NUMBER SIXTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '6';
                break;
            case 0x2497: // ⒗  [NUMBER SIXTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '6';
                out[output_pos++] = '.';
                break;
            case 0x2483: // ⒃  [PARENTHESIZED NUMBER SIXTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '6';
                out[output_pos++] = ')';
                break;
            case 0x2470: // ⑰  [CIRCLED NUMBER SEVENTEEN]
            case 0x24F1: // ⓱  [NEGATIVE CIRCLED NUMBER SEVENTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '7';
                break;
            case 0x2498: // ⒘  [NUMBER SEVENTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '7';
                out[output_pos++] = '.';
                break;
            case 0x2484: // ⒄  [PARENTHESIZED NUMBER SEVENTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '7';
                out[output_pos++] = ')';
                break;
            case 0x2471: // ⑱  [CIRCLED NUMBER EIGHTEEN]
            case 0x24F2: // ⓲  [NEGATIVE CIRCLED NUMBER EIGHTEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '8';
                break;
            case 0x2499: // ⒙  [NUMBER EIGHTEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '8';
                out[output_pos++] = '.';
                break;
            case 0x2485: // ⒅  [PARENTHESIZED NUMBER EIGHTEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '8';
                out[output_pos++] = ')';
                break;
            case 0x2472: // ⑲  [CIRCLED NUMBER NINETEEN]
            case 0x24F3: // ⓳  [NEGATIVE CIRCLED NUMBER NINETEEN]
                out[output_pos++] = '1';
                out[output_pos++] = '9';
                break;
            case 0x249A: // ⒚  [NUMBER NINETEEN FULL STOP]
                out[output_pos++] = '1';
                out[output_pos++] = '9';
                out[output_pos++] = '.';
                break;
            case 0x2486: // ⒆  [PARENTHESIZED NUMBER NINETEEN]
                out[output_pos++] = '(';
                out[output_pos++] = '1';
                out[output_pos++] = '9';
                out[output_pos++] = ')';
                break;
            case 0x2473: // ⑳  [CIRCLED NUMBER TWENTY]
            case 0x24F4: // ⓴  [NEGATIVE CIRCLED NUMBER TWENTY]
                out[output_pos++] = '2';
                out[output_pos++] = '0';
                break;
            case 0x249B: // ⒛  [NUMBER TWENTY FULL STOP]
                out[output_pos++] = '2';
                out[output_pos++] = '0';
                out[output_pos++] = '.';
                break;
            case 0x2487: // ⒇  [PARENTHESIZED NUMBER TWENTY]
                out[output_pos++] = '(';
                out[output_pos++] = '2';
                out[output_pos++] = '0';
                out[output_pos++] = ')';
                break;
            case 0x00AB: // «  [LEFT-POINTING DOUBLE ANGLE QUOTATION MARK]
            case 0x00BB: // »  [RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK]
            case 0x201C: // “  [LEFT DOUBLE QUOTATION MARK]
            case 0x201D: // ”  [RIGHT DOUBLE QUOTATION MARK]
            case 0x201E: // „  [DOUBLE LOW-9 QUOTATION MARK]
            case 0x2033: // ″  [DOUBLE PRIME]
            case 0x2036: // ‶  [REVERSED DOUBLE PRIME]
            case 0x275D: // ❝  [HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT]
            case 0x275E: // ❞  [HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT]
            case 0x276E: // ❮  [HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT]
            case 0x276F: // ❯  [HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT]
            case 0xFF02: // ＂  [FULLWIDTH QUOTATION MARK]
                out[output_pos++] = '"';
                break;
            case 0x2018: // ‘  [LEFT SINGLE QUOTATION MARK]
            case 0x2019: // ’  [RIGHT SINGLE QUOTATION MARK]
            case 0x201A: // ‚  [SINGLE LOW-9 QUOTATION MARK]
            case 0x201B: // ‛  [SINGLE HIGH-REVERSED-9 QUOTATION MARK]
            case 0x2032: // ′  [PRIME]
            case 0x2035: // ‵  [REVERSED PRIME]
            case 0x2039: // ‹  [SINGLE LEFT-POINTING ANGLE QUOTATION MARK]
            case 0x203A: // ›  [SINGLE RIGHT-POINTING ANGLE QUOTATION MARK]
            case 0x275B: // ❛  [HEAVY SINGLE TURNED COMMA QUOTATION MARK ORNAMENT]
            case 0x275C: // ❜  [HEAVY SINGLE COMMA QUOTATION MARK ORNAMENT]
            case 0xFF07: // ＇  [FULLWIDTH APOSTROPHE]
                out[output_pos++] = '\'';
                break;
            case 0x2010: // ‐  [HYPHEN]
            case 0x2011: // ‑  [NON-BREAKING HYPHEN]
            case 0x2012: // ‒  [FIGURE DASH]
            case 0x2013: // –  [EN DASH]
            case 0x2014: // —  [EM DASH]
            case 0x207B: // ⁻  [SUPERSCRIPT MINUS]
            case 0x208B: // ₋  [SUBSCRIPT MINUS]
            case 0xFF0D: // －  [FULLWIDTH HYPHEN-MINUS]
                out[output_pos++] = '-';
                break;
            case 0x2045: // ⁅  [LEFT SQUARE BRACKET WITH QUILL]
            case 0x2772: // ❲  [LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT]
            case 0xFF3B: // ［  [FULLWIDTH LEFT SQUARE BRACKET]
                out[output_pos++] = '[';
                break;
            case 0x2046: // ⁆  [RIGHT SQUARE BRACKET WITH QUILL]
            case 0x2773: // ❳  [LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT]
            case 0xFF3D: // ］  [FULLWIDTH RIGHT SQUARE BRACKET]
                out[output_pos++] = ']';
                break;
            case 0x207D: // ⁽  [SUPERSCRIPT LEFT PARENTHESIS]
            case 0x208D: // ₍  [SUBSCRIPT LEFT PARENTHESIS]
            case 0x2768: // ❨  [MEDIUM LEFT PARENTHESIS ORNAMENT]
            case 0x276A: // ❪  [MEDIUM FLATTENED LEFT PARENTHESIS ORNAMENT]
            case 0xFF08: // （  [FULLWIDTH LEFT PARENTHESIS]
                out[output_pos++] = '(';
                break;
            case 0x2E28: // ⸨  [LEFT DOUBLE PARENTHESIS]
                out[output_pos++] = '(';
                out[output_pos++] = '(';
                break;
            case 0x207E: // ⁾  [SUPERSCRIPT RIGHT PARENTHESIS]
            case 0x208E: // ₎  [SUBSCRIPT RIGHT PARENTHESIS]
            case 0x2769: // ❩  [MEDIUM RIGHT PARENTHESIS ORNAMENT]
            case 0x276B: // ❫  [MEDIUM FLATTENED RIGHT PARENTHESIS ORNAMENT]
            case 0xFF09: // ）  [FULLWIDTH RIGHT PARENTHESIS]
                out[output_pos++] = ')';
                break;
            case 0x2E29: // ⸩  [RIGHT DOUBLE PARENTHESIS]
                out[output_pos++] = ')';
                out[output_pos++] = ')';
                break;
            case 0x276C: // ❬  [MEDIUM LEFT-POINTING ANGLE BRACKET ORNAMENT]
            case 0x2770: // ❰  [HEAVY LEFT-POINTING ANGLE BRACKET ORNAMENT]
            case 0xFF1C: // ＜  [FULLWIDTH LESS-THAN SIGN]
                out[output_pos++] = '<';
                break;
            case 0x276D: // ❭  [MEDIUM RIGHT-POINTING ANGLE BRACKET ORNAMENT]
            case 0x2771: // ❱  [HEAVY RIGHT-POINTING ANGLE BRACKET ORNAMENT]
            case 0xFF1E: // ＞  [FULLWIDTH GREATER-THAN SIGN]
                out[output_pos++] = '>';
                break;
            case 0x2774: // ❴  [MEDIUM LEFT CURLY BRACKET ORNAMENT]
            case 0xFF5B: // ｛  [FULLWIDTH LEFT CURLY BRACKET]
                out[output_pos++] = '{';
                break;
            case 0x2775: // ❵  [MEDIUM RIGHT CURLY BRACKET ORNAMENT]
            case 0xFF5D: // ｝  [FULLWIDTH RIGHT CURLY BRACKET]
                out[output_pos++] = '}';
                break;
            case 0x207A: // ⁺  [SUPERSCRIPT PLUS SIGN]
            case 0x208A: // ₊  [SUBSCRIPT PLUS SIGN]
            case 0xFF0B: // ＋  [FULLWIDTH PLUS SIGN]
                out[output_pos++] = '+';
                break;
            case 0x207C: // ⁼  [SUPERSCRIPT EQUALS SIGN]
            case 0x208C: // ₌  [SUBSCRIPT EQUALS SIGN]
            case 0xFF1D: // ＝  [FULLWIDTH EQUALS SIGN]
                out[output_pos++] = '=';
                break;
            case 0xFF01: // ！  [FULLWIDTH EXCLAMATION MARK]
                out[output_pos++] = '!';
                break;
            case 0x203C: // ‼  [DOUBLE EXCLAMATION MARK]
                out[output_pos++] = '!';
                out[output_pos++] = '!';
                break;
            case 0x2049: // ⁉  [EXCLAMATION QUESTION MARK]
                out[output_pos++] = '!';
                out[output_pos++] = '?';
                break;
            case 0xFF03: // ＃  [FULLWIDTH NUMBER SIGN]
                out[output_pos++] = '#';
                break;
            case 0xFF04: // ＄  [FULLWIDTH DOLLAR SIGN]
                out[output_pos++] = '$';
                break;
            case 0x2052: // ⁒  [COMMERCIAL MINUS SIGN]
            case 0xFF05: // ％  [FULLWIDTH PERCENT SIGN]
                out[output_pos++] = '%';
                break;
            case 0xFF06: // ＆  [FULLWIDTH AMPERSAND]
                out[output_pos++] = '&';
                break;
            case 0x204E: // ⁎  [LOW ASTERISK]
            case 0xFF0A: // ＊  [FULLWIDTH ASTERISK]
                out[output_pos++] = '*';
                break;
            case 0xFF0C: // ，  [FULLWIDTH COMMA]
                out[output_pos++] = ',';
                break;
            case 0xFF0E: // ．  [FULLWIDTH FULL STOP]
                out[output_pos++] = '.';
                break;
            case 0x2044: // ⁄  [FRACTION SLASH]
            case 0xFF0F: // ／  [FULLWIDTH SOLIDUS]
                out[output_pos++] = '/';
                break;
            case 0xFF1A: // ：  [FULLWIDTH COLON]
                out[output_pos++] = ':';
                break;
            case 0x204F: // ⁏  [REVERSED SEMICOLON]
            case 0xFF1B: // ；  [FULLWIDTH SEMICOLON]
                out[output_pos++] = ';';
                break;
            case 0xFF1F: // ？  [FULLWIDTH QUESTION MARK]
                out[output_pos++] = '?';
                break;
            case 0x2047: // ⁇  [DOUBLE QUESTION MARK]
                out[output_pos++] = '?';
                out[output_pos++] = '?';
                break;
            case 0x2048: // ⁈  [QUESTION EXCLAMATION MARK]
                out[output_pos++] = '?';
                out[output_pos++] = '!';
                break;
            case 0xFF20: // ＠  [FULLWIDTH COMMERCIAL AT]
                out[output_pos++] = '@';
                break;
            case 0xFF3C: // ＼  [FULLWIDTH REVERSE SOLIDUS]
                out[output_pos++] = '\\';
                break;
            case 0x2038: // ‸  [CARET]
            case 0xFF3E: // ＾  [FULLWIDTH CIRCUMFLEX ACCENT]
                out[output_pos++] = '^';
                break;
            case 0xFF3F: // ＿  [FULLWIDTH LOW LINE]
                out[output_pos++] = '_';
                break;
            case 0x2053: // ⁓  [SWUNG DASH]
            case 0xFF5E: // ～  [FULLWIDTH TILDE]
                out[output_pos++] = '~';
                break;
            default: {
                for (size_t i = prev_pos; i < pos; i++) {
                    out[output_pos++] = in[i];
                }
            } break;
            }
        }
    }
    return output_pos;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index