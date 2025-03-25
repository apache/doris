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

#include <unicode/uchar.h>
#include <unicode/unistr.h>

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace doris::segment_v2::inverted_index {

class StandardTokenizerImpl {
public:
    StandardTokenizerImpl() = default;
    ~StandardTokenizerImpl() = default;

    int32_t get_next_token() {
        int32_t zzInput = 0;
        int32_t zzAction = 0;

        int32_t zzCurrentPosL = 0;
        int32_t zzMarkedPosL = 0;
        int32_t zzEndReadL = zzEndRead;

        while (true) {
            zzMarkedPosL = zzMarkedPos;

            zzAction = -1;

            zzCurrentPosL = zzCurrentPos = zzStartRead = zzMarkedPosL;

            zzState = ZZ_LEXSTATE[zzLexicalState];

            int32_t zzAttributes = ZZ_ATTRIBUTE[zzState];
            if ((zzAttributes & 1) == 1) {
                zzAction = zzState;
            }

            while (true) {
                if (zzCurrentPosL < zzEndReadL) {
                    UChar32 c = U_UNASSIGNED;
                    U8_NEXT(zzBuffer, zzCurrentPosL, zzEndReadL, c);
                    if (c < 0) {
                        return YYEOF;
                    }
                    zzInput = c;
                } else if (zzAtEOF) {
                    zzInput = YYEOF;
                    break;
                } else {
                    zzCurrentPos = zzCurrentPosL;
                    zzMarkedPos = zzMarkedPosL;
                    zzEndReadL = zzEndRead;
                    zzInput = YYEOF;
                    break;
                }
                int32_t zzNext = ZZ_TRANS[ZZ_ROWMAP[zzState] + zzCMap(zzInput)];
                if (zzNext == -1) {
                    break;
                }
                zzState = zzNext;

                zzAttributes = ZZ_ATTRIBUTE[zzState];
                if ((zzAttributes & 1) == 1) {
                    zzAction = zzState;
                    zzMarkedPosL = zzCurrentPosL;
                    if ((zzAttributes & 8) == 8) {
                        break;
                    }
                }
            }

            zzMarkedPos = zzMarkedPosL;

            if (zzInput == YYEOF && zzStartRead == zzCurrentPos) {
                zzAtEOF = true;
                return YYEOF;
            } else {
                switch (zzAction < 0 ? zzAction : ZZ_ACTION[zzAction]) {
                case 1: {
                    break;
                }
                case 10:
                    break;
                case 2: {
                    return NUMERIC_TYPE;
                }
                case 11:
                    break;
                case 3: {
                    return WORD_TYPE;
                }
                case 12:
                    break;
                case 4: {
                    return EMOJI_TYPE;
                }
                case 13:
                    break;
                case 5: {
                    return SOUTH_EAST_ASIAN_TYPE;
                }
                case 14:
                    break;
                case 6: {
                    return HANGUL_TYPE;
                }
                case 15:
                    break;
                case 7: {
                    return IDEOGRAPHIC_TYPE;
                }
                case 16:
                    break;
                case 8: {
                    return KATAKANA_TYPE;
                }
                case 17:
                    break;
                case 9: {
                    return HIRAGANA_TYPE;
                }
                case 18:
                    break;
                default:
                    return YYEOF;
                }
            }
        }
    }

    inline std::string_view get_text() {
        return {zzBuffer.data() + zzStartRead, (size_t)(zzMarkedPos - zzStartRead)};
    }

    inline void yyreset(const std::string_view& buffer) {
        zzBuffer = buffer;
        zzEndRead = zzBuffer.size();
        zzAtEOF = false;
        zzCurrentPos = 0;
        zzMarkedPos = 0;
        zzStartRead = 0;
        zzLexicalState = YYINITIAL;
    }

    inline int32_t yylength() const { return zzMarkedPos - zzStartRead; }

private:
    inline int32_t zzCMap(int32_t input) {
        int32_t offset = input & 255;
        return offset == input ? ZZ_CMAP_BLOCKS[offset]
                               : ZZ_CMAP_BLOCKS[ZZ_CMAP_TOP[input >> 8] | offset];
    }

public:
    static constexpr int32_t YYEOF = -1;
    static constexpr int32_t YYINITIAL = 0;

    static constexpr int32_t WORD_TYPE = 0;
    static constexpr int32_t NUMERIC_TYPE = 1;
    static constexpr int32_t SOUTH_EAST_ASIAN_TYPE = 2;
    static constexpr int32_t IDEOGRAPHIC_TYPE = 3;
    static constexpr int32_t HIRAGANA_TYPE = 4;
    static constexpr int32_t KATAKANA_TYPE = 5;
    static constexpr int32_t HANGUL_TYPE = 6;
    static constexpr int32_t EMOJI_TYPE = 7;

private:
    static constexpr int32_t ZZ_UNKNOWN_ERROR = 0;
    static constexpr int32_t ZZ_NO_MATCH = 1;
    static constexpr int32_t ZZ_PUSHBACK_2BIG = 2;

    static const std::vector<int32_t> ZZ_LEXSTATE;
    static const std::vector<int32_t> ZZ_CMAP_TOP;

    static const std::vector<int32_t> ZZ_CMAP_BLOCKS;
    static const std::vector<int32_t> ZZ_ACTION;
    static const std::vector<int32_t> ZZ_ROWMAP;
    static const std::vector<int32_t> ZZ_TRANS;
    static const std::vector<int32_t> ZZ_ATTRIBUTE;

    std::string_view zzBuffer;

    int32_t zzState = 0;
    int32_t zzLexicalState = YYINITIAL;
    int32_t zzMarkedPos = 0;
    int32_t zzCurrentPos = 0;
    int32_t zzStartRead = 0;
    int32_t zzEndRead = 0;
    bool zzAtEOF = false;
};
using StandardTokenizerImplPtr = std::unique_ptr<StandardTokenizerImpl>;

} // namespace doris::segment_v2::inverted_index