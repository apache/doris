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

#include <memory>
#include <string_view>
#include <vector>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class StandardTokenizerImpl {
public:
    StandardTokenizerImpl() : _zz_buffer(ZZ_BUFFERSIZE, 0) {}

    ~StandardTokenizerImpl() = default;

    int32_t get_next_token() {
        int32_t _zz_input = 0;
        int32_t _zz_action = 0;

        int32_t _zz_current_pos_l = 0;
        int32_t _zz_marked_pos_l = 0;
        int32_t _zz_end_read_l = _zz_end_read;
        const char* _zz_buffer_l = _zz_buffer.data();

        while (true) {
            _zz_marked_pos_l = _zz_marked_pos;
            _zz_action = -1;

            _zz_current_pos_l = _zz_current_pos = _zz_start_read = _zz_marked_pos_l;
            _zz_state = ZZ_LEXSTATE[_zz_lexical_state];

            int32_t _zz_attributes = ZZ_ATTRIBUTE[_zz_state];
            if ((_zz_attributes & 1) == 1) {
                _zz_action = _zz_state;
            }

            while (true) {
                if (_zz_current_pos_l < _zz_end_read_l) {
                    UChar32 c = U_UNASSIGNED;
                    U8_NEXT(_zz_buffer_l, _zz_current_pos_l, _zz_end_read_l, c);
                    if (c < 0) {
                        return YYEOF;
                    }
                    _zz_input = c;
                } else if (_zz_at_eof) {
                    _zz_input = YYEOF;
                    break;
                } else {
                    _zz_current_pos = _zz_current_pos_l;
                    _zz_marked_pos = _zz_marked_pos_l;
                    bool eof = zz_refill();
                    _zz_current_pos_l = _zz_current_pos;
                    _zz_marked_pos_l = _zz_marked_pos;
                    _zz_buffer_l = _zz_buffer.data();
                    _zz_end_read_l = _zz_end_read;

                    if (eof) {
                        _zz_input = YYEOF;
                        break;
                    } else {
                        UChar32 c = U_UNASSIGNED;
                        U8_NEXT(_zz_buffer_l, _zz_current_pos_l, _zz_end_read_l, c);
                        if (c < 0) {
                            break;
                        }
                        _zz_input = c;
                    }
                }
                int32_t zzNext = ZZ_TRANS[ZZ_ROWMAP[_zz_state] + zz_cmap(_zz_input)];
                if (zzNext == -1) {
                    break;
                }
                _zz_state = zzNext;

                _zz_attributes = ZZ_ATTRIBUTE[_zz_state];
                if ((_zz_attributes & 1) == 1) {
                    _zz_action = _zz_state;
                    _zz_marked_pos_l = _zz_current_pos_l;
                    if ((_zz_attributes & 8) == 8) {
                        break;
                    }
                }
            }

            _zz_marked_pos = _zz_marked_pos_l;

            if (_zz_input == YYEOF && _zz_start_read == _zz_current_pos) {
                _zz_at_eof = true;
                return YYEOF;
            } else {
                switch (_zz_action < 0 ? _zz_action : ZZ_ACTION[_zz_action]) {
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
        return {_zz_buffer.data() + _zz_start_read, (size_t)(_zz_marked_pos - _zz_start_read)};
    }

    inline void yyreset(CL_NS(util)::Reader* reader) {
        _zz_reader = reader;
        _zz_at_eof = false;
        _zz_current_pos = 0;
        _zz_marked_pos = 0;
        _zz_start_read = 0;
        _zz_end_read = 0;
        _zz_final_partial_char = 0;
        _zz_lexical_state = YYINITIAL;
    }

    inline int32_t yylength() const { return _zz_marked_pos - _zz_start_read; }

    void set_buffer_size(int32_t num_chars) { _zz_buffer.resize(num_chars); }

private:
    inline int32_t zz_cmap(int32_t input) {
        int32_t offset = input & 255;
        return offset == input ? ZZ_CMAP_BLOCKS[offset]
                               : ZZ_CMAP_BLOCKS[ZZ_CMAP_TOP[input >> 8] | offset];
    }

    bool zz_refill() {
        if (_zz_start_read > 0) {
            _zz_end_read += _zz_final_partial_char;
            _zz_final_partial_char = 0;

            std::copy(_zz_buffer.begin() + _zz_start_read, _zz_buffer.begin() + _zz_end_read,
                      _zz_buffer.begin());

            _zz_end_read -= _zz_start_read;
            _zz_current_pos -= _zz_start_read;
            _zz_marked_pos -= _zz_start_read;
            _zz_start_read = 0;
        }

        int32_t requested =
                static_cast<int32_t>(_zz_buffer.size()) - _zz_end_read - _zz_final_partial_char;
        if (requested == 0) {
            return true;
        }

        int32_t _num_read =
                _zz_reader->readCopy((const void**)_zz_buffer.data(), _zz_end_read, requested);
        if (_num_read <= 0) {
            return true;
        }

        if (_num_read > 0) {
            _zz_end_read += _num_read;

            int32_t pos = _zz_end_read - 1;
            int32_t trail_bytes = 0;
            while (pos >= 0 && U8_IS_TRAIL(static_cast<uint8_t>(_zz_buffer[pos]))) {
                --pos;
                ++trail_bytes;
            }

            if (pos >= 0) {
                const auto lead_byte = static_cast<uint8_t>(_zz_buffer[pos]);
                int32_t expected_trails = U8_COUNT_TRAIL_BYTES(lead_byte);

                if (expected_trails >= 0 && expected_trails <= 3) {
                    int32_t char_length = 1 + expected_trails;
                    if (pos + char_length > _zz_end_read) {
                        if (_num_read == requested) {
                            _zz_final_partial_char = _zz_end_read - pos;
                            _zz_end_read = pos;
                            if (_num_read == _zz_final_partial_char) {
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                    if (trail_bytes != 0 && expected_trails != 0 && trail_bytes > expected_trails) {
                        return true;
                    }
                    return false;
                }
            }
        }

        return true;
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

    static const int32_t ZZ_BUFFERSIZE = 255;

    CL_NS(util)::Reader* _zz_reader = nullptr;
    std::string _zz_buffer;

    int32_t _zz_state = 0;
    int32_t _zz_lexical_state = YYINITIAL;
    int32_t _zz_marked_pos = 0;
    int32_t _zz_current_pos = 0;
    int32_t _zz_start_read = 0;
    int32_t _zz_end_read = 0;
    int32_t _zz_final_partial_char = 0;

    bool _zz_at_eof = false;
};
using StandardTokenizerImplPtr = std::unique_ptr<StandardTokenizerImpl>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index