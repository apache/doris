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

#include <string>

namespace doris::segment_v2::inverted_index {

enum class CaseType {
    LOWERCASE, // '民' -> min2
    UPPERCASE, // '民' -> MIN2
    CAPITALIZE // '民' -> Min2
};

enum class ToneType {
    WITH_TONE_NUMBER, // '打' -> da3
    WITHOUT_TONE,     // '打' -> da
    WITH_TONE_MARK,   // '打' -> dǎ
    WITH_ABBR         // '打' -> d
};

enum class YuCharType {
    WITH_U_AND_COLON, // ü -> u:
    WITH_V,           // ü -> v
    WITH_U_UNICODE    // ü -> ü
};

class PinyinFormat {
public:
    static const PinyinFormat DEFAULT_PINYIN_FORMAT;
    static const PinyinFormat UNICODE_PINYIN_FORMAT;
    static const PinyinFormat TONELESS_PINYIN_FORMAT;
    static const PinyinFormat ABBR_PINYIN_FORMAT;

    PinyinFormat();
    PinyinFormat(YuCharType yu_char_type, ToneType tone_type);
    PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type);
    PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type,
                 const std::string& separator);
    PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type,
                 const std::string& separator, bool only_pinyin);

    YuCharType getYuCharType() const { return yu_char_type_; }
    ToneType getToneType() const { return tone_type_; }
    CaseType getCaseType() const { return case_type_; }
    const std::string& getSeparator() const { return separator_; }
    bool isOnlyPinyin() const { return only_pinyin_; }

    void setYuCharType(YuCharType yu_char_type) { yu_char_type_ = yu_char_type; }
    void setToneType(ToneType tone_type) { tone_type_ = tone_type; }
    void setCaseType(CaseType case_type) { case_type_ = case_type; }
    void setSeparator(const std::string& separator) { separator_ = separator; }
    void setOnlyPinyin(bool only_pinyin) { only_pinyin_ = only_pinyin; }

private:
    YuCharType yu_char_type_ = YuCharType::WITH_U_AND_COLON;
    ToneType tone_type_ = ToneType::WITH_TONE_NUMBER;
    CaseType case_type_ = CaseType::LOWERCASE;
    std::string separator_ = " ";
    bool only_pinyin_ = false;
};

} // namespace doris::segment_v2::inverted_index
