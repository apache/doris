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

#include "pinyin_format.h"

namespace doris::segment_v2::inverted_index {

const PinyinFormat PinyinFormat::DEFAULT_PINYIN_FORMAT = PinyinFormat();

const PinyinFormat PinyinFormat::UNICODE_PINYIN_FORMAT =
        PinyinFormat(YuCharType::WITH_U_UNICODE, ToneType::WITH_TONE_MARK);

const PinyinFormat PinyinFormat::TONELESS_PINYIN_FORMAT =
        PinyinFormat(YuCharType::WITH_V, ToneType::WITHOUT_TONE);

const PinyinFormat PinyinFormat::ABBR_PINYIN_FORMAT = PinyinFormat(
        YuCharType::WITH_U_AND_COLON, ToneType::WITH_ABBR, CaseType::LOWERCASE, "", true);

PinyinFormat::PinyinFormat()
        : yu_char_type_(YuCharType::WITH_U_AND_COLON),
          tone_type_(ToneType::WITH_TONE_NUMBER),
          case_type_(CaseType::LOWERCASE),
          separator_(" "),
          only_pinyin_(false) {}

PinyinFormat::PinyinFormat(YuCharType yu_char_type, ToneType tone_type)
        : yu_char_type_(yu_char_type),
          tone_type_(tone_type),
          case_type_(CaseType::LOWERCASE),
          separator_(" "),
          only_pinyin_(false) {}

PinyinFormat::PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type)
        : yu_char_type_(yu_char_type),
          tone_type_(tone_type),
          case_type_(case_type),
          separator_(" "),
          only_pinyin_(false) {}

PinyinFormat::PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type,
                           const std::string& separator)
        : yu_char_type_(yu_char_type),
          tone_type_(tone_type),
          case_type_(case_type),
          separator_(separator),
          only_pinyin_(false) {}

PinyinFormat::PinyinFormat(YuCharType yu_char_type, ToneType tone_type, CaseType case_type,
                           const std::string& separator, bool only_pinyin)
        : yu_char_type_(yu_char_type),
          tone_type_(tone_type),
          case_type_(case_type),
          separator_(separator),
          only_pinyin_(only_pinyin) {}

} // namespace doris::segment_v2::inverted_index
