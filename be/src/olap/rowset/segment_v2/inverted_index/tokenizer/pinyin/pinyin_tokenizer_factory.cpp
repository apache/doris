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

#include "pinyin_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

void PinyinTokenizerFactory::initialize(const Settings& settings) {
    config_->keepFirstLetter = settings.get_bool("keep_first_letter", true);
    config_->keepSeparateFirstLetter = settings.get_bool("keep_separate_first_letter", false);
    config_->keepFullPinyin = settings.get_bool("keep_full_pinyin", true);
    config_->keepJoinedFullPinyin = settings.get_bool("keep_joined_full_pinyin", false);
    config_->keepNoneChinese = settings.get_bool("keep_none_chinese", true);
    config_->keepNoneChineseTogether = settings.get_bool("keep_none_chinese_together", true);
    config_->noneChinesePinyinTokenize = settings.get_bool("none_chinese_pinyin_tokenize", true);
    config_->keepOriginal = settings.get_bool("keep_original", false);
    config_->limitFirstLetterLength = settings.get_int("limit_first_letter_length", 16);
    config_->lowercase = settings.get_bool("lowercase", true);
    config_->trimWhitespace = settings.get_bool("trim_whitespace", true);
    config_->keepNoneChineseInFirstLetter =
            settings.get_bool("keep_none_chinese_in_first_letter", true);
    config_->keepNoneChineseInJoinedFullPinyin =
            settings.get_bool("keep_none_chinese_in_joined_full_pinyin", false);
    config_->removeDuplicateTerm = settings.get_bool("remove_duplicated_term", false);
    config_->fixedPinyinOffset = settings.get_bool("fixed_pinyin_offset", false);
    config_->ignorePinyinOffset = settings.get_bool("ignore_pinyin_offset", true);
    config_->keepSeparateChinese = settings.get_bool("keep_separate_chinese", false);
}
} // namespace doris::segment_v2::inverted_index
