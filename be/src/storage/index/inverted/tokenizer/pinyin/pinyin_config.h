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

namespace doris::segment_v2 {

class PinyinConfig {
public:
    PinyinConfig() = default;
    ~PinyinConfig() = default;

    bool lowercase = true;
    bool trimWhitespace = true;
    bool keepNoneChinese = true;
    bool keepNoneChineseInFirstLetter = true;
    bool keepNoneChineseInJoinedFullPinyin = false;
    bool keepOriginal = false;
    bool keepFirstLetter = true;
    bool keepSeparateFirstLetter = false;
    bool keepNoneChineseTogether = true;
    bool noneChinesePinyinTokenize = true;
    int limitFirstLetterLength = 16;
    bool keepFullPinyin = true;
    bool keepJoinedFullPinyin = false;
    bool removeDuplicateTerm = false;
    bool fixedPinyinOffset = false;
    bool ignorePinyinOffset = true;
    bool keepSeparateChinese = false;
};

} // namespace doris::segment_v2