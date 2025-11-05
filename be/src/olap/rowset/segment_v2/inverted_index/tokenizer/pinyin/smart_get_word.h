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
#include <vector>

#include "rune.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

class SmartForest;

class SmartGetWord {
public:
    using ParamType = std::vector<std::string>;

    static const std::string EMPTYSTRING;
    static const std::string NULL_RESULT;

public:
    int offe = 0;

public:
    SmartGetWord(SmartForest* forest, const std::string& content);

    SmartGetWord(SmartForest* forest, const std::vector<Rune>& runes);

    std::string getFrontWords();

    const ParamType& getParam() const;

    void reset(const std::string& content);

    void reset(const std::vector<Rune>& runes);

    int getByteOffset() const;

    int getMatchStartByte() const;

    int getMatchEndByte() const;

    static const std::string& getNullResult() { return NULL_RESULT; }

    static const std::string& getEmptyString() { return EMPTYSTRING; }

private:
    SmartForest* forest_;
    std::vector<Rune> runes_;
    SmartForest* branch_;

    uint8_t status_ = 0;
    size_t root_ = 0;
    size_t i_ = 0;
    bool is_back_ = false;
    size_t temp_offe_ = 0;
    ParamType param_;

    std::string frontWords();

    std::string allWords();

    std::string checkNumberOrEnglish(const std::string& temp);

    bool checkSame(UChar32 l, UChar32 c);

    bool isE(UChar32 c) const;

    bool isNum(UChar32 c) const;

    static std::vector<Rune> utf8_to_runes(const std::string& utf8_str);

    static std::string runes_to_utf8(const std::vector<Rune>& runes, size_t start, size_t end);
};

using PolyphoneGetWord = SmartGetWord;

} // namespace doris::segment_v2::inverted_index
