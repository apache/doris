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

#include <cstdint>
#include <string>
#include <vector>

namespace doris::segment_v2::inverted_index::golden {

// A named list of input lines. The name is part of every key in the golden files, so a family is
// never renamed and its lines never change once frozen: a new risk gets a new family.
struct CorpusFamily {
    std::string name;
    std::vector<std::string> lines;
};

// xorshift64*. std::uniform_int_distribution is not used because its output may change with the
// standard library, and the corpus must not.
class GoldenRandom {
public:
    explicit GoldenRandom(uint64_t seed);

    uint64_t next();
    // A value in [0, bound); bound must be positive.
    uint32_t below(uint32_t bound);

private:
    uint64_t _state;
};

// The dictionaries the dictionary families sample their words from.
struct CorpusSources {
    std::string jieba_dict;         // "word frequency tag" per line
    std::string ik_main_dict;       // a word per line
    std::string ik_quantifier_dict; // a word per line
};

// Appends the UTF-8 form of a code point. A surrogate gets the three bytes its value would have,
// which is not valid UTF-8, so the code point families reach the invalid input paths too.
void append_utf8(std::string* out, uint32_t code_point);

// The families every run checks.
std::vector<CorpusFamily> default_families(const CorpusSources& sources);

// Every code point from U+0000 to U+10FFFF between Latin letters and between Han characters, for
// ANALYSIS_GOLDEN_FULL=1.
CorpusFamily full_code_point_family();

} // namespace doris::segment_v2::inverted_index::golden
