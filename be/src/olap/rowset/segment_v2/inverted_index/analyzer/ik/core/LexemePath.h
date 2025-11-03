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

#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>

#include "CLucene/_ApiHeader.h"
#include "QuickSortSet.h"

namespace doris::segment_v2 {

class LexemePath : public QuickSortSet {
public:
    LexemePath(vectorized::Arena& arena);
    LexemePath(LexemePath& other, vectorized::Arena& arena);
    LexemePath(LexemePath&& other, vectorized::Arena& arena) noexcept;
    bool addCrossLexeme(Lexeme& lexeme);
    bool addNotCrossLexeme(Lexeme& lexeme);
    std::optional<Lexeme> removeTail();
    bool checkCross(const Lexeme& lexeme) const;

    size_t getPathBegin() const { return path_begin_; }
    size_t getPathEnd() const { return path_end_; }
    size_t getPayloadLength() const { return payload_length_; }
    size_t getPathLength() const { return path_end_ - path_begin_; }
    size_t size() const { return getSize(); }

    size_t getXWeight() const;
    size_t getPWeight() const;

    bool operator<(const LexemePath& other) const;
    bool operator==(const LexemePath& other) const;

private:
    size_t path_begin_;     // Starting byte position
    size_t path_end_;       // Ending byte position
    size_t payload_length_; // Effective byte length of the lexeme chain
};

} // namespace doris::segment_v2
