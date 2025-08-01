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

#include <memory>
#include <set>
#include <stack>

#include "AnalyzeContext.h"
#include "LexemePath.h"
#include "QuickSortSet.h"

namespace doris::segment_v2 {

class IKArbitrator {
public:
    IKArbitrator(vectorized::Arena& arena) : arena_(arena) {}
    // Ambiguity handling
    void process(AnalyzeContext& context, bool use_smart);

private:
    vectorized::Arena& arena_;
    // Ambiguity identification
    LexemePath* judge(Cell* lexeme_cell, size_t full_text_length);

    // Forward traversal, add lexeme, construct a non-ambiguous token combination
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option,
                     std::stack<Cell*, std::vector<Cell*>>& conflictStack);
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option);
    // Roll back the token chain until it can accept the specified token
    void backPath(const Lexeme& lexeme, LexemePath* path_option);
};

} // namespace doris::segment_v2
