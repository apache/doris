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

#include <unicode/utext.h>

#include <memory>
#include <string_view>
#include <unordered_set>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

/**
 * All custom tokenizers and token_filters must use the following functions 
 * to set token information. Using these unified set methods helps avoid 
 * unnecessary data copying.
 * 
 * Note: Must not mix with other set methods
 */
class DorisTokenStream {
public:
    DorisTokenStream() = default;
    virtual ~DorisTokenStream() = default;

    void set(Token* t, const std::string_view& term, int32_t pos = 1) {
        t->setTextNoCopy(term.data(), cast_set<int32_t>(term.size()));
        t->setPositionIncrement(pos);
    }

    void set_text(Token* t, const std::string_view& term) {
        t->setTextNoCopy(term.data(), cast_set<int32_t>(term.size()));
    }

    int32_t get_position_increment(Token* t) { return t->getPositionIncrement(); }
    void set_position_increment(Token* t, int32_t pos) { t->setPositionIncrement(pos); }
};

}; // namespace doris::segment_v2::inverted_index
#include "common/compile_check_end.h"