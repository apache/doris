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

#include <unicode/utf8.h>

#include <string_view>

#include "olap/rowset/segment_v2/inverted_index/token_stream.h"

using TokenStreamPtr = std::shared_ptr<TokenStream>;

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class DorisTokenizer : public Tokenizer, public DorisTokenStream {
public:
    DorisTokenizer() = default;
    ~DorisTokenizer() override = default;

    void set_reader(CL_NS(util)::Reader* in) {
        if (in == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "reader must not be null");
        }
        _in_pending = in;
    }

    using Tokenizer::reset;
    // Only use the parameterless reset method
    void reset() override { _in = _in_pending; };

protected:
    CL_NS(util)::Reader* _in = nullptr;
    CL_NS(util)::Reader* _in_pending = nullptr;
};
using TokenizerPtr = std::shared_ptr<DorisTokenizer>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index