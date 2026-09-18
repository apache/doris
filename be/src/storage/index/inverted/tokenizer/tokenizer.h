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

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "storage/index/inverted/char_filter/char_filter.h"
#include "storage/index/inverted/token_stream.h"

namespace doris::segment_v2::inverted_index {

class DorisTokenizer : public Tokenizer, public DorisTokenStream {
public:
    DorisTokenizer() = default;
    ~DorisTokenizer() override = default;

    void set_reader(const ReaderPtr& in) {
        if (in == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "reader must not be null");
        }
        _in_pending = in;
    }

    using Tokenizer::reset;
    // Only use the parameterless reset method
    void reset() override { _in = _in_pending; };

    std::span<const int32_t> get_source_byte_offsets() const override {
        return _source_byte_offsets_enabled ? std::span<const int32_t> {_source_byte_offsets}
                                            : std::span<const int32_t> {};
    }

    void set_source_byte_offsets_enabled(bool enabled) override {
        _source_byte_offsets_enabled = enabled;
    }

protected:
    int32_t correct_source_offset(int32_t offset) const {
        const auto* char_filter = dynamic_cast<const DorisCharFilter*>(_in.get());
        return char_filter == nullptr ? offset : char_filter->correct_offset(offset);
    }

    void set_source_byte_offsets(std::string_view term, int32_t source_start) {
        _source_byte_offsets.clear();
        if (!_source_byte_offsets_enabled) {
            return;
        }

        const auto* char_filter = dynamic_cast<const DorisCharFilter*>(_in.get());
        const int32_t corrected_start =
                char_filter == nullptr ? source_start : char_filter->correct_offset(source_start);
        _source_byte_offsets.push_back(0);
        const char* data = term.data();
        const auto length = static_cast<int32_t>(term.size());
        int32_t offset = 0;
        while (offset < length) {
            UChar32 code_point;
            U8_NEXT(data, offset, length, code_point);
            if (code_point < 0) {
                _source_byte_offsets.clear();
                return;
            }
            _source_byte_offsets.push_back(
                    char_filter == nullptr
                            ? offset
                            : char_filter->correct_offset(source_start + offset) - corrected_start);
        }
    }

    ReaderPtr _in;
    ReaderPtr _in_pending;
    std::vector<int32_t> _source_byte_offsets;
    bool _source_byte_offsets_enabled {false};
};
using TokenizerPtr = std::shared_ptr<DorisTokenizer>;

} // namespace doris::segment_v2::inverted_index
