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

#include "storage/index/inverted/token_stream.h"

namespace doris::segment_v2::inverted_index {

class DorisTokenFilter : public TokenFilter, public DorisTokenStream {
public:
    DorisTokenFilter(TokenStreamPtr in) : TokenFilter(nullptr), _in(std::move(in)) {}
    ~DorisTokenFilter() override = default;

    void reset() override { _in->reset(); }

    std::span<const int32_t> get_source_byte_offsets() const override {
        const auto* source = dynamic_cast<const DorisTokenStream*>(_in.get());
        return source == nullptr ? std::span<const int32_t> {} : source->get_source_byte_offsets();
    }

    std::span<const int32_t> get_source_byte_end_offsets() const override {
        const auto* source = dynamic_cast<const DorisTokenStream*>(_in.get());
        return source == nullptr ? std::span<const int32_t> {}
                                 : source->get_source_byte_end_offsets();
    }

    bool get_conservative_source_byte_span(int32_t& start, int32_t& end) const override {
        const auto* source = dynamic_cast<const DorisTokenStream*>(_in.get());
        return source != nullptr && source->get_conservative_source_byte_span(start, end);
    }

    void set_source_byte_offsets_enabled(bool enabled) override {
        auto* source = dynamic_cast<DorisTokenStream*>(_in.get());
        if (source != nullptr) {
            source->set_source_byte_offsets_enabled(enabled);
        }
    }

protected:
    TokenStreamPtr _in;
};
using TokenFilterPtr = std::shared_ptr<DorisTokenFilter>;

} // namespace doris::segment_v2::inverted_index
