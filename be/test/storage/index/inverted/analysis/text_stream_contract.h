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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/text_token_stream.h"

namespace doris::segment_v2::inverted_index {

struct CollectedToken {
    std::string term;
    int32_t position_increment = 0;

    bool operator==(const CollectedToken&) const = default;
};

std::ostream& operator<<(std::ostream& out, const CollectedToken& token);

// Reads `text` with `stream`, checking every next_batch() call against the TextTokenStream
// contract: true comes with at least one token and no negative increment, false with an empty
// batch, and false again on the call after. The batch holds a stale token before every call, so a
// stream that does not clear it fails.
std::vector<CollectedToken> collect_checked(TextTokenStream& stream, std::string_view text);

using TextStreamFactory = std::function<std::unique_ptr<TextTokenStream>()>;

// The whole contract, for streams from `make_stream`: one stream reused over `texts` gives each
// text the tokens a stream of its own gives it; an empty value gives no token, whatever its data
// pointer; and a reset() after the first batch of a value drops the rest of it, which `texts`
// exercises when one of them makes more than one batch.
void expect_text_stream_contract(const TextStreamFactory& make_stream,
                                 const std::vector<std::string>& texts);

} // namespace doris::segment_v2::inverted_index
