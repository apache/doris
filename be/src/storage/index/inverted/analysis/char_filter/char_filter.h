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
#include <string_view>

namespace doris::segment_v2::inverted_index {

// A text-to-text function run over a whole value before it is tokenized. A stream owns its char
// filters, so a filter may keep state from one value to the next.
class TextCharFilter {
public:
    virtual ~TextCharFilter() = default;

    // Returns `text` itself when the filter leaves it as it is, and otherwise the filtered text,
    // written to `scratch`.
    virtual std::string_view apply(std::string_view text, std::string& scratch) = 0;
};

} // namespace doris::segment_v2::inverted_index
