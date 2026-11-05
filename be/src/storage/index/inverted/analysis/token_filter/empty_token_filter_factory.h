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

#include "storage/index/inverted/analysis/token_filter/token_filter_factory.h"

namespace doris::segment_v2::inverted_index {

// The filter that changes nothing: the stream is its input itself.
class EmptyTokenFilterFactory : public TokenFilterFactory {
public:
    EmptyTokenFilterFactory() = default;
    ~EmptyTokenFilterFactory() override = default;

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenStream> create(std::unique_ptr<TextTokenStream> in) const override {
        return in;
    }
};

} // namespace doris::segment_v2::inverted_index
