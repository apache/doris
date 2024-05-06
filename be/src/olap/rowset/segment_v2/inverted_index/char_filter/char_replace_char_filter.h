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

#include <CLucene.h>
#include <CLucene/analysis/CharFilter.h>

#include <bitset>

namespace doris {

class CharReplaceCharFilter : public lucene::analysis::CharFilter {
public:
    CharReplaceCharFilter(lucene::util::Reader* in, const std::string& pattern,
                          const std::string& replacement);
    ~CharReplaceCharFilter() override = default;

    void init(const void* _value, int32_t _length, bool copyData) override;
    int32_t read(const void** start, int32_t min, int32_t max) override;
    int32_t readCopy(void* start, int32_t off, int32_t len) override;

    size_t size() override { return _buf.size(); }

private:
    void fill();
    void process_pattern(std::string& buf);

    std::bitset<256> _patterns;
    std::string _replacement;

    std::string _buf;
    lucene::util::SStringReader<char> _transformed_input;
};

} // namespace doris