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

#include "char_replace_char_filter.h"

#include <boost/algorithm/string/replace.hpp>

namespace doris {

CharReplaceCharFilter::CharReplaceCharFilter(lucene::util::Reader* in, const std::string& pattern,
                                             const std::string& replacement)
        : CharFilter(in), _replacement(replacement) {
    std::for_each(pattern.begin(), pattern.end(), [this](uint8_t c) { _patterns.set(c); });
}

void CharReplaceCharFilter::init(const void* _value, int32_t _length, bool copyData) {
    input_->init(_value, _length, copyData);
    fill();
}

int32_t CharReplaceCharFilter::read(const void** start, int32_t min, int32_t max) {
    return _transformed_input.read(start, min, max);
}

int32_t CharReplaceCharFilter::readCopy(void* start, int32_t off, int32_t len) {
    return _transformed_input.readCopy(start, off, len);
}

void CharReplaceCharFilter::fill() {
    _buf.resize(input_->size());
    input_->readCopy(_buf.data(), 0, _buf.size());
    process_pattern(_buf);
    _transformed_input.init(_buf.data(), _buf.size(), false);
}

void CharReplaceCharFilter::process_pattern(std::string& buf) {
    for (char& c : buf) {
        uint8_t uc = static_cast<uint8_t>(c);
        if (_patterns.test(uc)) {
            c = _replacement[0];
        }
    }
}

} // namespace doris