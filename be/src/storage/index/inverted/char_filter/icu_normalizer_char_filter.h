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

#include <unicode/edits.h>
#include <unicode/normalizer2.h>

#include <string>
#include <vector>

#include "storage/index/inverted/char_filter/char_filter.h"

namespace doris::segment_v2::inverted_index {

class ICUNormalizerCharFilter : public DorisCharFilter {
public:
    ICUNormalizerCharFilter(ReaderPtr reader, std::shared_ptr<const icu::Normalizer2> normalizer);
    ~ICUNormalizerCharFilter() override = default;

    void initialize() override;

    void init(const void* _value, int32_t _length, bool copyData) override;
    int32_t read(const void** start, int32_t min, int32_t max) override;
    int32_t readCopy(void* start, int32_t off, int32_t len) override;

    size_t size() override { return _buf.size(); }
    int32_t correct_offset(int32_t current_offset) const override;

    size_t offset_correction_run_count() const { return _offset_correction_runs.size(); }

private:
    struct OffsetCorrectionRun {
        int32_t source_start;
        int32_t destination_start;
        int32_t source_length;
        int32_t destination_length;
        int32_t repeat_count;
    };

    void fill();
    void normalize_text(const std::string& input, std::string& output);
    void build_source_byte_offset_runs();

    std::shared_ptr<const icu::Normalizer2> _normalizer;
    icu::Edits _edits;
    std::string _buf;
    std::vector<OffsetCorrectionRun> _offset_correction_runs;
    lucene::util::SStringReader<char> _transformed_input;
};
using ICUNormalizerCharFilterPtr = std::shared_ptr<ICUNormalizerCharFilter>;

} // namespace doris::segment_v2::inverted_index
