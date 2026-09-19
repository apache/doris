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

#include "storage/index/inverted/char_filter/icu_normalizer_char_filter.h"

#include <unicode/bytestream.h>
#include <unicode/normalizer2.h>
#include <unicode/stringpiece.h>

#include <algorithm>
#include <iterator>

#include "common/exception.h"
#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

ICUNormalizerCharFilter::ICUNormalizerCharFilter(ReaderPtr reader,
                                                 std::shared_ptr<const icu::Normalizer2> normalizer)
        : DorisCharFilter(std::move(reader)), _normalizer(std::move(normalizer)) {
    if (_normalizer == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "ICUNormalizerCharFilter: normalizer cannot be null");
    }
}

void ICUNormalizerCharFilter::initialize() {
    if (_transformed_input.size() != 0) {
        return;
    }
    fill();
}

void ICUNormalizerCharFilter::init(const void* _value, int32_t _length, bool copyData) {
    _reader->init(_value, _length, copyData);
    fill();
}

int32_t ICUNormalizerCharFilter::read(const void** start, int32_t min, int32_t max) {
    return _transformed_input.read(start, min, max);
}

int32_t ICUNormalizerCharFilter::readCopy(void* start, int32_t off, int32_t len) {
    return _transformed_input.readCopy(start, off, len);
}

void ICUNormalizerCharFilter::fill() {
    std::string input;
    input.resize(_reader->size());
    _reader->readCopy(input.data(), 0, static_cast<int32_t>(input.size()));
    normalize_text(input, _buf);
    build_source_byte_offset_runs();
    _transformed_input.init(_buf.data(), static_cast<int32_t>(_buf.size()), false);
}

void ICUNormalizerCharFilter::normalize_text(const std::string& input, std::string& output) {
    output.clear();
    _edits.reset();
    if (input.empty()) {
        return;
    }

    UErrorCode status = U_ZERO_ERROR;
    icu::StringByteSink<std::string> sink(&output);
    _normalizer->normalizeUTF8(0, icu::StringPiece(input), sink, &_edits, status);
    if (U_FAILURE(status)) {
        LOG(WARNING) << "ICU normalize failed: " << u_errorName(status) << ", using original text";
        output = input;
        _edits.reset();
        _edits.addUnchanged(static_cast<int32_t>(input.size()));
        return;
    }
}

void ICUNormalizerCharFilter::build_source_byte_offset_runs() {
    _offset_correction_runs.clear();
    UErrorCode status = U_ZERO_ERROR;
    auto iterator = _edits.getFineChangesIterator();
    while (iterator.next(status)) {
        if (U_FAILURE(status)) {
            _offset_correction_runs.clear();
            return;
        }

        const int32_t source_start = iterator.sourceIndex();
        const int32_t destination_start = iterator.destinationIndex();
        const int32_t source_length = iterator.oldLength();
        const int32_t destination_length = iterator.newLength();
        if (!_offset_correction_runs.empty()) {
            auto& previous = _offset_correction_runs.back();
            const int64_t previous_source_end =
                    static_cast<int64_t>(previous.source_start) +
                    static_cast<int64_t>(previous.source_length) * previous.repeat_count;
            const int64_t previous_destination_end =
                    static_cast<int64_t>(previous.destination_start) +
                    static_cast<int64_t>(previous.destination_length) * previous.repeat_count;
            if (previous.source_length == source_length &&
                previous.destination_length == destination_length &&
                previous_source_end == source_start &&
                previous_destination_end == destination_start) {
                ++previous.repeat_count;
                continue;
            }
        }
        _offset_correction_runs.push_back(
                {source_start, destination_start, source_length, destination_length, 1});
    }
    if (U_FAILURE(status)) {
        _offset_correction_runs.clear();
    }
}

int32_t ICUNormalizerCharFilter::correct_offset(int32_t current_offset) const {
    if (current_offset < 0 || _offset_correction_runs.empty()) {
        return DorisCharFilter::correct_offset(current_offset);
    }

    const auto next_run = std::ranges::upper_bound(_offset_correction_runs, current_offset, {},
                                                   &OffsetCorrectionRun::destination_start);
    if (next_run == _offset_correction_runs.begin()) {
        return DorisCharFilter::correct_offset(current_offset);
    }

    const auto& run = *std::prev(next_run);
    const int64_t source_end = static_cast<int64_t>(run.source_start) +
                               static_cast<int64_t>(run.source_length) * run.repeat_count;
    const int64_t destination_end = static_cast<int64_t>(run.destination_start) +
                                    static_cast<int64_t>(run.destination_length) * run.repeat_count;
    if (run.destination_length == 0 && current_offset == run.destination_start) {
        return DorisCharFilter::correct_offset(static_cast<int32_t>(source_end));
    }

    if (current_offset <= destination_end) {
        const int64_t relative_destination = current_offset - run.destination_start;
        const int64_t completed_edits = relative_destination / run.destination_length;
        const int64_t source_offset =
                run.source_start + completed_edits * run.source_length +
                (relative_destination % run.destination_length == 0 ? 0 : run.source_length);
        return DorisCharFilter::correct_offset(static_cast<int32_t>(source_offset));
    }
    return DorisCharFilter::correct_offset(
            static_cast<int32_t>(source_end + current_offset - destination_end));
}

} // namespace doris::segment_v2::inverted_index
