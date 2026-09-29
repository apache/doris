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
    _source_length = static_cast<int32_t>(input.size());
    _offset_cursor = _edits.getFineIterator();
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

int32_t ICUNormalizerCharFilter::correct_offset(int32_t current_offset) const {
    if (current_offset < 0) {
        return DorisCharFilter::correct_offset(current_offset);
    }
    const auto destination_length = static_cast<int32_t>(_buf.size());
    if (current_offset >= destination_length) {
        return DorisCharFilter::correct_offset(_source_length +
                                               (current_offset - destination_length));
    }

    // Offsets at the start of an edit map to its source start, offsets inside an edit map to
    // its source end, and unchanged text keeps its relative position.
    UErrorCode status = U_ZERO_ERROR;
    const int32_t source_offset =
            _offset_cursor.sourceIndexFromDestinationIndex(current_offset, status);
    if (U_FAILURE(status)) {
        return DorisCharFilter::correct_offset(current_offset);
    }
    return DorisCharFilter::correct_offset(source_offset);
}

int32_t ICUNormalizerCharFilter::correct_start_offset(int32_t current_offset) const {
    const auto destination_length = static_cast<int32_t>(_buf.size());
    if (current_offset < 0 || current_offset >= destination_length) {
        return DorisCharFilter::correct_start_offset(correct_offset(current_offset));
    }

    // Inside a changed edit the term starts with output of that whole edit, so it maps to the
    // edit's source start; elsewhere this matches correct_offset().
    UErrorCode status = U_ZERO_ERROR;
    if (!_offset_cursor.findDestinationIndex(current_offset, status) || U_FAILURE(status)) {
        return correct_offset(current_offset);
    }
    int32_t source_offset = _offset_cursor.sourceIndex();
    if (!_offset_cursor.hasChange()) {
        source_offset += current_offset - _offset_cursor.destinationIndex();
    }
    return DorisCharFilter::correct_start_offset(source_offset);
}

} // namespace doris::segment_v2::inverted_index
