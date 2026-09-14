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
    build_source_byte_offset_map();
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

void ICUNormalizerCharFilter::build_source_byte_offset_map() {
    _source_byte_offsets.clear();
    _source_byte_offsets.reserve(_buf.size() + 1);
    _source_byte_offsets.push_back(0);

    UErrorCode status = U_ZERO_ERROR;
    auto iterator = _edits.getFineIterator();
    while (iterator.next(status)) {
        if (U_FAILURE(status) ||
            iterator.destinationIndex() != static_cast<int32_t>(_source_byte_offsets.size() - 1)) {
            _source_byte_offsets.clear();
            return;
        }

        const int32_t source_start = iterator.sourceIndex();
        const int32_t source_end = source_start + iterator.oldLength();
        if (iterator.hasChange()) {
            // ICU maps the start of a replacement to the start of its source span, and every
            // later destination boundary in that replacement to the end of the source span.
            for (int32_t i = 0; i < iterator.newLength(); ++i) {
                _source_byte_offsets.push_back(source_end);
            }
        } else {
            for (int32_t i = 1; i <= iterator.newLength(); ++i) {
                _source_byte_offsets.push_back(source_start + i);
            }
        }
    }

    if (U_FAILURE(status) || _source_byte_offsets.size() != _buf.size() + 1) {
        _source_byte_offsets.clear();
    }
}

int32_t ICUNormalizerCharFilter::correct_offset(int32_t current_offset) const {
    if (current_offset < 0 || static_cast<size_t>(current_offset) >= _source_byte_offsets.size()) {
        return DorisCharFilter::correct_offset(current_offset);
    }
    return DorisCharFilter::correct_offset(_source_byte_offsets[current_offset]);
}

} // namespace doris::segment_v2::inverted_index
