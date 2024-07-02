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

#include "olap/rowset/segment_v2/binary_prefix_page.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <vector>

#include "common/status.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status BinaryPrefixPageBuilder::add(const uint8_t* vals, size_t* add_count) {
    DCHECK(!_finished);
    if (*add_count == 0) {
        return Status::OK();
    }

    const Slice* src = reinterpret_cast<const Slice*>(vals);
    if (_count == 0) {
        _first_entry.assign_copy(reinterpret_cast<const uint8_t*>(src->get_data()),
                                 src->get_size());
    }

    int i = 0;
    for (; i < *add_count; ++i, ++src) {
        if (is_page_full()) {
            break;
        }
        const char* entry = src->data;
        size_t entry_len = src->size;
        int old_size = _buffer.size();

        int share_len;
        if (_count % RESTART_POINT_INTERVAL == 0) {
            share_len = 0;
            _restart_points_offset.push_back(old_size);
        } else {
            int max_share_len = std::min(_last_entry.size(), entry_len);
            share_len = max_share_len;
            for (int i = 0; i < max_share_len; ++i) {
                if (entry[i] != _last_entry[i]) {
                    share_len = i;
                    break;
                }
            }
        }
        int non_share_len = entry_len - share_len;
        // This may need a large memory, should return error if could not allocated
        // successfully, to avoid BE OOM.
        RETURN_IF_CATCH_EXCEPTION({
            put_varint32(&_buffer, share_len);
            put_varint32(&_buffer, non_share_len);
            _buffer.append(entry + share_len, non_share_len);

            _last_entry.clear();
            _last_entry.append(entry, entry_len);
        });

        ++_count;
    }
    *add_count = i;
    return Status::OK();
}

OwnedSlice BinaryPrefixPageBuilder::finish() {
    DCHECK(!_finished);
    _finished = true;
    put_fixed32_le(&_buffer, (uint32_t)_count);
    uint8_t restart_point_internal = RESTART_POINT_INTERVAL;
    _buffer.append(&restart_point_internal, 1);
    auto restart_point_size = _restart_points_offset.size();
    for (uint32_t i = 0; i < restart_point_size; ++i) {
        put_fixed32_le(&_buffer, _restart_points_offset[i]);
    }
    put_fixed32_le(&_buffer, restart_point_size);
    return _buffer.build();
}

const uint8_t* BinaryPrefixPageDecoder::_decode_value_lengths(const uint8_t* ptr, uint32_t* shared,
                                                              uint32_t* non_shared) {
    if ((ptr = decode_varint32_ptr(ptr, _footer_start, shared)) == nullptr) {
        return nullptr;
    }
    if ((ptr = decode_varint32_ptr(ptr, _footer_start, non_shared)) == nullptr) {
        return nullptr;
    }
    if (_footer_start - ptr < *non_shared) {
        return nullptr;
    }
    return ptr;
}

Status BinaryPrefixPageDecoder::_read_next_value() {
    if (_cur_pos >= _num_values) {
        return Status::EndOfFile("no more value to read");
    }
    uint32_t shared_len;
    uint32_t non_shared_len;
    auto data_ptr = _decode_value_lengths(_next_ptr, &shared_len, &non_shared_len);
    if (data_ptr == nullptr) {
        DCHECK(false) << "[BinaryPrefixPageDecoder::_read_next_value] corruption!";
        return Status::Corruption("Failed to decode value at position {}", _cur_pos);
    }
    _current_value.resize(shared_len);
    _current_value.append(data_ptr, non_shared_len);
    _next_ptr = data_ptr + non_shared_len;
    return Status::OK();
}

Status BinaryPrefixPageDecoder::_seek_to_restart_point(size_t restart_point_index) {
    _cur_pos = restart_point_index * _restart_point_internal;
    _next_ptr = _get_restart_point(restart_point_index);
    return _read_next_value();
}

Status BinaryPrefixPageDecoder::init() {
    _cur_pos = 0;
    _next_ptr = reinterpret_cast<const uint8_t*>(_data.get_data());

    const uint8_t* end = _next_ptr + _data.get_size();
    _num_restarts = decode_fixed32_le(end - 4);
    _restarts_ptr = end - (_num_restarts + 1) * 4;
    _footer_start = _restarts_ptr - 4 - 1;
    _num_values = decode_fixed32_le(_footer_start);
    _restart_point_internal = decode_fixed8(_footer_start + 4);
    _parsed = true;
    return _read_next_value();
}

Status BinaryPrefixPageDecoder::seek_to_position_in_page(size_t pos) {
    DCHECK(_parsed);
    DCHECK_LE(pos, _num_values);

    // seek past the last value is valid
    if (pos == _num_values) {
        _cur_pos = _num_values;
        return Status::OK();
    }

    size_t restart_point_index = pos / _restart_point_internal;
    RETURN_IF_ERROR(_seek_to_restart_point(restart_point_index));
    while (_cur_pos < pos) {
        _cur_pos++;
        RETURN_IF_ERROR(_read_next_value());
    }
    return Status::OK();
}

Status BinaryPrefixPageDecoder::seek_at_or_after_value(const void* value, bool* exact_match) {
    DCHECK(_parsed);
    Slice target = *reinterpret_cast<const Slice*>(value);

    uint32_t left = 0;
    uint32_t right = _num_restarts;
    // find the first restart point >= target. after loop,
    // - left == index of first restart point >= target when found
    // - left == _num_restarts when not found (all restart points < target)
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        // read first entry at restart point `mid`
        RETURN_IF_ERROR(_seek_to_restart_point(mid));
        Slice mid_entry(_current_value);
        if (mid_entry.compare(target) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // then linear search from the last restart pointer < target.
    // when left == 0, all restart points >= target, so search from first one.
    // otherwise search from the last restart point < target, which is left - 1
    uint32_t search_index = left > 0 ? left - 1 : 0;
    RETURN_IF_ERROR(_seek_to_restart_point(search_index));
    while (true) {
        int cmp = Slice(_current_value).compare(target);
        if (cmp >= 0) {
            *exact_match = cmp == 0;
            return Status::OK();
        }
        _cur_pos++;
        auto st = _read_next_value();
        if (st.is<ErrorCode::END_OF_FILE>()) {
            return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("all value small than the value");
        }
        if (!st.ok()) {
            return st;
        }
    }
}

Status BinaryPrefixPageDecoder::next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(*n == 0 || _cur_pos >= _num_values)) {
        *n = 0;
        return Status::OK();
    }
    size_t max_fetch = std::min(*n, static_cast<size_t>(_num_values - _cur_pos));

    // read and copy values
    for (size_t i = 0; i < max_fetch; ++i) {
        dst->insert_data((char*)(_current_value.data()), _current_value.size());
        _cur_pos++;
        // reach the end of the page, should not read the next value
        if (_cur_pos < _num_values) {
            RETURN_IF_ERROR(_read_next_value());
        }
    }

    *n = max_fetch;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
