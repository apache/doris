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

#include <gen_cpp/segment_v2.pb.h>

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

#include "common/status.h"
#include "util/debug_util.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {

// In our system, we have more complicated situation.
// First, our keys can be nullptr.
// Second, when key columns are not complete we want to distinguish GT and GE. For example,
// there are two key columns a and b, we have only one condition a > 1. We can only encode
// a prefix key 1, which is less than 1|2. This will make our read more data than
// we actually need. So we want to add more marker.
// a > 1: will be encoded into 1|\xFF
// a >= 1: will be encoded into 1|\x00
// a = 1 and b > 1: will be encoded into 1|\x02|1
// a = 1 and b is null: will be encoded into 1|\x01

// Used to represent minimal value for that field
constexpr uint8_t KEY_MINIMAL_MARKER = 0x00;
// Used to represent a null field, which value is seemed as minimal than other values
constexpr uint8_t KEY_NULL_FIRST_MARKER = 0x01;
// Used to represent a normal field, which content is encoded after this marker
constexpr uint8_t KEY_NORMAL_MARKER = 0x02;
// Used to represent maximal value for that field
constexpr uint8_t KEY_MAXIMAL_MARKER = 0xFF;
// Used to represent a value greater than the normal marker by 1, using by MoW
constexpr uint8_t KEY_NORMAL_NEXT_MARKER = 0x03;

// Encode one row into binary according given num_keys.
// A cell will be encoded in the format of a marker and encoded content.
// When function encoding row, if any cell isn't found in row, this function will
// fill a marker and return. If padding_minimal is true, KEY_MINIMAL_MARKER will
// be added, if padding_minimal is false, KEY_MAXIMAL_MARKER will be added.
// If all num_keys are found in row, no marker will be added.
template <typename RowType, bool is_mow = false>
void encode_key_with_padding(std::string* buf, const RowType& row, size_t num_keys,
                             bool padding_minimal) {
    for (auto cid = 0; cid < num_keys; cid++) {
        auto field = row.schema()->column(cid);
        if (field == nullptr) {
            if (padding_minimal) {
                buf->push_back(KEY_MINIMAL_MARKER);
            } else {
                if (is_mow) {
                    buf->push_back(KEY_NORMAL_NEXT_MARKER);
                } else {
                    buf->push_back(KEY_MAXIMAL_MARKER);
                }
            }
            break;
        }

        auto cell = row.cell(cid);
        if (cell.is_null()) {
            buf->push_back(KEY_NULL_FIRST_MARKER);
            continue;
        }
        buf->push_back(KEY_NORMAL_MARKER);
        if (is_mow) {
            field->full_encode_ascending(cell.cell_ptr(), buf);
        } else {
            field->encode_ascending(cell.cell_ptr(), buf);
        }
    }
}

// Encode one row into binary according given num_keys.
// Client call this function must assure that row contains the first
// num_keys columns.
template <typename RowType, bool full_encode = false>
void encode_key(std::string* buf, const RowType& row, size_t num_keys) {
    for (auto cid = 0; cid < num_keys; cid++) {
        auto cell = row.cell(cid);
        if (cell.is_null()) {
            buf->push_back(KEY_NULL_FIRST_MARKER);
            continue;
        }
        buf->push_back(KEY_NORMAL_MARKER);
        if (full_encode) {
            row.schema()->column(cid)->full_encode_ascending(cell.cell_ptr(), buf);
        } else {
            row.schema()->column(cid)->encode_ascending(cell.cell_ptr(), buf);
        }
    }
}

} // namespace doris
