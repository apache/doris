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

#include "olap/rowset/run_length_integer_reader.h"

#include "olap/in_stream.h"
#include "olap/rowset/column_reader.h"
#include "olap/serialize.h"

namespace doris {

RunLengthIntegerReader::RunLengthIntegerReader(ReadOnlyFileStream* input, bool is_singed)
        : _input(input), _signed(is_singed), _num_literals(0), _used(0) {}

Status RunLengthIntegerReader::_read_values() {
    Status res = Status::OK();

    // read the first 2 bits and determine the encoding type
    uint8_t first_byte = 0;

    res = _input->read((char*)&first_byte);
    if (!res.ok()) {
        LOG(WARNING) << "fail to read first byte.res = " << res;
        return res;
    } else {
        int enc = (first_byte >> 6) & 0x03;

        if (RunLengthIntegerWriter::SHORT_REPEAT == enc) {
            res = _read_short_repeat_values(first_byte);
        } else if (RunLengthIntegerWriter::DIRECT == enc) {
            res = _read_direct_values(first_byte);
        } else if (RunLengthIntegerWriter::PATCHED_BASE == enc) {
            res = _read_patched_base_values(first_byte);
        } else {
            res = _read_delta_values(first_byte);
        }
    }

    return res;
}

Status RunLengthIntegerReader::_read_delta_values(uint8_t first_byte) {
    Status res = Status::OK();

    // extract the number of fixed bits
    uint32_t fb = (first_byte >> 1) & 0x1f;

    if (fb != 0) {
        fb = ser::decode_bit_width(fb);
    }

    // extract the blob run length
    int32_t len = (first_byte & 0x01) << 8;
    uint8_t byte = 0;

    res = _input->read((char*)&byte);
    if (!res.ok()) {
        return res;
    }

    len |= byte;

    // read the first value stored as vint
    int64_t first_val = 0;

    if (_signed) {
        res = ser::read_var_signed(_input, &first_val);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read var signed.res = " << res;
            return res;
        }
    } else {
        res = ser::read_var_unsigned(_input, &first_val);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read var unsigned.res = " << res;
            return res;
        }
    }

    // store first value to result buffer
    int64_t prev_val = first_val;
    _literals[_num_literals++] = first_val;

    // if fixed bits is 0 then all values have fixed delta
    if (fb == 0) {
        // read the fixed delta value stored as vint (deltas can be negative even
        // if all number are positive)
        int64_t fd = 0;

        res = ser::read_var_signed(_input, &fd);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read var signed.res = " << res;
            return res;
        }

        // add fixed deltas to adjacent values
        for (int i = 0; i < len; i++) {
            //_literals[_num_literals++] = _literals[_num_literals - 2] + fd;
            _literals[_num_literals] = _literals[_num_literals - 1] + fd;
            _num_literals++;
        }
    } else {
        int64_t delta_base = 0;

        res = ser::read_var_signed(_input, &delta_base);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read var signed.res = " << res;
            return res;
        }

        // add delta base and first value
        _literals[_num_literals++] = first_val + delta_base;
        prev_val = _literals[_num_literals - 1];
        len -= 1;

        // write the unpacked values, add it to previous value and store final
        // value to result buffer. if the delta base value is negative then it
        // is a decreasing sequence else an increasing sequence
        res = ser::read_ints(_input, &_literals[_num_literals], len, fb);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read ints.res = " << res;
            return res;
        }

        while (len > 0) {
            if (delta_base < 0) {
                _literals[_num_literals] = prev_val - _literals[_num_literals];
            } else {
                _literals[_num_literals] = prev_val + _literals[_num_literals];
            }

            prev_val = _literals[_num_literals];
            --len;
            ++_num_literals;
        }
    }

    return res;
}

Status RunLengthIntegerReader::_read_patched_base_values(uint8_t first_byte) {
    Status res = Status::OK();

    // extract the number of fixed bits
    int32_t fbo = (first_byte >> 1) & 0x1f;
    int32_t fb = ser::decode_bit_width(fbo);

    // extract the run length of data blob
    int32_t len = (first_byte & 0x01) << 8;
    uint8_t byte = 0;

    res = _input->read((char*)&byte);
    if (!res.ok()) {
        return res;
    }

    len |= byte;
    // runs are always one off
    len += 1;

    // extract the number of bytes occupied by base
    char third_byte = '\0';

    res = _input->read(&third_byte);
    if (!res.ok()) {
        LOG(WARNING) << "fail to read byte from in_stream.res = " << res;
        return res;
    }

    int32_t bw = ((uint8_t)third_byte >> 5) & 0x07;
    // base width is one off
    bw += 1;

    // extract patch width
    uint32_t pwo = third_byte & 0x1f;
    uint32_t pw = ser::decode_bit_width(pwo);

    // read fourth byte and extract patch gap width
    char four_byte = '\0';

    res = _input->read(&four_byte);
    if (!res.ok()) {
        LOG(WARNING) << "fail to read byte from in_straem.res = " << res;
        return res;
    }

    int32_t pgw = ((uint8_t)four_byte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    int32_t pl = four_byte & 0x1f;

    // read the next base width number of bytes to extract base value
    int64_t base = 0;

    res = ser::bytes_to_long_be(_input, bw, &base);
    if (!res.ok()) {
        LOG(WARNING) << "fail to bytes to long be.res = " << res;
        return res;
    }

    int64_t mask = (1L << ((bw * 8) - 1));

    // if MSB of base value is 1 then base is negative value else positive
    // TODO(lijiao): Why is zig_zag not used here?
    if ((base & mask) != 0) {
        base = base & ~mask;
        base = -base;
    }

    // unpack the data blob
    int64_t unpacked[len];

    res = ser::read_ints(_input, unpacked, len, fb);
    if (!res.ok()) {
        return res;
    }

    // unpack the patch blob
    int64_t unpacked_patch[pl];
    uint32_t bit_width = ser::get_closet_fixed_bits(pw + pgw);

    res = ser::read_ints(_input, unpacked_patch, pl, bit_width);
    if (!res.ok()) {
        return res;
    }

    // apply the patch directly when decoding the packed data
    int32_t patch_idx = 0;
    int64_t curr_gap = 0;
    int64_t curr_patch = 0;
    curr_gap = (uint64_t)unpacked_patch[patch_idx] >> pw;
    curr_patch = unpacked_patch[patch_idx] & ((1L << pw) - 1);
    int64_t actual_gap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curr_gap == 255 && curr_patch == 0) {
        actual_gap += 255;
        ++patch_idx;
        curr_gap = (uint64_t)unpacked_patch[patch_idx] >> pw;
        curr_patch = unpacked_patch[patch_idx] & ((1L << pw) - 1);
    }

    // add the left over gap
    actual_gap += curr_gap;

    // unpack data blob, patch it (if required), add base to get final result
    for (int32_t i = 0; i < len; i++) {
        if (i == actual_gap) {
            // extract the patch value
            int64_t patched_val = unpacked[i] | (curr_patch << fb);

            // add base to patched value
            _literals[_num_literals++] = base + patched_val;

            // increment the patch to point to next entry in patch list
            ++patch_idx;

            if (patch_idx < pl) {
                // read the next gap and patch
                curr_gap = (uint64_t)unpacked_patch[patch_idx] >> pw;
                curr_patch = unpacked_patch[patch_idx] & ((1L << pw) - 1);
                actual_gap = 0;

                // special case: gap is >255 then patch will be 0. if gap is
                // <=255 then patch cannot be 0
                while (curr_gap == 255 && curr_patch == 0) {
                    actual_gap += 255;
                    ++patch_idx;
                    curr_gap = (uint64_t)unpacked_patch[patch_idx] >> pw;
                    curr_patch = unpacked_patch[patch_idx] & ((1L << pw) - 1);
                }

                // add the left over gap
                actual_gap += curr_gap;

                // next gap is relative to the current gap
                actual_gap += i;
            }
        } else {
            // no patching required. add base to unpacked value to get final value
            _literals[_num_literals++] = base + unpacked[i];
        }
    }

    return res;
}

Status RunLengthIntegerReader::_read_direct_values(uint8_t first_byte) {
    Status res = Status::OK();

    // extract the number of fixed bits
    uint32_t fbo = (first_byte >> 1) & 0x1f;
    uint32_t fb = ser::decode_bit_width(fbo);

    // extract the run length
    int32_t len = (first_byte & 0x01) << 8;
    uint8_t byte = 0;

    res = _input->read((char*)&byte);
    if (!res.ok()) {
        return res;
    }

    len |= byte;
    // runs are one off
    len += 1;

    // write the unpacked values and zigzag decode to result buffer
    res = ser::read_ints(_input, _literals, len, fb);
    if (!res.ok()) {
        return res;
    }

    if (_signed) {
        for (int32_t i = 0; i < len; ++i) {
            _literals[_num_literals] = ser::zig_zag_decode(_literals[_num_literals]);
            ++_num_literals;
        }
    } else {
        _num_literals += len;
    }

    return res;
}

Status RunLengthIntegerReader::_read_short_repeat_values(uint8_t first_byte) {
    Status res = Status::OK();

    // read the number of bytes occupied by the value
    int32_t size = (first_byte >> 3) & 0x07;
    // #bytes are one off
    size += 1;

    // read the run length
    int32_t len = first_byte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    len += RunLengthIntegerWriter::MIN_REPEAT;

    // read the repeated value which is store using fixed bytes
    int64_t val = 0;

    res = ser::bytes_to_long_be(_input, size, &val);
    if (!res.ok()) {
        return res;
    }

    if (_signed) {
        val = ser::zig_zag_decode(val);
    }

    // repeat the value for length times
    for (int32_t i = 0; i < len; i++) {
        _literals[_num_literals++] = val;
    }

    return res;
}

Status RunLengthIntegerReader::seek(PositionProvider* position) {
    Status res = Status::OK();

    if (!(res = _input->seek(position))) {
        return res;
    }

    int32_t consumed = static_cast<int32_t>(position->get_next());

    if (consumed != 0) {
        // a loop is required for cases where we break the run into two parts
        while (consumed > 0) {
            _num_literals = 0;

            res = _read_values();
            if (!res.ok()) {
                return res;
            }

            _used = consumed;
            consumed -= _num_literals;
        }
    } else {
        _used = 0;
        _num_literals = 0;
    }

    return res;
}

Status RunLengthIntegerReader::skip(uint64_t num_values) {
    Status res = Status::OK();

    while (num_values > 0) {
        if (_used == _num_literals) {
            _num_literals = 0;
            _used = 0;

            res = _read_values();
            if (!res.ok()) {
                LOG(WARNING) << "fail to read values.res = " << res;
                return res;
            }
        }

        int64_t consume = std::min(num_values, static_cast<uint64_t>(_num_literals - _used));
        _used += consume;
        num_values -= consume;
    }

    return res;
}

} // namespace doris
