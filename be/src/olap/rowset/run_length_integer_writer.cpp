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

#include "olap/rowset/run_length_integer_writer.h"

#include <cmath>

#include "olap/out_stream.h"
#include "olap/serialize.h"

namespace doris {

RunLengthIntegerWriter::RunLengthIntegerWriter(OutStream* output, bool is_singed)
        : _output(output), _is_signed(is_singed), _fixed_run_length(0), _var_run_length(0) {
    _clear();
}

// NOTE _fixed_run_length 和 _var_run_length没有修改
void RunLengthIntegerWriter::_clear() {
    _num_literals = 0;
    _encoding = NONE_ENCODING;
    _prev_delta = 0;
    _fixed_delta = 0;
    _zz_bits_90p = 0;
    _zz_bits_100p = 0;
    _br_bits_95p = 0;
    _br_bits_100p = 0;
    _bits_delta_max = 0;
    _patch_gap_width = 0;
    _patch_length = 0;
    _patch_width = 0;
    _min = 0;
    _is_fixed_delta = false;
}

void RunLengthIntegerWriter::_init_literals(int64_t value) {
    _literals[0] = value;
    _num_literals = 1;
    _fixed_run_length = 1;
    _var_run_length = 1;
}

void RunLengthIntegerWriter::_determined_encoding() {
    // for identifying monotonic sequences
    bool is_increasing = false;
    uint32_t increasing_count = 1;
    bool is_decreasing = false;
    uint32_t decreasing_count = 1;

    // for identifying type of delta encoding
    _min = _literals[0];
    int64_t max = _literals[0];
    _is_fixed_delta = true;
    int64_t current_delta = 0;
    int64_t delta_max = 0;

    // populate all variables to identify the encoding type
    if (_num_literals >= 1) {
        if (_num_literals > 1) {
            current_delta = _literals[1] - _literals[0];
        }

        for (uint32_t i = 0; i < _num_literals; i++) {
            if (i > 0 && _literals[i] >= max) {
                max = _literals[i];
                ++increasing_count;
            }

            if (i > 0 && _literals[i] <= _min) {
                _min = _literals[i];
                //decreasing_count--;
                ++decreasing_count;
            }

            //if (i > 0 and _is_fixed_delta) {
            if (i > 0 && _is_fixed_delta) {
                if (_literals[i] - _literals[i - 1] != current_delta) {
                    _is_fixed_delta = false;
                }

                _fixed_delta = current_delta;
            }

            // populate zigzag encoded literals
            if (_is_signed) {
                _zig_zag_literals[i] = ser::zig_zag_encode(_literals[i]);
            } else {
                _zig_zag_literals[i] = _literals[i];
            }

            // max delta value is required for computing the fixed bits
            // required for delta blob in delta encoding
            if (i > 0) {
                if (i == 1) {
                    // first value preserve the sign
                    _adj_deltas[0] = _literals[1] - _literals[0];
                } else {
                    _adj_deltas[i - 1] = std::abs(_literals[i] - _literals[i - 1]);

                    if (_adj_deltas[i - 1] > delta_max) {
                        delta_max = _adj_deltas[i - 1];
                    }
                }
            }
        }

        // stores the number of bits required for packing delta blob in delta
        // encoding
        _bits_delta_max = ser::find_closet_num_bits(delta_max);

        // if decreasing count equals total number of literals then the sequence
        // is monotonically decreasing
        if (increasing_count == 1 && decreasing_count == _num_literals) {
            is_decreasing = true;
        }

        // if increasing count equals total number of literals then the sequence
        // is monotonically increasing
        if (decreasing_count == 1 && increasing_count == _num_literals) {
            is_increasing = true;
        }
    }

    // use DIRECT for delta overflows
    uint16_t hists[65];
    ser::compute_hists(_zig_zag_literals, _num_literals, hists);

    _zz_bits_90p = ser::percentile_bits_with_hist(hists, _num_literals, 0.9);
    _zz_bits_100p = ser::percentile_bits_with_hist(hists, _num_literals, 1.0);
    if (!ser::is_safe_subtract(max, _min)) {
        _encoding = DIRECT;
        return;
    }

    // if the sequence is both increasing and decreasing then it is not monotonic
    if (is_decreasing && is_increasing) {
        is_decreasing = false;
        is_increasing = false;
    }

    // fixed delta condition
    if (!is_increasing && !is_decreasing && _is_fixed_delta) {
        _encoding = DELTA;
        return;
    }

    // monotonic condition
    //if (is_increasing && is_decreasing) {
    if (is_increasing || is_decreasing) {
        _encoding = DELTA;
        return;
    }

    // percentile values are computed for the zigzag encoded values. if the
    // number of bit requirement between 90th and 100th percentile varies
    // beyond a threshold then we need to patch the values. if the variation
    // is not significant then we can use direct or delta encoding
    uint32_t diff_bits = _zz_bits_100p - _zz_bits_90p;

    // if the difference between 90th percentile and 100th percentile fixed
    // bits is > 1 then we need patch the values
    if (!is_increasing && !is_decreasing && diff_bits > 1 && !_is_fixed_delta) {
        // patching is done only on base reduced values.
        // remove base from literals
        for (uint32_t i = 0; i < _num_literals; i++) {
            _base_reduced_literals[i] = _literals[i] - _min;
        }

        ser::compute_hists(_base_reduced_literals, _num_literals, hists);
        // 95th percentile width is used to determine max allowed value
        // after which patching will be done
        _br_bits_95p = ser::percentile_bits_with_hist(hists, _num_literals, 0.95);

        // 100th percentile is used to compute the max patch width
        _br_bits_100p = ser::percentile_bits_with_hist(hists, _num_literals, 1.0);

        // after base reducing the values, if the difference in bits between
        // 95th percentile and 100th percentile value is zero then there
        // is no point in patching the values, in which case we will
        // fallback to DIRECT encoding.
        // The decision to use patched base was based on zigzag values, but the
        // actual patching is done on base reduced literals.
        if ((_br_bits_100p - _br_bits_95p) != 0) {
            _encoding = PATCHED_BASE;
            _prepare_patched_blob();
            return;
        } else {
            _encoding = DIRECT;
            return;
        }
    }

    // if difference in bits between 95th percentile and 100th percentile is
    // 0, then patch length will become 0. Hence we will fallback to direct
    if (!is_increasing && !is_decreasing && diff_bits <= 1 && !_is_fixed_delta) {
        _encoding = DIRECT;
        return;
    }

    // never happen
    LOG(FATAL) << "ops: fail to determine encoding type.";
}

void RunLengthIntegerWriter::_prepare_patched_blob() {
    // mask will be max value beyond which patch will be generated
    int64_t mask = (1L << _br_bits_95p) - 1;

    int32_t gap_list[MAX_PATCH_LIST];
    int64_t patch_list[MAX_PATCH_LIST];

    // #bit for patch
    _patch_width = ser::get_closet_fixed_bits(_br_bits_100p - _br_bits_95p);

    // if patch bit requirement is 64 then it will not possible to pack
    // gap and patch together in a long. To make sure gap and patch can be
    // packed together adjust the patch width
    if (_patch_width == 64) {
        _patch_width = 56;
        _br_bits_95p = 8;
        mask = (1L << _br_bits_95p) - 1;
    }

    uint32_t gap_idx = 0;
    uint32_t patch_idx = 0;
    uint32_t prev = 0;
    uint32_t gap = 0;
    uint32_t max_gap = 0;

    for (uint32_t i = 0; i < _num_literals; i++) {
        // if value is above mask then create the patch and record the gap
        if (((uint64_t)_base_reduced_literals[i]) > (uint64_t)mask) {
            gap = i - prev;

            if (gap > max_gap) {
                max_gap = gap;
            }

            // gaps are relative, so store the previous patched value index
            prev = i;
            gap_list[gap_idx++] = gap;

            // extract the most significant bits that are over mask bits
            int64_t patch = ((uint64_t)_base_reduced_literals[i]) >> _br_bits_95p;
            patch_list[patch_idx++] = patch;

            // strip off the MSB to enable safe bit packing
            _base_reduced_literals[i] &= mask;
        }
    }

    _patch_length = gap_idx;

    // if the element to be patched is the first and only element then
    // max gap will be 0, but to store the gap as 0 we need at least 1 bit
    if (max_gap == 0 && _patch_length != 0) {
        _patch_gap_width = 1;
    } else {
        _patch_gap_width = ser::find_closet_num_bits(max_gap);
    }

    // special case: if the patch gap width is greater than 256, then
    // we need 9 bits to encode the gap width. But we only have 3 bits in
    // header to record the gap width. To deal with this case, we will save
    // two entries in patch list in the following way
    // 256 gap width => 0 for patch value
    // actual gap - 256 => actual patch value
    // We will do the same for gap width = 511. If the element to be patched is
    // the last element in the scope then gap width will be 511. In this case we
    // will have 3 entries in the patch list in the following way
    // 255 gap width => 0 for patch value
    // 255 gap width => 0 for patch value
    // 1 gap width => actual patch value
    if (_patch_gap_width > 8) {
        _patch_gap_width = 8;

        // for gap = 511, we need two additional entries in patch list
        if (max_gap == 511) {
            _patch_length += 2;
        } else {
            _patch_length += 1;
        }
    }

    // create gap vs patch list
    gap_idx = 0;
    patch_idx = 0;

    for (uint32_t i = 0; i < _patch_length; i++) {
        int64_t g = gap_list[gap_idx++];
        int64_t p = patch_list[patch_idx++];

        while (g > 255) {
            _gap_vs_patch_list[i++] = (255L << _patch_width);
            g -= 255;
        }

        // store patch value in LSBs and gap in MSBs
        _gap_vs_patch_list[i] = (g << _patch_width) | p;
    }
}

Status RunLengthIntegerWriter::_write_short_repeat_values() {
    Status res = Status::OK();
    int64_t repeat_value;

    repeat_value = _is_signed ? ser::zig_zag_encode(_literals[0]) : _literals[0];

    uint32_t num_bits_repeat_value = ser::find_closet_num_bits(repeat_value);
    uint32_t num_bytes_repeat_value = num_bits_repeat_value % 8 == 0
                                              ? num_bits_repeat_value >> 3
                                              : (num_bits_repeat_value >> 3) + 1;

    ShortRepeatHead head;
    head.type = SHORT_REPEAT;
    head.value_bytes = num_bytes_repeat_value - 1;
    head.run_length = _fixed_run_length - MIN_REPEAT;

    res = _output->write((char*)&head, sizeof(head));
    if (!res.ok()) {
        OLAP_LOG_WARNING("fail to write SHORT_REPEAT head.");
        return res;
    }

    // write the repeating value in big endian byte order
    for (int32_t i = num_bytes_repeat_value - 1; i >= 0; i--) {
        char byte = (char)(((uint64_t)repeat_value >> (i * 8)) & 0xff);

        if (!(res = _output->write(byte))) {
            OLAP_LOG_WARNING("fail to write SHORT_REPEAT data.");
            return res;
        }
    }

    _fixed_run_length = 0;
    return Status::OK();
}

Status RunLengthIntegerWriter::_write_direct_values() {
    Status res = Status::OK();
    DirectHead head;

    head.type = DIRECT;
    head.bit_width = ser::encode_bit_width(_zz_bits_100p);
    head.set_length(_var_run_length - 1);

    res = _output->write((char*)&head, sizeof(head));
    if (!res.ok()) {
        OLAP_LOG_WARNING("fail to write DIRECT head.");
        return res;
    }

    res = ser::write_ints(_output, _zig_zag_literals, _num_literals, _zz_bits_100p);
    if (!res.ok()) {
        OLAP_LOG_WARNING("fail to write DIRECT data.");
        return res;
    }

    _var_run_length = 0;
    return Status::OK();
}

Status RunLengthIntegerWriter::_write_patched_base_values() {
    Status res = Status::OK();
    uint32_t bit_width = 0;
    PatchedBaseHead head;

    head.type = PATCHED_BASE;
    head.bit_width = ser::encode_bit_width(_br_bits_95p);
    head.set_length(_var_run_length - 1);

    // if the min value is negative toggle the sign
    bool is_negative = _min < 0;

    if (is_negative) {
        _min = -_min;
    }

    // find the number of bytes required for base.
    // The additional bit is used to store the sign of the base value
    uint32_t base_width = ser::find_closet_num_bits(_min) + 1;
    if (base_width > 64) {
        base_width = 64;
    }
    uint32_t base_bytes = base_width % 8 == 0 ? base_width / 8 : (base_width / 8 + 1);

    if (is_negative) {
        _min |= (1L << (base_bytes * 8 - 1));
    }

    head.base_bytes = base_bytes - 1;
    head.patch_width = ser::encode_bit_width(_patch_width);
    head.gap_width = _patch_gap_width - 1;
    head.patch_length = _patch_length;

    // write header
    res = _output->write((char*)&head, sizeof(head));

    if (!res.ok()) {
        LOG(WARNING) << "fail to write data.res = " << res;
        return res;
    }

    // write the base value using fixed bytes in big endian order
    for (int32_t i = base_bytes - 1; i >= 0; i--) {
        char byte = ((uint64_t)_min >> (i * 8)) & 0xff;
        res = _output->write(byte);

        if (!res.ok()) {
            LOG(WARNING) << "fail to write data.res = " << res;
            return res;
        }
    }

    // write reduced literals
    bit_width = ser::get_closet_fixed_bits(_br_bits_95p);
    res = ser::write_ints(_output, _base_reduced_literals, _num_literals, bit_width);

    if (!res.ok()) {
        LOG(WARNING) << "fail to write data.res = " << res;
        return res;
    }

    // write patch list
    bit_width = ser::get_closet_fixed_bits(_patch_gap_width + _patch_width);
    res = ser::write_ints(_output, _gap_vs_patch_list, _patch_length, bit_width);

    if (!res.ok()) {
        LOG(WARNING) << "fail to write data.res = " << res;
        return res;
    }

    _var_run_length = 0;
    return Status::OK();
}

Status RunLengthIntegerWriter::_write_delta_values() {
    Status res = Status::OK();
    uint32_t len = 0;
    uint32_t bit_width = _bits_delta_max;
    uint32_t encoded_bit_width = 0;
    DeltaHead head;

    if (_is_fixed_delta) {
        // if fixed run length is greater than threshold then it will be fixed
        // delta sequence with delta value 0 else fixed delta sequence with
        // non-zero delta value
        if (_fixed_run_length > MIN_REPEAT) {
            // ex. sequence: 2 2 2 2 2
            len = _fixed_run_length - 1;
            _fixed_run_length = 0;
        } else {
            // ex. sequence: 4 6 8 10 12 14 16
            len = _var_run_length - 1;
            _var_run_length = 0;
        }
    } else {
        // fixed width 0 is used for long repeating values.
        // sequences that require only 1 bit to encode will have an additional bit
        if (bit_width == 1) {
            bit_width = 2;
        }

        encoded_bit_width = ser::encode_bit_width(bit_width);
        len = _var_run_length - 1;
        _var_run_length = 0;
    }

    head.type = DELTA;
    head.bit_width = encoded_bit_width;
    head.set_length(len);

    // write header
    res = _output->write((char*)&head, sizeof(head));

    if (!res.ok()) {
        LOG(WARNING) << "fail to write data.res = " << res;
        return res;
    }

    // store the first value from zigzag literal array
    if (_is_signed) {
        res = ser::write_var_signed(_output, _literals[0]);
    } else {
        res = ser::write_var_unsigned(_output, _literals[0]);
    }

    if (!res.ok()) {
        LOG(WARNING) << "fail to write data.res = " << res;
        return res;
    }

    if (_is_fixed_delta) {
        // if delta is fixed then we don't need to store delta blob
        res = ser::write_var_signed(_output, _fixed_delta);

        if (!res.ok()) {
            LOG(WARNING) << "fail to write data.res = " << res;
            return res;
        }
    } else {
        // store the first value as delta value using zigzag encoding
        res = ser::write_var_signed(_output, _adj_deltas[0]);

        if (!res.ok()) {
            LOG(WARNING) << "fail to write data.res = " << res;
            return res;
        }

        // adjacent delta values are bit packed
        res = ser::write_ints(_output, &_adj_deltas[1], _num_literals - 2, bit_width);

        if (!res.ok()) {
            LOG(WARNING) << "fail to write data.res = " << res;
            return res;
        }
    }

    return Status::OK();
}

Status RunLengthIntegerWriter::_write_values() {
    Status res = Status::OK();

    if (_num_literals > 0) {
        switch (_encoding) {
        case SHORT_REPEAT:
            if (!(res = _write_short_repeat_values())) {
                LOG(WARNING) << "fail to write short repeat value.res = " << res;
                return res;
            }

            break;

        case DIRECT:
            if (!(res = _write_direct_values())) {
                LOG(WARNING) << "fail to write direct value.res = " << res;
                return res;
            }

            break;

        case PATCHED_BASE:
            if (!(res = _write_patched_base_values())) {
                LOG(WARNING) << "fail to write patched base value.res = " << res;
                return res;
            }

            break;

        case DELTA:
            if (!(res = _write_delta_values())) {
                LOG(WARNING) << "fail to write delta value.res = " << res;
                return res;
            }

            break;

        default:
            LOG(WARNING) << "Unknown encoding encoding=" << _encoding;
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        _clear();
    }

    return Status::OK();
}

Status RunLengthIntegerWriter::write(int64_t value) {
    Status res = Status::OK();

    if (_num_literals == 0) {
        _init_literals(value);
    } else if (_num_literals == 1) {
        _prev_delta = value - _literals[0];
        _literals[1] = value;
        _num_literals = 2;

        if (value == _literals[0]) {
            _fixed_run_length = 2;
            _var_run_length = 0;
        } else {
            _fixed_run_length = 0;
            _var_run_length = 2;
        }
    } else { // _num_literals >= 2
        long current_delta = value - _literals[_num_literals - 1];

        if (_prev_delta == 0 && current_delta == 0) {
            // fixed delta run
            _literals[_num_literals++] = value;

            // if variable run is non-zero then we are seeing repeating
            // values at the end of variable run in which case keep
            // updating variable and fixed runs
            if (_var_run_length > 0) {
                _fixed_run_length = 2;
            }

            _fixed_run_length++;

            // if fixed run met the minimum condition and if variable
            // run is non-zero then flush the variable run and shift the
            // tail fixed runs to start of the buffer
            if (_fixed_run_length >= MIN_REPEAT && _var_run_length > 0) {
                _num_literals -= MIN_REPEAT;
                _var_run_length -= MIN_REPEAT - 1;
                uint32_t tail_pos = _num_literals;
                _determined_encoding();

                if (!(res = _write_values())) {
                    OLAP_LOG_WARNING("fail to write values.");
                    return res;
                }

                // shift tail fixed runs to beginning of the buffer
                for (uint32_t i = 0; i < MIN_REPEAT; i++) {
                    _literals[_num_literals++] = _literals[tail_pos + i];
                }
            }

            if (_fixed_run_length == MAX_SCOPE) {
                _determined_encoding();

                if (!(res = _write_values())) {
                    OLAP_LOG_WARNING("fail to write values.");
                    return res;
                }
            }
        } else {
            // variable delta run

            // if fixed run length is non-zero and if it satisfies the
            // short repeat conditions then write the values as short repeats
            // else use delta encoding
            if (_fixed_run_length >= MIN_REPEAT) {
                if (_fixed_run_length <= MAX_SHORT_REPEAT_LENGTH) {
                    _encoding = SHORT_REPEAT;
                } else {
                    _encoding = DELTA;
                    _is_fixed_delta = true;
                }

                if (!(res = _write_values())) {
                    OLAP_LOG_WARNING("fail to write values.");
                    return res;
                }
            } else if (_fixed_run_length > 0) {
                if (value != _literals[_num_literals - 1]) {
                    _var_run_length = _fixed_run_length;
                    _fixed_run_length = 0;
                }
            }

            // after writing values re-initialize
            if (_num_literals == 0) {
                _init_literals(value);
            } else {
                // keep updating variable run length
                _prev_delta = value - _literals[_num_literals - 1];
                _literals[_num_literals++] = value;
                _var_run_length++;

                if (_var_run_length == MAX_SCOPE) {
                    _determined_encoding();

                    if (!(res = _write_values())) {
                        OLAP_LOG_WARNING("fail to write values.");
                        return res;
                    }
                }
            }
        }
    }

    return res;
}

Status RunLengthIntegerWriter::flush() {
    Status res = Status::OK();

    if (_num_literals != 0) {
        if (_var_run_length != 0) {
            _determined_encoding();
        } else if (_fixed_run_length != 0) {
            if (_fixed_run_length < MIN_REPEAT) {
                _var_run_length = _fixed_run_length;
                _fixed_run_length = 0;
                _determined_encoding();
            } else if (_fixed_run_length <= MAX_SHORT_REPEAT_LENGTH) {
                _encoding = SHORT_REPEAT;
            } else {
                _encoding = DELTA;
                _is_fixed_delta = true;
            }
        }

        if (!(res = _write_values())) {
            OLAP_LOG_WARNING("fail to write values.");
            return res;
        }
    }

    // 这里的output是不是需要flush？
    // 如果flush了。那么流中会吧这一块单独搞成一个buffer段。比如写10行，也会单独成段
    // 如果不flush，会接着写，但我认为是没问题的
    return _output->flush();
}

void RunLengthIntegerWriter::get_position(PositionEntryWriter* index_entry, bool print) const {
    _output->get_position(index_entry);
    index_entry->add_position(_num_literals);

    if (print) {
        _output->print_position_debug_info();
        VLOG_TRACE << "literals=" << _num_literals;
    }
}

} // namespace doris
