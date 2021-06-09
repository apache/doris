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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_WRITER_H

#include <endian.h>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/out_stream.h"

namespace doris {

class OutStream;
class RowIndexEntryMessage;

// 以下注释来自Hive orc
/*
A writer that performs light weight compression over sequence of integers.

There are four types of lightweight integer compression:
    - SHORT_REPEAT
    - DIRECT
    - PATCHED_BASE
    - DELTA

The description and format for these types are as below:

|SHORT_REPEAT|: Used for short repeated integer sequences.
    # 1 byte header
        - 2 bits for encoding type
        - 3 bits for bytes required for repeating value
        - 3 bits for repeat count (MIN_REPEAT + run length)
    # Blob - repeat value (fixed bytes)

|DIRECT|: Used for random integer sequences whose number of bit requirement
doesn't vary a lot.
    # 2 bytes header
        - 1st byte
            > 2 bits for encoding type
            > 5 bits for fixed bit width of values in blob
            > 1 bit for storing MSB of run length
        - 2nd byte
            > 8 bits for lower run length bits
    # Blob - stores the direct values using fixed bit width. The length of the
      data blob is (fixed width * run length) bits long

|PATCHED_BASE|: Used for random integer sequences whose number of bit
requirement varies beyond a threshold.

    # 4 bytes header
        - 1st byte
            > 2 bits for encoding type
            > 5 bits for fixed bit width of values in blob
            > 1 bit for storing MSB of run length
        - 2nd byte
            > 8 bits for lower run length bits
        - 3rd byte
            > 3 bits for bytes required to encode base value
            > 5 bits for patch width
        - 4th byte
            > 3 bits for patch gap width
            > 5 bits for patch length
   # Base value - Stored using fixed number of bytes. If MSB is set, base
     value is negative else positive. Length of base value is (base width * 8)
     bits.
   # Data blob - Base reduced values as stored using fixed bit width. Length
     of data blob is (fixed width * run length) bits.
   # Patch blob - Patch blob is a list of gap and patch value. Each entry in
     the patch list is (patch width + patch gap width) bits long. Gap between the
     subsequent elements to be patched are stored in upper part of entry whereas
     patch values are stored in lower part of entry. Length of patch blob is
     ((patch width + patch gap width) * patch length) bits.

|DELTA|: Used for monotonically increasing or decreasing sequences, sequences
with fixed delta values or long repeated sequences.
    # 2 bytes header
        - 1st byte
            > 2 bits for encoding type
            > 5 bits for fixed bit width of values in blob
            > 1 bit for storing MSB of run length
        - 2nd byte
            > 8 bits for lower run length bits
    # Base value - encoded as varint
    # Delta base - encoded as varint
    # Delta blob - only positive values. monotonicity and orderness are decided
      based on the sign of the base value and delta base
 */
class RunLengthIntegerWriter {
public:
    // 如果RunLengthIntegerWriter用于输出带符号的数据(int8, int16, int32, int64),
    // 则设置is_singed为true, 此时会使用ZigZag
    // 编码以减少存储负数时使用的比特数
    explicit RunLengthIntegerWriter(OutStream* output, bool is_signed);
    ~RunLengthIntegerWriter() {}
    OLAPStatus write(int64_t value);
    OLAPStatus flush();
    void get_position(PositionEntryWriter* index_entry, bool print) const;

    void print_position_debug_info() {
        _output->print_position_debug_info();
        VLOG_TRACE << "_num_literals=" << _num_literals;
    }

private:
    friend class RunLengthIntegerReader;
    // 定义头部, 利用C的bitfield功能
    // Note: 百度的机器通常是LITTLE_ENDIAN的, ARM的机器未知,
    // 这里处理LITTLE_ENDIAN, 如果遇到了BIG_ENDIAN请自行修改
    //
    // 补充说明
    // 请注意：
    // C++位域是不可移植的,位域在一个字节中的排列方向与CPU/OS的大小端关系没有明确的文档说明
    //
    // A bit-field may have type int, unsigned int, or signed int.
    // Whether the high-order bit position of a “plain” int bit-field is treated as
    // a sign bit is implementation-defined.
    // A bit-field is interpreted as an integral type consisting of the specified number of bits.
    //
    // An implementation may allocate any addressable storage unit large enough to
    // hold a bit-field. If enough space remains, a bit-field that immediately follows
    // another bit-field in a structure shall be packed into adjacent bits of the same unit.
    // If insufficient space remains, whether a bit-field that does not fit is put into the
    // next unit or overlaps adjacent units is implementation-defined.
    // The order of allocation of bit-fields within a unit (high-order to low-order or
    // low-order to high-order) is implementation-defined.
    // The alignment of the addressable storage unit is unspecified.
    //
    // 即使在小端机器上，以下排列顺序也只是一种可能性较大的实现
    // 文档对位域的排列顺序说明，几乎都是implementation-defined或unspecified
    // 对于实际的排列顺序，未来可以写个小函数来判断
#if __BYTE_ORDER == __LITTLE_ENDIAN
    struct ShortRepeatHead {
        uint8_t run_length : 3, value_bytes : 3, type : 2;
        char blob[];
    };

    struct DirectHead {
        uint8_t length_msb : 1, bit_width : 5, type : 2;
        uint8_t length_low : 8;
        char blob[];

        inline uint16_t length() const { return (((uint16_t)length_msb) << 8) | length_low; }
        inline void set_length(uint16_t length) {
            length_msb = (length >> 8) & 0x01;
            length_low = length & 0xff;
        }
    };

    struct PatchedBaseHead {
        uint8_t length_msb : 1, bit_width : 5, type : 2;
        uint8_t length_low : 8;
        uint8_t patch_width : 5, base_bytes : 3;
        uint8_t patch_length : 5, gap_width : 3;

        char blob[];
        inline uint16_t length() const { return (((uint16_t)length_msb) << 8) | length_low; }
        inline void set_length(uint16_t length) {
            length_msb = (length >> 8) & 0x01;
            length_low = length & 0xff;
        }
    };

    struct DeltaHead {
        uint8_t length_msb : 1, bit_width : 5, type : 2;
        uint8_t length_low : 8;
        char blob[];

        inline uint16_t length() const { return (((uint16_t)length_msb) << 8) | length_low; }
        inline void set_length(uint16_t length) {
            length_msb = (length >> 8) & 0x01;
            length_low = length & 0xff;
        }
    };
#elif __BYTE_ORDER == __BIG_ENDIAN
#error "Target machine is big endian."
    // 大端的时候, 可以使用下面的代码, 请自行测试

    /*
    struct ShortRepeatHead {
        uint8_t type: 2,
                value_bytes: 3,
                repeat: 3;
        char blob[];
    };

    struct DirectHead {
        uint16_t type: 2,
                 bit_width: 5,
                 length: 9;
        char blob[];
    };

    struct PatchedBase {
        uint32_t type: 2,
                 bit_width: 5,
                 length: 9,
                 base_bytes: 3,
                 patch_width: 5,
                 gap_width: 3,
                 patch_length: 5;
        char blob[];
    };
    struct DeltaHead {
        uint16_t type: 2,
                 bit_width: 5,
                 length: 9;
        char blob[];
    };
    */
#else
#error "Endian order can not be determined."
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_RUN_LENGTH_INTEGER_WRITER_H
    enum EncodingType {
        SHORT_REPEAT = 0,
        DIRECT = 1,
        PATCHED_BASE = 2,
        DELTA = 3,
        NONE_ENCODING = 4
    };

private:
    void _clear();
    void _determined_encoding();
    void _init_literals(int64_t value);
    void _prepare_patched_blob();
    OLAPStatus _write_values();
    OLAPStatus _write_short_repeat_values();
    OLAPStatus _write_direct_values();
    OLAPStatus _write_patched_base_values();
    OLAPStatus _write_delta_values();

    static const uint16_t MAX_SCOPE = 512;
    static const uint16_t MIN_REPEAT = 3; // NOTE 不要修改这个值, 否则程序出错
    static const uint16_t MAX_SHORT_REPEAT_LENGTH = 10;
    // MAX_PATCH_LIST原本只需要0.05*MAX_SCOPE, 然而为了防止percentile_bits()里
    // 与这里的浮点计算产生的误差, 这里直接放大两倍, 请不要修改这个值
    static const uint16_t MAX_PATCH_LIST = uint16_t(MAX_SCOPE * 0.1);
    OutStream* _output;
    bool _is_signed;
    uint32_t _fixed_run_length;
    uint32_t _var_run_length;
    int64_t _prev_delta;
    int64_t _literals[MAX_SCOPE];
    EncodingType _encoding;
    uint16_t _num_literals;
    int64_t _zig_zag_literals[MAX_SCOPE];      // for direct encoding
    int64_t _base_reduced_literals[MAX_SCOPE]; // for for patched base encoding
    int64_t _adj_deltas[MAX_SCOPE - 1];        // for delta encoding
    int64_t _fixed_delta;
    uint32_t _zz_bits_90p;
    uint32_t _zz_bits_100p;
    uint32_t _br_bits_95p;
    uint32_t _br_bits_100p;
    uint32_t _bits_delta_max;
    uint32_t _patch_width;
    uint32_t _patch_gap_width;
    uint32_t _patch_length;
    int64_t _gap_vs_patch_list[MAX_PATCH_LIST];
    int64_t _min;
    bool _is_fixed_delta;

    DISALLOW_COPY_AND_ASSIGN(RunLengthIntegerWriter);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_WRITER_H