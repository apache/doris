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

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_DECIMAL_VALUE_H
#define DORIS_BE_SRC_QUERY_RUNTIME_DECIMAL_VALUE_H

#include <cctype>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "gutil/strings/numbers.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"
#include "util/mysql_global.h"

namespace doris {

// The number of digits per "big digits"
static const int32_t DIG_PER_DEC1 = 9;
// Maximum length of buffer, whose item is our "big digits" (uint32),
static const int32_t DECIMAL_BUFF_LENGTH = 9;
// The maximum number of digits that my_decimal can possibly contain
static const int32_t DECIMAL_MAX_POSSIBLE_PRECISION = DECIMAL_BUFF_LENGTH * 9;

// Maximum guaranteed precision of number in decimal digits (number of our
// digits * number of decimal digits in one our big digit - number of decimal
// digits in one our big digit decreased by 1 (because we always put decimal
// point on the border of our big digits))
static const int32_t DECIMAL_MAX_PRECISION = DECIMAL_MAX_POSSIBLE_PRECISION - 8 * 2;
static const int32_t DECIMAL_MAX_SCALE = 30;
// NOT_FIXED_DEC is defined in mysql_com.h
#ifndef NOT_FIXED_DEC
static const int32_t NOT_FIXED_DEC = 31;
#endif
// maximum length of string representation (number of maximum decimal
// digits + 1 position for sign + 1 position for decimal point, no terminator)
static const int32_t DECIMAL_MAX_STR_LENGTH = (DECIMAL_MAX_POSSIBLE_PRECISION + 2);

static const int32_t DIG_MASK = 100000000;  // 10^8
static const int32_t DIG_BASE = 1000000000; // 10^9
static const int32_t DIG_MAX = DIG_BASE - 1;

static const int32_t powers10[DIG_PER_DEC1 + 1] = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
static const int32_t frac_max[DIG_PER_DEC1 - 1] = {900000000, 990000000, 999000000, 999900000,
                                                   999990000, 999999000, 999999900, 999999990};

// TODO(lingbin): add to mysql result if we support "show warning" in our mysql protocol?
enum DecimalError {
    E_DEC_OK = 0,
    E_DEC_TRUNCATED = 1,
    E_DEC_OVERFLOW = 2,
    E_DEC_DIV_ZERO = 4,
    E_DEC_BAD_NUM = 8,
    E_DEC_OOM = 16,

    E_DEC_ERROR = 31,
    E_DEC_FATAL_ERROR = 30
};

enum DecimalRoundMode { HALF_UP = 1, HALF_EVEN = 2, CEILING = 3, FLOOR = 4, TRUNCATE = 5 };

// Type T should be an integer: int8_t, int16_t...
template <typename T>
inline T round_up(T length);

// Internally decimal numbers are stored base 10^9 (see DIG_BASE)
// So one variable of type big_digit_type is limited:
//      0 < decimal_digit <= DIG_MAX < DIG_BASE
class DecimalValue {
public:
    friend DecimalValue operator+(const DecimalValue& v1, const DecimalValue& v2);
    friend DecimalValue operator-(const DecimalValue& v1, const DecimalValue& v2);
    friend DecimalValue operator*(const DecimalValue& v1, const DecimalValue& v2);
    friend DecimalValue operator/(const DecimalValue& v1, const DecimalValue& v2);
    friend int32_t do_add(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to);
    friend int32_t do_sub(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to);
    friend int do_mul(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to);
    friend int do_div_mod(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to,
                          DecimalValue* mod);
    friend std::istream& operator>>(std::istream& ism, DecimalValue& decimal_value);

    friend DecimalValue operator-(const DecimalValue& v);

    DecimalValue() : _buffer_length(DECIMAL_BUFF_LENGTH) { set_to_zero(); }

    DecimalValue(const std::string& decimal_str) : _buffer_length(DECIMAL_BUFF_LENGTH) {
        parse_from_str(decimal_str.c_str(), decimal_str.size());
    }

    // Construct from olap engine
    // Note: the base is 10^9 for parameter frac_value, which means the max length of fraction part
    // is 9, and the parameter frac_value need to be divided by 10^9.
    DecimalValue(int64_t int_value, int64_t frac_value) : _buffer_length(DECIMAL_BUFF_LENGTH) {
        set_to_zero();
        if (int_value < 0 || frac_value < 0) {
            _sign = true;
        } else {
            _sign = false;
        }

        int32_t big_digit_length = copy_int_to_decimal_int(std::abs(int_value), _buffer);
        _int_length = big_digit_length * DIG_PER_DEC1;
        _frac_length = copy_int_to_decimal_frac(std::abs(frac_value), _buffer + big_digit_length);
    }

    DecimalValue(int64_t int_value) : _buffer_length(DECIMAL_BUFF_LENGTH) {
        set_to_zero();
        _sign = int_value < 0 ? true : false;

        int32_t big_digit_length = copy_int_to_decimal_int(std::abs(int_value), _buffer);
        _int_length = big_digit_length * DIG_PER_DEC1;
        _frac_length = 0;
    }

    DecimalValue& assign_from_float(const float float_value) {
        // buffer is short, sign and '\0' is the 2.
        char buffer[MAX_FLOAT_STR_LENGTH + 2];
        buffer[0] = '\0';
        int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
        DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
        parse_from_str(buffer, length);
        return *this;
    }

    DecimalValue& assign_from_double(const double double_value) {
        char buffer[MAX_DOUBLE_STR_LENGTH + 2];
        buffer[0] = '\0';
        int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
        DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
        parse_from_str(buffer, length);
        return *this;
    }

    // These cast functions are needed in "functions.cc", which is generated by python script.
    // e.g. "ComputeFunctions::Cast_DecimalValue_double()"
    // Discard the scale part
    // ATTN: invoker must make sure no OVERFLOW
    operator int64_t() const {
        const int32_t* buff = _buffer;
        int64_t result = 0;

        int32_t int_length = _int_length;
        for (int32_t i = 0; int_length > 0; ++i) {
            result = (result * DIG_BASE) + *(buff + i);
            int_length -= DIG_PER_DEC1;
        }

        // negative
        if (_sign) {
            result = -result;
        }

        return result;
    }

    // These cast functions are needed in "functions.cc", which is generated by python script.
    // e.g. "ComputeFunctions::Cast_DecimalValue_double()"
    // Discard the scale part
    // ATTN: invoker must make sure no OVERFLOW
    operator __int128() const {
        const int32_t* buff = _buffer;
        __int128 result = 0;

        int32_t int_length = _int_length;
        for (int32_t i = 0; int_length > 0; ++i) {
            result = (result * DIG_BASE) + *(buff + i);
            int_length -= DIG_PER_DEC1;
        }

        // negative
        if (_sign) {
            result = -result;
        }

        return result;
    }

    operator bool() const { return !is_zero(); }

    operator int8_t() const { return static_cast<char>(operator int64_t()); }

    operator int16_t() const { return static_cast<int16_t>(operator int64_t()); }

    operator int32_t() const { return static_cast<int32_t>(operator int64_t()); }

    operator size_t() const { return static_cast<size_t>(operator int64_t()); }

    operator float() const { return (float)operator double(); }

    operator double() const {
        std::string str_buff = to_string();
        double result = std::strtod(str_buff.c_str(), nullptr);
        return result;
    }

    DecimalValue& operator+=(const DecimalValue& other);

    // To be Compatible with OLAP
    // ATTN: NO-OVERFLOW should be guaranteed.
    int64_t int_value() const { return operator int64_t(); }

    // To be Compatible with OLAP
    // NOTE: return a negative value if decimal is negative.
    // ATTN: the max length of fraction part in OLAP is 9, so the 'big digits' except the first one
    // will be truncated.
    int32_t frac_value() const {
        const int32_t intg = round_up(_int_length);
        const int32_t frac = round_up(_frac_length);
        const int32_t* frac_begin = _buffer + intg;
        int32_t frac_val = (frac != 0) ? *frac_begin : 0;
        frac_val = (_sign == true) ? -frac_val : frac_val;
        return frac_val;
    }

    bool equal(const DecimalValue& other) const { return (*this - other).is_zero(); }

    bool bigger(const DecimalValue& other) const { return (other - *this)._sign; }

    bool smaller(const DecimalValue& other) const { return (*this - other)._sign; }

    bool operator==(const DecimalValue& other) const { return equal(other); }

    bool operator!=(const DecimalValue& other) const { return !equal(other); }

    bool operator<=(const DecimalValue& other) const { return !bigger(other); }

    bool operator>=(const DecimalValue& other) const { return !smaller(other); }

    bool operator<(const DecimalValue& other) const { return smaller(other); }

    bool operator>(const DecimalValue& other) const { return bigger(other); }

    // change to maximum value for given precision and scale
    // precision/scale - see decimal_bin_size() below
    // to              - decimal where where the result will be stored
    //                  to->buf and to->len must be set.
    void to_max_decimal(int precision, int frac);
    void to_min_decimal(int precision, int frac) {
        to_max_decimal(precision, frac);
        _sign = -1;
    }

    // The maximum of fraction part is "scale".
    // If the length of fraction part is less than "scale", '0' will be filled.
    std::string to_string(int scale) const;
    // Output actual "scale", remove ending zeroes.
    std::string to_string() const;

    // Convert string to decimal
    // @param from - value to convert. Doesn't have to be \0 terminated!
    //               will stop at the fist non-digit char(nor '.' 'e' 'E'),
    //               or reaches the length
    // @param length - maximum length
    // @return error number.
    //
    // E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW/E_DEC_BAD_NUM/E_DEC_OOM
    // In case of E_DEC_FATAL_ERROR *to is set to decimal zero
    // (to make error handling easier)
    //
    // e.g. "1.2"  ".2"  "1.2e-3"  "1.2e3"
    int parse_from_str(const char* decimal_str, int32_t length);

    std::string get_debug_info() const {
        std::stringstream ss;
        ss << "_int_length: " << _int_length << "; "
           << "_frac_length: " << _frac_length << "; "
           << "_sign: " << _sign << "; "
           << "_buffer_length: " << _buffer_length << "; ";
        ss << "_buffer: [";
        for (int i = 0; i < DIG_PER_DEC1; ++i) {
            ss << _buffer[i] << ", ";
        }
        ss << "]; ";
        return ss.str();
    }

    static DecimalValue get_min_decimal() {
        DecimalValue value;
        value._sign = true;
        value._int_length = DECIMAL_MAX_POSSIBLE_PRECISION;
        value._frac_length = 0;
        for (int i = 0; i < DIG_PER_DEC1; ++i) {
            value._buffer[i] = DIG_BASE - 1;
        }
        return value;
    }

    static DecimalValue get_max_decimal() {
        DecimalValue value;
        value._sign = false;
        value._int_length = DECIMAL_MAX_POSSIBLE_PRECISION;
        value._frac_length = 0;
        for (int i = 0; i < DIG_PER_DEC1; ++i) {
            value._buffer[i] = DIG_BASE - 1;
        }
        return value;
    }

    static DecimalValue from_decimal_val(const doris_udf::DecimalVal& val) {
        DecimalValue result;
        result._int_length = val.int_len;
        result._frac_length = val.frac_len;
        result._sign = val.sign;

        result._buffer_length = DECIMAL_BUFF_LENGTH;
        memcpy(result._buffer, val.buffer, sizeof(int32_t) * DECIMAL_BUFF_LENGTH);
        return result;
    }

    void to_decimal_val(doris_udf::DecimalVal* value) const {
        value->int_len = _int_length;
        value->frac_len = _frac_length;
        value->sign = _sign;
        memcpy(value->buffer, _buffer, sizeof(int32_t) * DECIMAL_BUFF_LENGTH);
    }

    // set DecimalValue to zero
    void set_to_zero() {
        _buffer_length = DECIMAL_BUFF_LENGTH;
        memset(_buffer, 0, sizeof(int32_t) * DECIMAL_BUFF_LENGTH);
        _int_length = 1;
        _frac_length = 0;
        _sign = false;
    }

    void to_abs_value() { _sign = false; }

    uint32_t hash_uint(uint32_t value, uint32_t seed) const {
        return HashUtil::hash(&value, sizeof(value), seed);
    }

    uint32_t hash(uint32_t seed) const {
        uint32_t int_len = round_up(_int_length);
        uint32_t frac_len = round_up(_frac_length);
        int idx = 0;
        while (idx < int_len && _buffer[idx] == 0) {
            idx++;
        }
        while (idx < int_len) {
            // Hash
            seed = hash_uint(_buffer[idx++], seed);
        }
        idx = int_len + frac_len;
        while (idx > int_len && _buffer[idx - 1] == 0) {
            idx--;
        }
        while (idx > int_len) {
            // Hash
            seed = hash_uint(_buffer[--idx], seed);
        }
        // Hash sign
        return hash_uint(_sign, seed);
    }

    int32_t precision() const { return _int_length + _frac_length; }

    int32_t scale() const { return _frac_length; }

    int round(DecimalValue* to, int scale, DecimalRoundMode mode);

private:
    friend class MultiDistinctDecimalState;

    bool is_zero() const {
        const int32_t* buff = _buffer;
        const int32_t* end = buff + round_up(_int_length) + round_up(_frac_length);
        while (buff < end) {
            if (*buff++) {
                return false;
            }
        }
        return true;
    }

    // TODO(lingbin): complete this function
    int shift(int32_t shift) { return 0; }

    // Invoker make sure buff has enough space.
    // return the number of "big digits".
    int copy_int_to_decimal_int(int64_t int_value, int32_t* buff);

    // ATTN: the max length of fraction part is 9 for now, so we can directly assign parameter
    // frac_value to buff member.
    int copy_int_to_decimal_frac(int64_t frac_value, int32_t* buff);

    const int32_t* get_first_no_zero_index(int32_t* int_digit_num) const;

    // _int_length is the number of *decimal* digits (NOT number of big_digit_type's !)
    //      before the point
    // _frac_length is the number of decimal digits after the point
    // _buffer_length  is the length of buf (length of allocated space) in big_digit_type's,
    //      not in bytes
    // _sign false means positive, true means negative
    // _buffer  is an array of big_digit_type's
    // TODO(zc): use int64_t to aligned to 8
    int32_t _int_length : 8;
    int32_t _frac_length : 8;
    int32_t _buffer_length : 8;
    bool _sign;
    int32_t _buffer[DECIMAL_BUFF_LENGTH];
};

DecimalValue operator+(const DecimalValue& v1, const DecimalValue& v2);
DecimalValue operator-(const DecimalValue& v1, const DecimalValue& v2);
DecimalValue operator*(const DecimalValue& v1, const DecimalValue& v2);
DecimalValue operator/(const DecimalValue& v1, const DecimalValue& v2);
DecimalValue operator%(const DecimalValue& v1, const DecimalValue& v2);

DecimalValue operator-(const DecimalValue& v);

std::ostream& operator<<(std::ostream& os, DecimalValue const& decimal_value);
std::istream& operator>>(std::istream& ism, DecimalValue& decimal_value);

// TODO(lingbin) discard the fraction part?
int64_t operator&(const DecimalValue& v1, const DecimalValue& v2);
int64_t operator|(const DecimalValue& v1, const DecimalValue& v2);
int64_t operator^(const DecimalValue& v1, const DecimalValue& v2);
int64_t operator~(const DecimalValue& v1);

// help to get the number of decimal_digit_t's digits
//    e.g. for 1234567891.222 . intg=10, ROUND_UP(10) = 2.
// It means in decimal_digit_t type buff,
//     it takes '2' bytes to store integer part
template <typename T>
inline T round_up(T length) {
    return (T)((length + DIG_PER_DEC1 - 1) / DIG_PER_DEC1);
}

inline int DecimalValue::copy_int_to_decimal_int(int64_t int_value, int32_t* buff) {
    int64_t dividend = int_value;
    int32_t temp_buff[DECIMAL_BUFF_LENGTH];
    int32_t index = 0; // index in temp_buffer

    if (int_value == 0) {
        _int_length = 0;
        return 0;
    }

    int64_t quotient = 0;
    do {
        temp_buff[index++] = dividend % DIG_BASE;
        quotient = dividend / DIG_BASE;
        dividend = quotient;
    } while (quotient != 0);

    for (int32_t i = 0; i < index; ++i) {
        buff[i] = temp_buff[index - i - 1];
    }
    return index;
}

inline int32_t DecimalValue::copy_int_to_decimal_frac(int64_t frac_value, int32_t* buff) {
    if (frac_value == 0) {
        return 0;
    }
    int32_t abs_frac_value = std::abs(frac_value);
    if (std::abs(frac_value > DIG_BASE)) {
        *buff = DIG_MAX;
        return DIG_PER_DEC1;
    }

    *buff = abs_frac_value;
    // Count digit length: (DIG_PER_DEC1 - the number of ending zeroes)
    int32_t frac_len = DIG_PER_DEC1;
    int32_t quotient = 0;
    while ((quotient = frac_value % 10) == 0) {
        frac_value /= 10;
        --frac_len;
    }

    return frac_len;
}

inline const int32_t* DecimalValue::get_first_no_zero_index(int32_t* int_digit_num) const {
    int32_t temp_intg = _int_length;
    const int32_t* buff = _buffer;
    int32_t first_big_digit_num = (temp_intg - 1) % DIG_PER_DEC1 + 1;

    while (temp_intg > 0 && *buff == 0) {
        temp_intg -= first_big_digit_num;
        first_big_digit_num = DIG_PER_DEC1;
        ++buff;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    // When the value of a "big digit" is "000099999", its 'intg' may be 5/6/7/8/9,
    // we get accurate 'intg' here and the first no zero index of buff
    if (temp_intg > 0) {
        first_big_digit_num = (temp_intg - 1) % DIG_PER_DEC1 + 1;
        for (; *buff < powers10[first_big_digit_num - 1]; --first_big_digit_num) {
            --temp_intg;
        }
    } else {
        temp_intg = 0;
    }
#pragma GCC diagnostic pop
    *int_digit_num = temp_intg;
    return buff;
}

std::size_t hash_value(DecimalValue const& value);

} // end namespace doris

namespace std {
template <>
struct hash<doris::DecimalValue> {
    size_t operator()(const doris::DecimalValue& v) const { return doris::hash_value(v); }
};
} // namespace std

#endif // DORIS_BE_SRC_QUERY_RUNTIME_DECIMAL_VALUE_H
