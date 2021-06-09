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

#include "runtime/decimal_value.h"

#include <algorithm>
#include <iostream>
#include <utility>

namespace doris {

// set the 1st param if the second param is smaller.
template <typename T>
inline void set_if_smaller(T* num1_ptr, const T num2) {
    if (*num1_ptr > num2) {
        *num1_ptr = num2;
    }
}

// set the 1st param if the second param is smaller.
template <typename T>
inline void set_if_bigger(T* num1_ptr, const T num2) {
    if (*num1_ptr < num2) {
        *num1_ptr = num2;
    }
}

// util function: check if there is error and fix it.
inline void fix_intg_frac_error(const int32_t len, int32_t* int_len, int32_t* frac_len,
                                int32_t* error) {
    if (*int_len + *frac_len > len) {
        if (*int_len > len) {
            *int_len = len;
            *frac_len = 0;
            *error = E_DEC_OVERFLOW;
        } else {
            *frac_len = len - *int_len;
            *error = E_DEC_TRUNCATED;
        }
    } else {
        *error = E_DEC_OK;
    }
}

// Note: the carry <= 1, so for the sum of three number(value1, value2, *carry),
//      the maximum value of carry is 1.
inline void add(const int32_t value1, const int32_t value2, int32_t* to, int32_t* carry) {
    int32_t sum = value1 + value2 + *carry;
    *carry = (sum >= DIG_BASE) ? 1 : 0;
    if (*carry) {
        sum -= DIG_BASE;
    }
    *to = sum;
}

// to = value1 - value2
inline void sub(const int32_t value1, const int32_t value2, int32_t* to, int32_t* carry) {
    int32_t a = value1 - value2 - *carry;
    *carry = (a < 0) ? 1 : 0;
    if (*carry) {
        a += DIG_BASE;
    }
    *to = a;
}

// Note: the input carry may > 1, after the summation process of three number (value1, value2, *carry),
//      the maximum value of carry may be 2, when sum() >= 2 * DIG_BASE.
inline void add2(const int32_t value1, const int32_t value2, int32_t* to, int32_t* carry) {
    // NOTE: When three int32_t integers (the maximum value of each number is 10 ^ 9 - 1) are added,
    // because the maximum value of int32_t is 2147483647, the result may overflow, so it is
    // necessary to convert int32_t to int64_t.
    int64_t sum = (int64_t)value1 + value2 + *carry;
    *carry = (sum >= DIG_BASE) ? 1 : 0;
    if (*carry) {
        sum -= DIG_BASE;
    }
    if (sum >= DIG_BASE) {
        sum -= DIG_BASE;
        ++(*carry);
    }
    // the value of sum must small than DIG_BASE here
    *to = (int32_t)sum;
}

// to = value1 - value2 ƒ
inline void sub2(const int32_t value1, const int32_t value2, int32_t* to, int32_t* carry) {
    int32_t a = value1 - value2 - *carry;
    *carry = (a < 0) ? 1 : 0;
    if (*carry) {
        a += DIG_BASE;
    }
    if (a < 0) {
        a += DIG_BASE;
        ++(*carry);
    }
    *to = a;
}

int32_t do_add(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to) {
    int32_t intg1 = round_up(value1._int_length);
    int32_t intg2 = round_up(value2._int_length);
    int32_t frac1 = round_up(value1._frac_length);
    int32_t frac2 = round_up(value2._frac_length);
    int32_t frac0 = std::max(frac1, frac2);
    int32_t intg0 = std::max(intg1, intg2);

    // Is there a need for extra word because of carry?
    int32_t first_big_digit_sum = intg1 > intg2   ? value1._buffer[0]
                                  : intg2 > intg1 ? value2._buffer[0]
                                                  : value1._buffer[0] + value2._buffer[0];
    if (first_big_digit_sum > DIG_MAX - 1) {
        // yes, there is
        ++intg0;
        to->_buffer[0] = 0; // for safety
    }

    to->_sign = value1._sign;

    int32_t error = E_DEC_OK;
    fix_intg_frac_error(to->_buffer_length, &intg0, &frac0, &error);
    if (error == E_DEC_OVERFLOW) {
        to->to_max_decimal(to->_buffer_length * DIG_PER_DEC1, 0);
        return error;
    }

    int32_t* buf0 = to->_buffer + intg0 + frac0;
    to->_int_length = intg0 * DIG_PER_DEC1;
    to->_frac_length = std::max(value1._frac_length, value2._frac_length);
    if (error) { // E_DEC_TRUNCATED
        int32_t to_frac_length = to->_frac_length;
        //ATTN: _int_length is bit-field struct member, can not take address directly.
        set_if_smaller(&to_frac_length, frac0 * DIG_PER_DEC1);
        to->_frac_length = to_frac_length;
        set_if_smaller(&frac1, frac0);
        set_if_smaller(&frac2, frac0);
        set_if_smaller(&intg1, intg0);
        set_if_smaller(&intg2, intg0);
    }

    // part 1 - max(frac) ... min (frac)
    const int32_t* buf1 = nullptr;
    const int32_t* buf2 = nullptr;
    const int32_t* stop = nullptr;
    const int32_t* stop2 = nullptr;
    if (frac1 > frac2) {
        buf1 = value1._buffer + intg1 + frac1;
        stop = value1._buffer + intg1 + frac2;
        buf2 = value2._buffer + intg2 + frac2;
        stop2 = value1._buffer + ((intg1 > intg2) ? (intg1 - intg2) : 0);
    } else {
        buf1 = value2._buffer + intg2 + frac2;
        stop = value2._buffer + intg2 + frac1;
        buf2 = value1._buffer + intg1 + frac1;
        stop2 = value2._buffer + ((intg2 > intg1) ? (intg2 - intg1) : 0);
    }
    while (buf1 > stop) {
        *--buf0 = *--buf1;
    }

    // part 2 - min(frac) ... min(intg)
    int32_t carry = 0;
    while (buf1 > stop2) {
        add(*--buf1, *--buf2, --buf0, &carry);
    }

    // part 3 - min(intg) ... max(intg)
    if (intg1 > intg2) {
        stop = value1._buffer;
        buf1 = stop + intg1 - intg2;
    } else {
        stop = value2._buffer;
        buf1 = stop + intg2 - intg1;
    }
    while (buf1 > stop) {
        add(*--buf1, 0, --buf0, &carry);
    }

    if (carry) {
        *--buf0 = 1;
    }

    return error;
}

// to=value1-value2.
// if to==0, return -1/0/+1 - the result of the comparison
int do_sub(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to) {
    int32_t intg1 = round_up(value1._int_length);
    int32_t intg2 = round_up(value2._int_length);
    int32_t frac1 = round_up(value1._frac_length);
    int32_t frac2 = round_up(value2._frac_length);
    int32_t frac0 = std::max(frac1, frac2);
    int32_t error = E_DEC_OK;
    int32_t carry = 0;

    // let carry:=1 if value2 > value1
    // TODO(lingbin): add another variable 'is_bigger' to replace 'carry' to make carry only
    //                has one meaning
    const int32_t* buf1 = value1._buffer;
    const int32_t* start1 = buf1;
    const int32_t* stop1 = buf1 + intg1;
    const int32_t* buf2 = value2._buffer;
    const int32_t* start2 = buf2;
    const int32_t* stop2 = buf2 + intg2;
    // ignore leading zeroes
    if (*buf1 == 0) {
        while (buf1 < stop1 && *buf1 == 0) {
            buf1++;
        }
        start1 = buf1;
        intg1 = (int32_t)(stop1 - buf1);
    }
    if (*buf2 == 0) {
        while (buf2 < stop2 && *buf2 == 0) {
            buf2++;
        }
        start2 = buf2;
        intg2 = (int32_t)(stop2 - buf2);
    }
    if (intg2 > intg1) {
        carry = 1;
    } else if (intg2 == intg1) {
        const int32_t* end1 = stop1 + (frac1 - 1);
        const int32_t* end2 = stop2 + (frac2 - 1);
        // ignore trailing zeroes
        while ((buf1 <= end1) && (*end1 == 0)) {
            end1--;
        }
        while ((buf2 <= end2) && (*end2 == 0)) {
            end2--;
        }

        frac1 = (int32_t)(end1 - stop1) + 1;
        frac2 = (int32_t)(end2 - stop2) + 1;
        while (buf1 <= end1 && buf2 <= end2 && *buf1 == *buf2) {
            buf1++;
            buf2++;
        }

        if (buf1 <= end1) {
            if (buf2 <= end2) { // not equal
                carry = (*buf2 > *buf1);
            } else { // value1 is longer, so value1 > value2
                carry = 0;
            }
        } else {
            if (buf2 <= end2) { // value2 is longer
                carry = 1;
            } else {                 // short-circuit everything: value1 == value2
                if (to == nullptr) { // for decimal_cmp()
                    return 0;
                }
                to->set_to_zero();
                return E_DEC_OK;
            }
        }
    }

    if (to == nullptr) { // decimal_cmp(), not equal
        return (carry == value1._sign) ? 1 : -1;
    }

    to->_sign = value1._sign;

    DecimalValue value_big = value1;
    DecimalValue value_small = value2;

    // ensure that always value1 > value2 (and intg1 >= intg2)
    if (carry) {
        std::swap(value_big, value_small);
        std::swap(start1, start2);
        std::swap(intg1, intg2);
        std::swap(frac1, frac2);
        to->_sign = 1 - to->_sign;
    }

    fix_intg_frac_error(to->_buffer_length, &intg1, &frac0, &error);
    int32_t* buf0 = to->_buffer + intg1 + frac0;

    to->_frac_length = std::max(value_big._frac_length, value_small._frac_length);
    to->_int_length = intg1 * DIG_PER_DEC1;
    if (error) { // must be E_DEC_TRUNCATE.
        int32_t temp_to_frac_length = to->_frac_length;
        set_if_smaller(&temp_to_frac_length, frac0 * DIG_PER_DEC1);
        to->_frac_length = temp_to_frac_length;

        set_if_smaller(&frac1, frac0);
        set_if_smaller(&frac2, frac0);
        set_if_smaller(&intg2, intg1);
    }
    carry = 0;

    // part 1 - max(frac) ... min (frac)
    if (frac1 > frac2) {
        buf1 = start1 + intg1 + frac1;
        stop1 = start1 + intg1 + frac2;
        buf2 = start2 + intg2 + frac2;
        while (frac0-- > frac1) { //occur when there are trailing zeroes
            *--buf0 = 0;
        }
        while (buf1 > stop1) {
            *--buf0 = *--buf1;
        }
    } else {
        buf1 = start1 + intg1 + frac1;
        buf2 = start2 + intg2 + frac2;
        stop2 = start2 + intg2 + frac1;
        while (frac0-- > frac2) {
            *--buf0 = 0;
        }
        while (buf2 > stop2) {
            sub(0, *--buf2, --buf0, &carry);
        }
    }

    // part 2 - min(frac) ... intg2
    while (buf2 > start2) {
        sub(*--buf1, *--buf2, --buf0, &carry);
    }

    // part 3 - intg2 ... intg1 */
    while (carry && buf1 > start1) {
        sub(*--buf1, 0, --buf0, &carry);
    }

    while (buf1 > start1) {
        *--buf0 = *--buf1;
    }

    while (buf0 > to->_buffer) { // TODO(lingbin): will not happen?
        *--buf0 = 0;
    }

    return error;
}

// multiply two decimals
// @return E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW;
//
// NOTE:
//  in this implementation, with sizeof(big_digit_type)=4 we have DIG_PER_DEC1=9,
//  and 63-digit number will take only 7 big_digit_type words (basically a 7-digit
//  "base 999999999" number).  Thus there's no need in fast multiplication
//  algorithms, 7-digit numbers can be multiplied with a naive O(n*n)
//  method.
//  XXX if this library is to be used with huge numbers of thousands of
//  digits, fast multiplication must be implemented.
int do_mul(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to) {
    int32_t intg1 = round_up(value1._int_length);
    int32_t intg2 = round_up(value2._int_length);
    int32_t frac1 = round_up(value1._frac_length);
    int32_t frac2 = round_up(value2._frac_length);
    int32_t intg0 = round_up(value1._int_length + value2._int_length);
    int32_t frac0 = frac1 + frac2;
    int32_t error = E_DEC_OK;

    const int32_t* buf1 = value1._buffer + intg1;
    const int32_t* buf2 = value2._buffer + intg2;

    // If E_DEC_OVERFLOW, save 'ideal' values. Note that no need to calculate fraction now.
    int32_t temp_intg = intg0;
    // if E_DEC_TRUNCATE, use to
    int32_t temp_frac = frac0;
    fix_intg_frac_error(to->_buffer_length, &intg0, &frac0, &error); // bound size
    to->_sign = (value1._sign != value2._sign) ? true : false;
    to->_int_length = intg0 * DIG_PER_DEC1;
    to->_frac_length = value1._frac_length + value2._frac_length; // store size in digits

    int32_t temp_to_frac_length = to->_frac_length;
    //ATTN: _int_length is bit-field struct member, can not take address directly.
    set_if_smaller(&temp_to_frac_length, NOT_FIXED_DEC);
    to->_frac_length = temp_to_frac_length;
    if (error) {
        int32_t temp_to_int_length = to->_int_length;
        set_if_smaller(&temp_to_int_length, intg0 * DIG_PER_DEC1);
        to->_int_length = temp_to_int_length;

        int32_t temp_to_frac_length = to->_frac_length;
        set_if_smaller(&temp_to_frac_length, frac0 * DIG_PER_DEC1);
        to->_frac_length = temp_to_frac_length;

        if (temp_intg > intg0) { // bounded integer-part, E_DEC_OVERFLOW
            temp_intg -= intg0;
            temp_frac = temp_intg >> 1;
            intg1 -= temp_frac;
            intg2 -= temp_intg - temp_frac;
            frac1 = frac2 = 0; // frac0 is already 0 here
        } else {               // bounded fract part, E_DEC_TRUNCATE
            temp_frac -= frac0;
            temp_intg = temp_frac >> 1;
            if (frac1 <= frac2) {
                frac1 -= temp_intg;
                frac2 -= temp_frac - temp_intg;
            } else {
                frac2 -= temp_intg;
                frac1 -= temp_frac - temp_intg;
            }
        }
    }
    int32_t* start0 = to->_buffer + intg0 + frac0 - 1;
    const int32_t* start2 = buf2 + frac2 - 1;
    const int32_t* stop1 = buf1 - intg1;
    const int32_t* stop2 = buf2 - intg2;
    int32_t* buf0 = nullptr;
    int32_t carry = 0;

    memset(to->_buffer, 0, (intg0 + frac0) * sizeof(int32_t));
    for (buf1 += frac1 - 1; buf1 >= stop1; buf1--, start0--) {
        carry = 0;
        for (buf0 = start0, buf2 = start2; buf2 >= stop2; buf2--, buf0--) {
            int64_t mul_result = ((int64_t)*buf1) * ((int64_t)*buf2);
            int32_t high = (int32_t)(mul_result / DIG_BASE);
            int32_t low = (int32_t)(mul_result - ((int64_t)high) * DIG_BASE);
            add2(*buf0, low, buf0, &carry);
            carry += high;
        }
        if (carry) {
            if (buf0 < to->_buffer) {
                return E_DEC_OVERFLOW;
            }
            add2(*buf0, 0, buf0, &carry);
        }
        // may carry again.
        for (buf0--; carry; buf0--) {
            if (buf0 < to->_buffer) {
                return E_DEC_OVERFLOW;
            }
            add2(*buf0, 0, buf0, &carry);
        }
    }

    // Now we have to check for '-0.000' case
    if (to->_sign) {
        int32_t* buf = to->_buffer;
        int32_t* end = to->_buffer + intg0 + frac0;
        for (; buf < end; ++buf) {
            if (*buf) {
                break;
            }
        }
        if (buf == end) {
            // We got decimal zero
            to->set_to_zero();
        }
    }

    // remove leading zeros.
    buf1 = to->_buffer;
    int32_t d_to_move = intg0 + round_up(to->_frac_length);
    while ((*buf1 == 0) && (to->_int_length > DIG_PER_DEC1)) {
        ++buf1;
        to->_int_length -= DIG_PER_DEC1;
        d_to_move--;
    }
    if (to->_buffer < buf1) {
        int32_t* cur_d = to->_buffer;
        for (; d_to_move--; cur_d++, buf1++) {
            *cur_d = *buf1;
        }
    }

    return error;
}

// if N1/N2 mod==NULL; if N1%N2 to==NULL;
int do_div_mod(const DecimalValue& value1, const DecimalValue& value2, DecimalValue* to,
               DecimalValue* mod) {
    int32_t frac1 = round_up(value1._frac_length) * DIG_PER_DEC1;
    int32_t frac2 = round_up(value2._frac_length) * DIG_PER_DEC1;
    int32_t prec1 = value1._int_length + frac1;
    int32_t prec2 = value2._int_length + frac2;
    int32_t error = E_DEC_OK;

    if (mod) {
        to = mod;
    }

    const int32_t* buff1 = value1._buffer;
    const int32_t* buff2 = value2._buffer;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    // removing all the leading zeros
    // process value2
    int32_t first_big_digit_length = (prec2 - 1) % DIG_PER_DEC1 + 1;
    while (prec2 > 0 && *buff2 == 0) {
        prec2 -= first_big_digit_length;
        first_big_digit_length = DIG_PER_DEC1;
        ++buff2;
    }
    if (prec2 <= 0) { // short-circuit everything, value2 == 0
        return E_DEC_DIV_ZERO;
    }
    first_big_digit_length = (prec2 - 1) % DIG_PER_DEC1 + 1;
    for (; *buff2 < powers10[--first_big_digit_length];) {
        --prec2;
    }

    // process value1
    first_big_digit_length = (prec1 - 1) % DIG_PER_DEC1 + 1;
    while (prec1 > 0 && *buff1 == 0) {
        prec1 -= first_big_digit_length;
        first_big_digit_length = DIG_PER_DEC1;
        ++buff1;
    }
    if (prec1 <= 0) { // short-circuit everything, value1 == 0
        to->set_to_zero();
        return E_DEC_OK;
    }
    first_big_digit_length = (prec1 - 1) % DIG_PER_DEC1 + 1;
    for (; *buff1 < powers10[--first_big_digit_length];) {
        --prec1;
    }
#pragma GCC diagnostic pop

    // 比较两个数的整形部分，得到结果的intg。 如果被除数较小，intg=0.
    int32_t dintg = (prec1 - frac1) - (prec2 - frac2) + (*buff1 >= *buff2);
    int32_t intg0 = 0; // big digit length
    int32_t frac0 = 0;
    if (dintg < 0) {
        dintg /= DIG_PER_DEC1;
        intg0 = 0;
    } else {
        intg0 = round_up(dintg);
    }

    if (mod) {
        // we are calculating N1 % N2. The result will have
        //  1) frac = max(frac1, frac2), as for subtraction
        //  2) intg = intg2.
        to->_sign = value1._sign;
        to->_frac_length = std::max(value1._frac_length, value2._frac_length);
        frac0 = 0;
    } else {
        // we are calculating N1/N2.
        // N1 is in the buff1, has prec1 digits; N2 is in the buff2, has prec2 digits.
        // Scales are frac1 and frac2 accordingly.
        // Thus, the result will have
        //  1) frac = round_up(frac1 + frac2)
        //  2) intg = (prec1 - frac1) - (prec2 - frac2) + 1
        //  3) prec = intg + frac

        frac0 = round_up(frac1 + frac2);
        if (frac0 == 0) {
            frac0 = 1;
        }
        fix_intg_frac_error(to->_buffer_length, &intg0, &frac0, &error);

        to->_sign = (value1._sign != value2._sign);
        to->_int_length = intg0 * DIG_PER_DEC1;
        to->_frac_length = frac0 * DIG_PER_DEC1;
    }

    int32_t* buff0 = to->_buffer;
    int32_t* stop0 = buff0 + intg0 + frac0;
    int32_t div_mod = !(mod); // true when do div, false when do mod.
    if (div_mod) {            // do div
        while (dintg++ < 0 && buff0 < &(to->_buffer[to->_buffer_length])) {
            *buff0++ = 0;
        }
    }

    int32_t i = round_up(prec1);
    int32_t len1 = i + round_up(2 * frac2 + 1) + 1;
    set_if_bigger(&len1, 3);
    int32_t* tmp1 = new (std::nothrow) int32_t[len1 * sizeof(int32_t)];
    if (tmp1 == nullptr) {
        return E_DEC_OOM;
    }
    memcpy(tmp1, buff1, i * sizeof(int32_t));
    memset(tmp1 + i, 0, (len1 - i) * sizeof(int32_t));

    int32_t* start1 = tmp1;
    int32_t* stop1 = start1 + len1;
    const int32_t* start2 = buff2;
    const int32_t* stop2 = buff2 + round_up(prec2) - 1;

    // removing end zeroes
    while (*stop2 == 0 && stop2 >= start2) {
        --stop2;
    }
    int32_t len2 = (int32_t)((stop2++) - start2);

    // calculating norm2 (normalized *start2) - we need *start2 to be large
    // (at least > DIG_BASE/2), but unlike Knuth's Alg. D we don't want to
    // normalize input numbers (as we don't make a copy of the divisor).
    // Thus we normalize first big_digit_type of buf2 only, and we'll normalize *start1
    // on the fly for the purpose of guesstimation only.
    // It's also faster, as we're saving on normalization of buf2
    int64_t norm_factor = DIG_BASE / (*start2 + 1);
    int32_t norm2 = (int32_t)(norm_factor * start2[0]);
    if (len2 > 0) {
        norm2 += (int32_t)(norm_factor * start2[1] / DIG_BASE);
    }

    int32_t dcarry;
    if (*start1 < *start2) {
        dcarry = *start1++;
    } else {
        dcarry = 0;
    }

    int64_t guess = 0;
    // main loop
    for (; buff0 < stop0; buff0++) {
        // short-circuit, if possible
        if (dcarry == 0 && *start1 < *start2) {
            guess = 0;
        } else {
            // D3: make a guess
            int64_t x = start1[0] + ((int64_t)dcarry) * DIG_BASE;
            int64_t y = start1[1];
            guess = (norm_factor * x + norm_factor * y / DIG_BASE) / norm2;
            if (guess >= DIG_BASE) {
                guess = DIG_BASE - 1;
            }
            if (len2 > 0) {
                // hmm, this is a suspicious trick - I removed normalization here
                if (start2[1] * guess > (x - guess * start2[0]) * DIG_BASE + y) {
                    guess--;
                }
                if (start2[1] * guess > (x - guess * start2[0]) * DIG_BASE + y) {
                    guess--;
                }
            }

            // D4: multiply and subtract
            buff2 = stop2;
            int32_t* temp_prt = start1 + len2;
            int32_t carry = 0;
            for (; buff2 > start2; --temp_prt) {
                int32_t high;
                int32_t low;
                x = guess * (*--buff2);
                high = (int32_t)(x / DIG_BASE);
                low = (int32_t)(x - ((int64_t)high) * DIG_BASE);
                sub2(*temp_prt, low, temp_prt, &carry);
                carry += high;
            }
            carry = dcarry < carry;

            // D5: check the remainder
            if (carry) {
                // D6: correct the guess
                guess--;
                buff2 = stop2;
                temp_prt = start1 + len2;
                for (carry = 0; buff2 > start2; temp_prt--) {
                    add(*temp_prt, *--buff2, temp_prt, &carry);
                }
            }
        }
        if (div_mod) {
            *buff0 = (int32_t)guess;
        }
        dcarry = *start1;
        ++start1;
    }

    do {
        if (mod) {
            // now the result is in tmp1, it has
            //  intg=prec1-frac1
            //  frac=max(frac1, frac2)=to->frac
            if (dcarry) {
                *--start1 = dcarry;
            }
            buff0 = to->_buffer;
            intg0 = (int)(round_up(prec1 - frac1) - (start1 - tmp1));
            frac0 = round_up(to->_frac_length);
            error = E_DEC_OK;
            if (frac0 == 0 && intg0 == 0) {
                to->set_to_zero();
                break;
            }
            if (intg0 <= 0) {
                if (-intg0 >= to->_buffer_length) {
                    error = E_DEC_TRUNCATED;
                    to->set_to_zero();
                    break;
                }
                stop1 = start1 + frac0 + intg0;
                frac0 += intg0;
                to->_int_length = 0;
                while (intg0++ < 0) {
                    *buff0++ = 0;
                }
            } else {
                if (intg0 > to->_buffer_length) {
                    frac0 = 0;
                    intg0 = to->_buffer_length;
                    error = E_DEC_OVERFLOW;
                    break;
                }
                stop1 = start1 + frac0 + intg0;
                to->_int_length = std::min(intg0 * DIG_PER_DEC1, (int)value2._int_length);
            }
            if (intg0 + frac0 > to->_buffer_length) {
                stop1 -= frac0 + intg0 - to->_buffer_length;
                frac0 = to->_buffer_length - intg0;
                to->_frac_length = frac0 * DIG_PER_DEC1;
                error = E_DEC_TRUNCATED;
            }
            while (start1 < stop1) {
                *buff0++ = *start1++;
            }
        }
    } while (0);

    delete[] tmp1;
    int32_t to_int_length = 0;
    const int32_t* first_no_zero = to->get_first_no_zero_index(&to_int_length);
    to->_int_length = to_int_length;
    if (to->_buffer != first_no_zero) {
        memmove(to->_buffer, first_no_zero,
                (round_up(to->_int_length) + round_up(to->_frac_length)) * sizeof(int32_t));
    }
    return error;
}

// TODO(lingbin): if ignore do_add's error code
DecimalValue operator+(const DecimalValue& v1, const DecimalValue& v2) {
    DecimalValue result;
    if (v1._sign == v2._sign) {
        do_add(v1, v2, &result);
    } else {
        do_sub(v1, v2, &result);
    }
    return result;
}

DecimalValue operator-(const DecimalValue& v1, const DecimalValue& v2) {
    DecimalValue result;
    if (v1._sign == v2._sign) {
        do_sub(v1, v2, &result);
    } else {
        do_add(v1, v2, &result);
    }
    return result;
}

DecimalValue operator*(const DecimalValue& v1, const DecimalValue& v2) {
    DecimalValue result;
    do_mul(v1, v2, &result);
    return result;
}

DecimalValue operator/(const DecimalValue& v1, const DecimalValue& v2) {
    DecimalValue result;
    do_div_mod(v1, v2, &result, nullptr);
    return result;
}

DecimalValue operator%(const DecimalValue& v1, const DecimalValue& v2) {
    DecimalValue result;
    do_div_mod(v1, v2, nullptr, &result);
    return result;
}

std::ostream& operator<<(std::ostream& os, DecimalValue const& decimal_value) {
    return os << decimal_value.to_string();
}

std::istream& operator>>(std::istream& ism, DecimalValue& decimal_value) {
    std::string str_buff;
    ism >> str_buff;
    decimal_value.parse_from_str(str_buff.c_str(), str_buff.size());
    return ism;
}

DecimalValue operator-(const DecimalValue& v) {
    DecimalValue result = v;
    result._sign = !result._sign;
    return result;
}

DecimalValue& DecimalValue::operator+=(const DecimalValue& other) {
    *this = *this + other;
    return *this;
}

int DecimalValue::parse_from_str(const char* decimal_str, int32_t length) {
    set_to_zero();
    const char* begin = decimal_str;
    const char* end = decimal_str + length;
    int32_t error = E_DEC_OK;

    // ignore leading spaces
    while (begin < end && std::isspace(*begin)) {
        ++begin;
    }
    if (begin == end) {
        set_to_zero();
        return E_DEC_BAD_NUM;
    }

    // positive or negative
    if (*begin == '-') {
        _sign = true;
        ++begin;
    } else if (*begin == '+') {
        _sign = false;
        ++begin;
    } else {
        _sign = false;
    }

    // count int_length and frac_length
    const char* temp_ptr = begin;
    const char* frac_ptr = nullptr;
    // after this loop, 'begin' point to the first non digital position
    while (begin < end && std::isdigit(*begin)) {
        ++begin;
    }
    int32_t int_len = (int32_t)(begin - temp_ptr);
    int32_t frac_len = 0;
    if (begin < end && *begin == '.') {
        frac_ptr = begin + 1;
        while (frac_ptr < end && std::isdigit(*frac_ptr)) {
            ++frac_ptr;
        }
        frac_len = frac_ptr - begin - 1; // -1 for char '.'
    } else {
        frac_len = 0;
        frac_ptr = begin;
    }

    // bad num like "  a"
    if ((int_len + frac_len) == 0) {
        set_to_zero();
        return E_DEC_BAD_NUM;
    }

    int32_t int_big_digit_len = round_up(int_len);
    int32_t frac_big_digit_len = round_up(frac_len);
    fix_intg_frac_error(_buffer_length, &int_big_digit_len, &frac_big_digit_len, &error);
    if (error) {
        frac_len = frac_big_digit_len * DIG_PER_DEC1;
        if (error == E_DEC_OVERFLOW) {
            int_len = int_big_digit_len * DIG_PER_DEC1;
        }
    }

    _int_length = int_len;
    _frac_length = frac_len;
    temp_ptr = begin;

    // fill int value
    int32_t* buff = _buffer + int_big_digit_len;
    int32_t value = 0;
    int32_t index_in_powers10 = 0;
    for (; int_len > 0; --int_len) {
        value += (*--temp_ptr - '0') * powers10[index_in_powers10];
        ++index_in_powers10;
        if (index_in_powers10 == DIG_PER_DEC1) {
            *--buff = value;
            value = 0;
            index_in_powers10 = 0;
        }
    }
    if (index_in_powers10) {
        *--buff = value;
    }

    // fill fraction value
    buff = _buffer + int_big_digit_len;
    temp_ptr = begin;
    for (value = 0, index_in_powers10 = 0; frac_len > 0; --frac_len) {
        value = (*++temp_ptr - '0') + value * 10;
        ++index_in_powers10;
        if (index_in_powers10 == DIG_PER_DEC1) {
            *buff++ = value;
            value = 0;
            index_in_powers10 = 0;
        }
    }
    if (index_in_powers10) {
        *buff = value * powers10[DIG_PER_DEC1 - index_in_powers10];
    }

    // TODO: we do not support decimal in scientific notation
    if ((frac_ptr + 1) < end && (*frac_ptr == 'e' || *frac_ptr == 'E')) {
        // return E_DEC_BAD_NUM;
        int64_t exponent = strtoll(frac_ptr + 1, (char**)&end, 10);
        if (end != frac_ptr + 1) { // If at least one digit
            if (errno) {           // system error number, it is thread local
                set_to_zero();
                return E_DEC_BAD_NUM;
            }
            if (exponent > (INT_MAX / 2) || (errno == 0 && exponent < 0)) {
                set_to_zero();
                return E_DEC_OVERFLOW;
            }
            if (exponent < INT_MAX / 2 && error != E_DEC_OVERFLOW) {
                set_to_zero();
                return E_DEC_TRUNCATED;
            }
            if (error != E_DEC_OVERFLOW) {
                // error = shift((int32_t) exponent);
            }
        }
    }
    if (_sign && is_zero()) {
        _sign = false;
    }
    return error;
}

// TODO(lingbin): should be refactored with to_string(int scale)
std::string DecimalValue::to_string() const {
    // Ignore trailing zeroes
    int32_t intg = round_up(_int_length);
    int32_t frac = round_up(_frac_length);
    const int32_t* frac_begin = _buffer + intg;
    const int32_t* frac_end = frac_begin + (frac - 1);

    const int32_t* buff = frac_end;
    while ((buff < frac_end) && (*buff == 0)) {
        --buff;
    }

    int32_t actual_frac = (int32_t)(buff - frac_begin) + 1;
    int32_t actual_frac_len = actual_frac * DIG_PER_DEC1;

    // Count the number of zeroes at the end of last "big digit" number
    if (actual_frac_len > 0) {
        int32_t last_big_digit = *buff;
        while ((last_big_digit > 0) && (last_big_digit % 10) == 0) {
            --actual_frac_len;
            last_big_digit /= 10;
        }
    }

    return to_string(actual_frac_len);
}

std::string DecimalValue::to_string(int scale) const {
    int32_t temp_intg = _int_length;
    int32_t temp_frac = _frac_length;
    if (temp_frac > scale) {
        temp_frac = scale;
    }
    const int32_t* buff_no_zero = get_first_no_zero_index(&temp_intg);

    int32_t temp = 0;
    if ((temp_intg + temp_frac) == 0) {
        temp_intg = 1;
        buff_no_zero = &temp;
    }

    int32_t int_str_length = temp_intg;
    if (temp_intg == 0) {
        int_str_length = 1;
    }
    int32_t length = (_sign ? 1 : 0) + int_str_length + (temp_frac ? 1 : 0) + temp_frac;

    char result_str[DECIMAL_MAX_STR_LENGTH + 1];
    char* result_ptr = result_str;

    result_ptr[length] = '\0';
    if (_sign) {
        *result_ptr++ = '-';
    }
    if (temp_frac) {
        char* char_point = result_ptr + int_str_length;
        int32_t fill_length = scale - temp_frac;
        const int32_t* buff = buff_no_zero + round_up(temp_intg);
        *char_point++ = '.';
        for (; temp_frac > 0; temp_frac -= DIG_PER_DEC1) {
            int32_t m = *buff++;
            for (int32_t i = std::min(temp_frac, DIG_PER_DEC1); i; --i) {
                int32_t n = m / DIG_MASK;
                *char_point++ = '0' + n;
                m -= n * DIG_MASK;
                m *= 10;
            }
        }
        while (fill_length-- > 0) {
            *result_ptr = '0'; // use char '0' to fill
        }
    }

    if (temp_intg) {
        char* char_point = result_ptr + int_str_length;
        const int32_t* buff = buff_no_zero + round_up(temp_intg);
        for (; temp_intg > 0; temp_intg -= DIG_PER_DEC1) {
            int32_t m = *--buff;
            for (int32_t i = std::min(temp_intg, DIG_PER_DEC1); i; --i) {
                int32_t n = m / 10;
                *--char_point = '0' + (m - n * 10);
                m = n;
            }
        }
    } else {
        *result_ptr = '0';
    }

    return std::string(result_str, length);
}

// NOTE: only change abstract value, do not change sign
void DecimalValue::to_max_decimal(int32_t precision, int32_t frac) {
    int32_t* buf = _buffer;

    _int_length = precision - frac;
    int32_t intpart = precision - frac;
    if (intpart) {
        int32_t firstdigits = intpart % DIG_PER_DEC1;
        if (firstdigits) {
            *buf++ = powers10[firstdigits] - 1; // get 9 99 999 ...
        }
        for (intpart /= DIG_PER_DEC1; intpart; intpart--) {
            *buf++ = DIG_MAX;
        }
    }

    _frac_length = frac;
    if (frac) {
        int32_t lastdigits = frac % DIG_PER_DEC1;
        for (frac /= DIG_PER_DEC1; frac; frac--) {
            *buf++ = DIG_MAX;
        }
        if (lastdigits) {
            *buf = frac_max[lastdigits - 1];
        }
    }
}

std::size_t hash_value(DecimalValue const& value) {
    return value.hash(0);
}

int DecimalValue::round(DecimalValue* to, int scale, DecimalRoundMode mode) {
    int frac0 = scale > 0 ? round_up(scale) : (scale + 1) / DIG_PER_DEC1;
    int frac1 = round_up(_frac_length);
    int intg0 = round_up(_int_length);
    int error = E_DEC_OK;
    int len = _buffer_length;

    int32_t* buf0 = _buffer;
    int32_t* buf1 = to->_buffer;
    int32_t x = 0;
    int32_t y = 0;
    int32_t carry = 0;
    int first_dig = 0;

    int round_digit = 0;
    switch (mode) {
    case HALF_UP:
    case HALF_EVEN:
        round_digit = 5;
        break;
    case CEILING:
        round_digit = _sign ? 10 : 0;
        break;
    case FLOOR:
        round_digit = _sign ? 0 : 10;
        break;
    case TRUNCATE:
        round_digit = 10;
        break;
    default:
        return E_DEC_ERROR;
    }

    // input is too large, make it meaningful
    if (frac0 + intg0 > len) {
        frac0 = len - intg0;
        scale = frac0 * DIG_PER_DEC1;
        error = E_DEC_TRUNCATED;
    }
    // zero
    if (scale + _int_length < 0) {
        to->set_to_zero();
        return E_DEC_OK;
    }
    // normal case copy
    if (to != this) {
        int32_t* p0 = buf0 + intg0 + std::max(frac1, frac0);
        int32_t* p1 = buf1 + intg0 + std::max(frac1, frac0);

        while (buf0 < p0) {
            *(--p1) = *(--p0);
        }

        buf0 = to->_buffer;
        buf1 = to->_buffer;
        to->_sign = _sign;
        to->_int_length = std::min(intg0, len) * DIG_PER_DEC1;
    }
    // no need to trunk
    if (frac0 > frac1) {
        buf1 += intg0 + frac1;
        while (frac0-- > frac1) {
            *buf1++ = 0;
        }
        to->_frac_length = scale;
        return error;
    }
    if (scale >= _frac_length) {
        /* nothing to do */
        to->_frac_length = scale;
        return error;
    }

    buf0 += intg0 + frac0 - 1;
    buf1 += intg0 + frac0 - 1;
    if (scale == frac0 * DIG_PER_DEC1) {
        bool do_inc = false;
        switch (round_digit) {
        case 0: {
            int32_t* p0 = buf0 + (frac1 - frac0);
            for (; p0 > buf0; p0--) {
                if (*p0) {
                    do_inc = true;
                    break;
                }
            }
            break;
        }
        case 5: {
            x = buf0[1] / DIG_MASK;
            do_inc = (x > 5) || ((x == 5) && (mode == HALF_UP || (frac0 + intg0 > 0 && *buf0 & 1)));
            break;
        }
        default:
            break;
        }
        if (do_inc) {
            if (frac0 + intg0 > 0) {
                (*buf1)++;
            } else {
                *(++buf1) = DIG_BASE;
            }
        } else if (frac0 + intg0 == 0) {
            to->set_to_zero();
            return E_DEC_OK;
        }
    } else {
        /* TODO - fix this code as it won't work for CEILING mode */
        int pos = frac0 * DIG_PER_DEC1 - scale - 1;
        x = *buf1 / powers10[pos];
        y = x % 10;
        if (y > round_digit || (round_digit == 5 && y == 5 && (mode == HALF_UP || (x / 10) & 1))) {
            x += 10;
        }
        *buf1 = powers10[pos] * (x - y);
    }
    /*
       In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal_digit_t's inside
       the buffer are as follows.

       Before <1, 5e8>
       After  <2, 5e8>

       Hence we need to set the 2nd field to 0.
       The same holds if we round 1.5e-9 to 2e-9.
       */
    if (frac0 < frac1) {
        int32_t* buf = to->_buffer + ((scale == 0 && intg0 == 0) ? 1 : intg0 + frac0);
        int32_t* end = to->_buffer + len;
        while (buf < end) {
            *buf++ = 0;
        }
    }
    if (*buf1 >= DIG_BASE) {
        carry = 1;
        *buf1 -= DIG_BASE;
        while (carry && --buf1 >= to->_buffer) {
            add(0, *buf1, buf1, &carry);
        }
        if (carry) {
            /* shifting the number to create space for new digit */
            if (frac0 + intg0 >= len) {
                frac0--;
                scale = frac0 * DIG_PER_DEC1;
                error = E_DEC_TRUNCATED; /* XXX */
            }
            for (buf1 = to->_buffer + intg0 + std::max(frac0, 0); buf1 > to->_buffer; buf1--) {
                /* Avoid out-of-bounds write. */
                if (buf1 < to->_buffer + len) {
                    buf1[0] = buf1[-1];
                } else {
                    error = E_DEC_OVERFLOW;
                }
            }
            *buf1 = 1;
            /* We cannot have more than 9 * 9 = 81 digits. */
            if (to->_int_length < len * DIG_PER_DEC1) {
                to->_int_length++;
            } else {
                error = E_DEC_OVERFLOW;
            }
        }
    } else {
        for (;;) {
            if (*buf1) {
                break;
            }
            if (buf1-- == to->_buffer) {
                /* making 'zero' with the proper scale */
                int32_t* p0 = to->_buffer + frac0 + 1;
                to->_int_length = 1;
                to->_frac_length = std::max(scale, 0);
                to->_sign = 0;
                for (buf1 = to->_buffer; buf1 < p0; buf1++) {
                    *buf1 = 0;
                }
                return E_DEC_OK;
            }
        }
    }

    /* Here we  check 999.9 -> 1000 case when we need to increase intg */
    first_dig = to->_int_length % DIG_PER_DEC1;
    if (first_dig && (*buf1 >= powers10[first_dig])) {
        to->_int_length++;
    }

    if (scale < 0) {
        scale = 0;
    }

    to->_frac_length = scale;
    return error;
}

} // end namespace doris
