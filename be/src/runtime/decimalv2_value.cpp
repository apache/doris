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

#include "runtime/decimalv2_value.h"

#include <fmt/format.h>

#include <cmath>
#include <cstring>
#include <iostream>
#include <utility>

#include "util/frame_of_reference_coding.h"
#include "util/string_parser.hpp"

namespace doris {

const int128_t DecimalV2Value::MAX_DECIMAL_VALUE;

static inline int128_t abs(const int128_t& x) {
    return (x < 0) ? -x : x;
}

// x>=0 && y>=0
static int do_add(int128_t x, int128_t y, int128_t* result) {
    int error = E_DEC_OK;
    if (DecimalV2Value::MAX_DECIMAL_VALUE - x >= y) {
        *result = x + y;
    } else {
        *result = DecimalV2Value::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
    }
    return error;
}

// x>=0 && y>=0
static int do_sub(int128_t x, int128_t y, int128_t* result) {
    int error = E_DEC_OK;
    *result = x - y;
    return error;
}

// clear leading zero for __int128
static int clz128(unsigned __int128 v) {
    if (v == 0) return sizeof(__int128);
    unsigned __int128 shifted = v >> 64;
    if (shifted != 0) {
        return leading_zeroes(shifted);
    } else {
        return leading_zeroes(v) + 64;
    }
}

// x>0 && y>0
static int do_mul(int128_t x, int128_t y, int128_t* result) {
    int error = E_DEC_OK;
    int128_t max128 = ~(static_cast<int128_t>(1ll) << 127);

    int leading_zero_bits = clz128(x) + clz128(y);
    if (leading_zero_bits < sizeof(int128_t) || max128 / x < y) {
        *result = DecimalV2Value::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
        return error;
    }

    int128_t product = x * y;
    *result = product / DecimalV2Value::ONE_BILLION;

    // overflow
    if (*result > DecimalV2Value::MAX_DECIMAL_VALUE) {
        *result = DecimalV2Value::MAX_DECIMAL_VALUE;
        error = E_DEC_OVERFLOW;
        return error;
    }

    // truncate with round
    int128_t remainder = product % DecimalV2Value::ONE_BILLION;
    if (remainder != 0) {
        error = E_DEC_TRUNCATED;
        if (remainder >= (DecimalV2Value::ONE_BILLION >> 1)) {
            *result += 1;
        }
    }

    return error;
}

// x>0 && y>0
static int do_div(int128_t x, int128_t y, int128_t* result) {
    int error = E_DEC_OK;
    int128_t dividend = x * DecimalV2Value::ONE_BILLION;
    *result = dividend / y;

    // overflow
    int128_t remainder = dividend % y;
    if (remainder != 0) {
        error = E_DEC_TRUNCATED;
        if (remainder >= (y >> 1)) {
            *result += 1;
        }
    }

    return error;
}

// x>0 && y>0
static int do_mod(int128_t x, int128_t y, int128_t* result) {
    int error = E_DEC_OK;
    *result = x % y;
    return error;
}

DecimalV2Value operator+(const DecimalV2Value& v1, const DecimalV2Value& v2) {
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();
    if (x == 0) {
        result = y;
    } else if (y == 0) {
        result = x;
    } else if (x > 0) {
        if (y > 0) {
            do_add(x, y, &result);
        } else {
            do_sub(x, -y, &result);
        }
    } else { // x < 0
        if (y > 0) {
            do_sub(y, -x, &result);
        } else {
            do_add(-x, -y, &result);
            result = -result;
        }
    }

    return DecimalV2Value(result);
}

DecimalV2Value operator-(const DecimalV2Value& v1, const DecimalV2Value& v2) {
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();
    if (x == 0) {
        result = -y;
    } else if (y == 0) {
        result = x;
    } else if (x > 0) {
        if (y > 0) {
            do_sub(x, y, &result);
        } else {
            do_add(x, -y, &result);
        }
    } else { // x < 0
        if (y > 0) {
            do_add(-x, y, &result);
            result = -result;
        } else {
            do_sub(-x, -y, &result);
            result = -result;
        }
    }

    return DecimalV2Value(result);
}

DecimalV2Value operator*(const DecimalV2Value& v1, const DecimalV2Value& v2) {
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    if (x == 0 || y == 0) return DecimalV2Value(0);

    bool is_positive = (x > 0 && y > 0) || (x < 0 && y < 0);

    do_mul(abs(x), abs(y), &result);

    if (!is_positive) result = -result;

    return DecimalV2Value(result);
}

DecimalV2Value operator/(const DecimalV2Value& v1, const DecimalV2Value& v2) {
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    DCHECK(y != 0);
    if (x == 0 || y == 0) return DecimalV2Value(0);
    bool is_positive = (x > 0 && y > 0) || (x < 0 && y < 0);
    do_div(abs(x), abs(y), &result);

    if (!is_positive) result = -result;

    return DecimalV2Value(result);
}

DecimalV2Value operator%(const DecimalV2Value& v1, const DecimalV2Value& v2) {
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    DCHECK(y != 0);
    if (x == 0 || y == 0) return DecimalV2Value(0);

    do_mod(x, y, &result);

    return DecimalV2Value(result);
}

std::ostream& operator<<(std::ostream& os, DecimalV2Value const& decimal_value) {
    return os << decimal_value.to_string();
}

std::istream& operator>>(std::istream& ism, DecimalV2Value& decimal_value) {
    std::string str_buff;
    ism >> str_buff;
    decimal_value.parse_from_str(str_buff.c_str(), str_buff.size());
    return ism;
}

DecimalV2Value operator-(const DecimalV2Value& v) {
    return DecimalV2Value(-v.value());
}

DecimalV2Value& DecimalV2Value::operator+=(const DecimalV2Value& other) {
    *this = *this + other;
    return *this;
}

// Solve a one-dimensional quadratic equation: ax2 + bx + c =0
// Reference: https://gist.github.com/miloyip/1fcc1859c94d33a01957cf41a7c25fdf
// Reference: https://www.zhihu.com/question/51381686
static std::pair<double, double> quadratic_equation_naive(__uint128_t a, __uint128_t b,
                                                          __uint128_t c) {
    __uint128_t dis = b * b - 4 * a * c;
    // assert(dis >= 0);
    // not handling complex root
    double sqrtdis = std::sqrt(static_cast<double>(dis));
    double a_r = static_cast<double>(a);
    double b_r = static_cast<double>(b);
    double x1 = (-b_r - sqrtdis) / (a_r + a_r);
    double x2 = (-b_r + sqrtdis) / (a_r + a_r);
    return std::make_pair(x1, x2);
}

static inline double sgn(double x) {
    if (x > 0)
        return 1;
    else if (x < 0)
        return -1;
    else
        return 0;
}

// In the above quadratic_equation_naive solution process, we found that -b + sqrtdis will
// get the correct answer, and -b-sqrtdis will get the wrong answer. For two close floating-point
// decimals a, b, a-b will cause larger errors than a + b, which is called catastrophic cancellation.
// Both -b and sqrtdis are positive numbers. We can first find the roots brought by -b + sqrtdis,
// and then use the product of the two roots of the quadratic equation in one unknown to find another root
static std::pair<double, double> quadratic_equation_better(int128_t a, int128_t b, int128_t c) {
    if (b == 0) return quadratic_equation_naive(a, b, c);
    int128_t dis = b * b - 4 * a * c;
    // assert(dis >= 0);
    // not handling complex root
    if (dis < 0) return std::make_pair(0, 0);

    // There may be a loss of precision, but here is used to find the mantissa of the square root.
    // The current SCALE=9, which is less than the 15 significant digits of the double type,
    // so theoretically the loss of precision will not be reflected in the result.
    double sqrtdis = std::sqrt(static_cast<double>(dis));
    double a_r = static_cast<double>(a);
    double b_r = static_cast<double>(b);
    double c_r = static_cast<double>(c);
    // Here b comes from an unsigned integer, and sgn(b) is always 1,
    // which is only used to preserve the complete algorithm
    double x1 = (-b_r - sgn(b_r) * sqrtdis) / (a_r + a_r);
    double x2 = c_r / (a_r * x1);
    return std::make_pair(x1, x2);
}

// Large integer square roots, returns the integer part.
// The time complexity is lower than the traditional dichotomy
// and Newton iteration method, and the number of iterations is fixed.
// in real-time systems, functions that execute an unpredictable number of iterations
// will make the total time per task unpredictable, and introduce jitter
// Reference: https://www.embedded.com/integer-square-roots/
// Reference: https://link.zhihu.com/?target=https%3A//gist.github.com/miloyip/69663b78b26afa0dcc260382a6034b1a
// Reference: https://www.zhihu.com/question/35122102
static std::pair<__uint128_t, __uint128_t> sqrt_integer(__uint128_t n) {
    __uint128_t remainder = 0, root = 0;
    for (size_t i = 0; i < 64; i++) {
        root <<= 1;
        ++root;
        remainder <<= 2;
        remainder |= n >> 126;
        n <<= 2; // Extract 2 MSB from n
        if (root <= remainder) {
            remainder -= root;
            ++root;
        } else {
            --root;
        }
    }
    return std::make_pair(root >>= 1, remainder);
}

// According to the integer part and the remainder of the square root,
// Use one-dimensional quadratic equation to solve the fractional part of the square root
static double sqrt_fractional(int128_t sqrt_int, int128_t remainder) {
    std::pair<double, double> p = quadratic_equation_better(1, 2 * sqrt_int, -remainder);
    if ((0 < p.first) && (p.first < 1)) return p.first;
    if ((0 < p.second) && (p.second < 1)) return p.second;
    return 0;
}

const int128_t DecimalV2Value::SQRT_MOLECULAR_MAGNIFICATION = get_scale_base(PRECISION / 2);
const int128_t DecimalV2Value::SQRT_DENOMINATOR =
        int128_t(std::sqrt(ONE_BILLION) * get_scale_base(PRECISION / 2 - SCALE));

DecimalV2Value DecimalV2Value::sqrt(const DecimalV2Value& v) {
    int128_t x = v.value();
    std::pair<__uint128_t, __uint128_t> sqrt_integer_ret;
    bool is_negative = (x < 0);
    if (x == 0) {
        return DecimalV2Value(0);
    }
    sqrt_integer_ret = sqrt_integer(abs(x));
    int128_t integer_root = static_cast<int128_t>(sqrt_integer_ret.first);
    int128_t integer_remainder = static_cast<int128_t>(sqrt_integer_ret.second);
    double fractional = sqrt_fractional(integer_root, integer_remainder);

    // Multiplying by SQRT_MOLECULAR_MAGNIFICATION here will not overflow,
    // because integer_root can be up to 64 bits.
    int128_t molecular_integer = integer_root * SQRT_MOLECULAR_MAGNIFICATION;
    int128_t molecular_fractional =
            static_cast<int128_t>(fractional * SQRT_MOLECULAR_MAGNIFICATION);
    int128_t ret = (molecular_integer + molecular_fractional) / SQRT_DENOMINATOR;
    if (is_negative) ret = -ret;
    return DecimalV2Value(ret);
}

int DecimalV2Value::parse_from_str(const char* decimal_str, int32_t length) {
    int32_t error = E_DEC_OK;
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;

    _value = StringParser::string_to_decimal<TYPE_DECIMALV2>(decimal_str, length, PRECISION, SCALE,
                                                             &result);
    if (!config::allow_invalid_decimalv2_literal && result != StringParser::PARSE_SUCCESS) {
        error = E_DEC_BAD_NUM;
    } else if (config::allow_invalid_decimalv2_literal && result == StringParser::PARSE_FAILURE) {
        error = E_DEC_BAD_NUM;
    }
    return error;
}

std::string DecimalV2Value::to_string(int scale) const {
    int64_t int_val = int_value();
    int32_t frac_val = abs(frac_value());
    if (scale < 0 || scale > SCALE) {
        if (frac_val == 0) {
            scale = 0;
        } else {
            scale = SCALE;
            while (frac_val != 0 && frac_val % 10 == 0) {
                frac_val = frac_val / 10;
                scale--;
            }
        }
    } else {
        // roundup to FIX 17191
        if (scale < SCALE) {
            int32_t frac_val_tmp = frac_val / SCALE_TRIM_ARRAY[scale];
            if (frac_val / SCALE_TRIM_ARRAY[scale + 1] % 10 >= 5) {
                frac_val_tmp++;
                if (frac_val_tmp >= SCALE_TRIM_ARRAY[9 - scale]) {
                    frac_val_tmp = 0;
                    _value >= 0 ? int_val++ : int_val--;
                }
            }
            frac_val = frac_val_tmp;
        }
    }
    auto f_int = fmt::format_int(int_val);
    if (scale == 0) {
        return f_int.str();
    }
    std::string str;
    if (_value < 0 && int_val == 0 && frac_val != 0) {
        str.reserve(f_int.size() + scale + 2);
        str.push_back('-');
    } else {
        str.reserve(f_int.size() + scale + 1);
    }
    str.append(f_int.data(), f_int.size());
    str.push_back('.');
    if (frac_val == 0) {
        str.append(scale, '0');
    } else {
        auto f_frac = fmt::format_int(frac_val);
        if (f_frac.size() < scale) {
            str.append(scale - f_frac.size(), '0');
        }
        str.append(f_frac.data(), f_frac.size());
    }
    return str;
}

int32_t DecimalV2Value::to_buffer(char* buffer, int scale) const {
    int64_t int_val = int_value();
    int32_t frac_val = abs(frac_value());
    if (scale < 0 || scale > SCALE) {
        if (frac_val == 0) {
            scale = 0;
        } else {
            scale = SCALE;
            while (frac_val != 0 && frac_val % 10 == 0) {
                frac_val = frac_val / 10;
                scale--;
            }
        }
    } else {
        // roundup to FIX 17191
        if (scale < SCALE) {
            int32_t frac_val_tmp = frac_val / SCALE_TRIM_ARRAY[scale];
            if (frac_val / SCALE_TRIM_ARRAY[scale + 1] % 10 >= 5) {
                frac_val_tmp++;
                if (frac_val_tmp >= SCALE_TRIM_ARRAY[9 - scale]) {
                    frac_val_tmp = 0;
                    _value >= 0 ? int_val++ : int_val--;
                }
            }
            frac_val = frac_val_tmp;
        }
    }
    int extra_sign_size = 0;
    if (_value < 0 && int_val == 0 && frac_val != 0) {
        *buffer++ = '-';
        extra_sign_size = 1;
    }
    auto f_int = fmt::format_int(int_val);
    memcpy(buffer, f_int.data(), f_int.size());
    if (scale == 0) {
        return f_int.size();
    }
    *(buffer + f_int.size()) = '.';
    buffer = buffer + f_int.size() + 1;
    if (frac_val == 0) {
        memset(buffer, '0', scale);
    } else {
        auto f_frac = fmt::format_int(frac_val);
        if (f_frac.size() < scale) {
            memset(buffer, '0', scale - f_frac.size());
            buffer = buffer + scale - f_frac.size();
        }
        memcpy(buffer, f_frac.data(), f_frac.size());
    }
    return f_int.size() + scale + 1 + extra_sign_size;
}

std::string DecimalV2Value::to_string() const {
    return to_string(-1);
}

// NOTE: only change abstract value, do not change sign
void DecimalV2Value::to_max_decimal(int32_t precision, int32_t scale) {
    static const int64_t INT_MAX_VALUE[PRECISION] = {9ll,
                                                     99ll,
                                                     999ll,
                                                     9999ll,
                                                     99999ll,
                                                     999999ll,
                                                     9999999ll,
                                                     99999999ll,
                                                     999999999ll,
                                                     9999999999ll,
                                                     99999999999ll,
                                                     999999999999ll,
                                                     9999999999999ll,
                                                     99999999999999ll,
                                                     999999999999999ll,
                                                     9999999999999999ll,
                                                     99999999999999999ll,
                                                     999999999999999999ll};
    static const int32_t FRAC_MAX_VALUE[SCALE] = {900000000, 990000000, 999000000,
                                                  999900000, 999990000, 999999000,
                                                  999999900, 999999990, 999999999};

    // precision > 0 && scale >= 0 && scale <= SCALE
    if (precision <= 0 || scale < 0) return;
    if (scale > SCALE) scale = SCALE;

    // precision: (scale, PRECISION]
    if (precision > PRECISION) precision = PRECISION;
    if (precision - scale > PRECISION - SCALE) {
        precision = PRECISION - SCALE + scale;
    } else if (precision <= scale) {
        LOG(WARNING) << "Warning: error precision: " << precision << " or scale: " << scale;
        precision = scale + 1; // correct error precision
    }

    int64_t int_value = INT_MAX_VALUE[precision - scale - 1];
    int64_t frac_value = scale == 0 ? 0 : FRAC_MAX_VALUE[scale - 1];
    _value = static_cast<int128_t>(int_value) * DecimalV2Value::ONE_BILLION + frac_value;
}

std::size_t hash_value(DecimalV2Value const& value) {
    return value.hash(0);
}

int DecimalV2Value::round(DecimalV2Value* to, int rounding_scale, DecimalRoundMode op) {
    int32_t error = E_DEC_OK;
    int128_t result;

    if (rounding_scale >= SCALE) return error;
    if (rounding_scale < -(PRECISION - SCALE)) return 0;

    int128_t base = get_scale_base(SCALE - rounding_scale);
    result = _value / base;

    int one = _value > 0 ? 1 : -1;
    int128_t remainder = _value % base;
    switch (op) {
    case HALF_UP:
    case HALF_EVEN:
        if (abs(remainder) >= (base >> 1)) {
            result = (result + one) * base;
        } else {
            result = result * base;
        }
        break;
    case CEILING:
        if (remainder > 0 && _value > 0) {
            result = (result + one) * base;
        } else {
            result = result * base;
        }
        break;
    case FLOOR:
        if (remainder < 0 && _value < 0) {
            result = (result + one) * base;
        } else {
            result = result * base;
        }
        break;
    case TRUNCATE:
        result = result * base;
        break;
    default:
        break;
    }

    to->set_value(result);
    return error;
}

bool DecimalV2Value::greater_than_scale(int scale) {
    if (scale >= SCALE || scale < 0) {
        return false;
    } else if (scale == SCALE) {
        return true;
    }

    int frac_val = frac_value();
    if (scale == 0) {
        bool ret = frac_val == 0 ? false : true;
        return ret;
    }

    static const int values[SCALE] = {1,      10,      100,      1000,     10000,
                                      100000, 1000000, 10000000, 100000000};

    int base = values[SCALE - scale];
    if (frac_val % base != 0) return true;
    return false;
}

} // end namespace doris
