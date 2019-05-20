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
#include "util/string_parser.hpp"

#include <algorithm>
#include <iostream>
#include <utility>

namespace doris {

const char* DecimalV2Value::_s_llvm_class_name = "class.doris::DecimalV2Value";

static inline int128_t abs(const int128_t& x) { return (x < 0) ? -x : x; }

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
        return __builtin_clzll(shifted);
    } else {
        return __builtin_clzll(v) + 64;
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

DecimalV2Value operator*(const DecimalV2Value& v1, const DecimalV2Value& v2){
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    if (x == 0 || y == 0) return DecimalV2Value(0);

    bool is_positive = (x > 0 && y > 0) || (x < 0 && y < 0);

    do_mul(abs(x), abs(y), &result);

    if (!is_positive) result = -result;

    return DecimalV2Value(result);
}

DecimalV2Value operator/(const DecimalV2Value& v1, const DecimalV2Value& v2){
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    //todo: return 0 for divide zero 
    if (x == 0 || y == 0) return DecimalV2Value(0);
    bool is_positive = (x > 0 && y > 0) || (x < 0 && y < 0);
    do_div(abs(x), abs(y), &result);

    if (!is_positive) result = -result;

    return DecimalV2Value(result);
}

DecimalV2Value operator%(const DecimalV2Value& v1, const DecimalV2Value& v2){
    int128_t result;
    int128_t x = v1.value();
    int128_t y = v2.value();

    //todo: return 0 for divide zero 
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

int DecimalV2Value::parse_from_str(const char* decimal_str, int32_t length) {
    int32_t error = E_DEC_OK;
    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    
    _value = StringParser::string_to_decimal(decimal_str, length, 
		    PRECISION, SCALE, &result);

    if (result == StringParser::PARSE_FAILURE) { 
       error = E_DEC_BAD_NUM;
    }
    return error;
}

std::string DecimalV2Value::to_string(int round_scale) const {
    if (_value == 0) return std::string(1, '0');

    int last_char_idx = PRECISION + 2 + (_value < 0);  
    std::string str = std::string(last_char_idx, '0');

    int128_t remaining_value = _value;
    int first_digit_idx = 0;
    if (_value < 0) {
        remaining_value = -_value;
        first_digit_idx = 1;
    }

    int remaining_scale = SCALE;
    do {
        str[--last_char_idx] = (remaining_value % 10) + '0'; 
        remaining_value /= 10;
    } while (--remaining_scale > 0);
    str[--last_char_idx] = '.';

    do {
        str[--last_char_idx] = (remaining_value % 10) + '0';
        remaining_value /= 10;
        if (remaining_value == 0) {
            if (last_char_idx > first_digit_idx) str.erase(0, last_char_idx - first_digit_idx);
            break;
        }
    } while (last_char_idx > first_digit_idx);

    if (_value < 0) str[0] = '-';

    // right trim and round
    int scale = 0;
    int len = str.size();
    for(scale = 0; scale < SCALE && scale < len; scale++) {
        if (str[len - scale - 1] != '0') break;
    }
    if (scale == SCALE) scale++; //integer, trim .
    if (round_scale >= 0 && round_scale <= SCALE) {
        scale = std::max(scale, SCALE - round_scale);
    }
    if (scale > 1 && scale <= len) str.erase(len - scale, len - 1);

    return str;
}

std::string DecimalV2Value::to_string() const {
    return to_string(-1);
}

// NOTE: only change abstract value, do not change sign
void DecimalV2Value::to_max_decimal(int32_t precision, int32_t scale) {
    bool is_negtive = (_value < 0);
    static const int64_t INT_MAX_VALUE[PRECISION] = {
        9ll, 
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
        999999999999999999ll
    };
    static const int32_t FRAC_MAX_VALUE[SCALE] = { 
        900000000, 
        990000000, 
        999000000,
        999900000, 
        999990000, 
        999999000,
        999999900, 
        999999990, 
        999999999
    };

    // precison > 0 && scale >= 0 && scale <= SCALE
    if (precision <= 0 || scale < 0) return;
    if (scale > SCALE) scale = SCALE;

    // precision: (scale, PRECISION]
    if (precision > PRECISION) precision = PRECISION;
    if (precision - scale > PRECISION - SCALE) {
        precision = PRECISION - SCALE + scale;
    } else if (precision <= scale) {
        LOG(WARNING) << "Warning: error precision: " << precision << " or scale: " << scale;
        precision = scale + 1; // corect error precision
    }

    int64_t int_value = INT_MAX_VALUE[precision - scale - 1];
    int64_t frac_value = scale == 0? 0 : FRAC_MAX_VALUE[scale - 1];
    _value = static_cast<int128_t>(int_value) * DecimalV2Value::ONE_BILLION + frac_value;
    if (is_negtive) _value = -_value;
}

std::size_t hash_value(DecimalV2Value const& value) {
    return value.hash(0);
}

int DecimalV2Value::round(DecimalV2Value *to, int rounding_scale, DecimalRoundMode op) {
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

    static const int values[SCALE] = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000
    };

    int base = values[SCALE - scale];
    if (frac_val % base != 0) return true;
    return false;
}

} // end namespace doris
