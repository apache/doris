// Copyright 2010 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.cc
//
// This file contains string processing functions related to
// numeric values.

#include "gutil/strings/numbers.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h> // for DBL_DIG and FLT_DIG
#include <math.h>  // for HUGE_VAL
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/types.h>
#include <limits>
#include <ostream>

#include "common/exception.h"

using std::numeric_limits;
#include <string>

using std::string;

#include <fmt/compile.h>
#include <fmt/format.h>

#include "common/logging.h"

#include "gutil/integral_types.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strtoint.h"

namespace {

// Represents integer values of digits.
// Uses 36 to indicate an invalid character since we support
// bases up to 36.
static const int8 kAsciiToInt[256] = {
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, // 16 36s.
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  36, 36,
        36, 36, 36, 36, 36, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
        27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 36, 36, 36, 36, 36, 10, 11, 12, 13, 14, 15, 16,
        17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
        36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36};

// Input format based on POSIX.1-2008 strtol
// http://pubs.opengroup.org/onlinepubs/9699919799/functions/strtol.html
template <typename IntType>
bool safe_int_internal(const char* start, const char* end, int base, IntType* value_p) {
    // Consume whitespace.
    while (start < end && ascii_isspace(start[0])) {
        ++start;
    }
    while (start < end && ascii_isspace(end[-1])) {
        --end;
    }
    if (start >= end) {
        return false;
    }

    // Consume sign.
    const bool negative = (start[0] == '-');
    if (negative || start[0] == '+') {
        ++start;
        if (start >= end) {
            return false;
        }
    }

    // Consume base-dependent prefix.
    //  base 0: "0x" -> base 16, "0" -> base 8, default -> base 10
    //  base 16: "0x" -> base 16
    // Also validate the base.
    if (base == 0) {
        if (end - start >= 2 && start[0] == '0' && (start[1] == 'x' || start[1] == 'X')) {
            base = 16;
            start += 2;
        } else if (end - start >= 1 && start[0] == '0') {
            base = 8;
            start += 1;
        } else {
            base = 10;
        }
    } else if (base == 16) {
        if (end - start >= 2 && start[0] == '0' && (start[1] == 'x' || start[1] == 'X')) {
            start += 2;
        }
    } else if (base >= 2 && base <= 36) {
        // okay
    } else {
        return false;
    }

    // Consume digits.
    //
    // The classic loop:
    //
    //   for each digit
    //     value = value * base + digit
    //   value *= sign
    //
    // The classic loop needs overflow checking.  It also fails on the most
    // negative integer, -2147483648 in 32-bit two's complement representation.
    //
    // My improved loop:
    //
    //  if (!negative)
    //    for each digit
    //      value = value * base
    //      value = value + digit
    //  else
    //    for each digit
    //      value = value * base
    //      value = value - digit
    //
    // Overflow checking becomes simple.
    //
    // I present the positive code first for easier reading.
    IntType value = 0;
    if (!negative) {
        const IntType vmax = std::numeric_limits<IntType>::max();
        assert(vmax > 0);
        assert(vmax >= base);
        const IntType vmax_over_base = vmax / base;
        // loop over digits
        // loop body is interleaved for perf, not readability
        for (; start < end; ++start) {
            unsigned char c = static_cast<unsigned char>(start[0]);
            int digit = kAsciiToInt[c];
            if (value > vmax_over_base) return false;
            value *= base;
            if (digit >= base) return false;
            if (value > vmax - digit) return false;
            value += digit;
        }
    } else {
        const IntType vmin = std::numeric_limits<IntType>::min();
        assert(vmin < 0);
        assert(vmin <= 0 - base);
        IntType vmin_over_base = vmin / base;
        // 2003 c++ standard [expr.mul]
        // "... the sign of the remainder is implementation-defined."
        // Although (vmin/base)*base + vmin%base is always vmin.
        // 2011 c++ standard tightens the spec but we cannot rely on it.
        if (vmin % base > 0) {
            vmin_over_base += 1;
        }
        // loop over digits
        // loop body is interleaved for perf, not readability
        for (; start < end; ++start) {
            unsigned char c = static_cast<unsigned char>(start[0]);
            int digit = kAsciiToInt[c];
            if (value < vmin_over_base) return false;
            value *= base;
            if (digit >= base) return false;
            if (value < vmin + digit) return false;
            value -= digit;
        }
    }

    // Store output.
    *value_p = value;
    return true;
}

} // anonymous namespace

bool safe_strto32_base(const char* startptr, const int buffer_size, int32* v, int base) {
    return safe_int_internal<int32>(startptr, startptr + buffer_size, base, v);
}

bool safe_strto64_base(const char* startptr, const int buffer_size, int64* v, int base) {
    return safe_int_internal<int64>(startptr, startptr + buffer_size, base, v);
}

bool safe_strto32(const char* startptr, const int buffer_size, int32* value) {
    return safe_int_internal<int32>(startptr, startptr + buffer_size, 10, value);
}

bool safe_strto64(const char* startptr, const int buffer_size, int64* value) {
    return safe_int_internal<int64>(startptr, startptr + buffer_size, 10, value);
}

bool safe_strto32_base(const char* str, int32* value, int base) {
    char* endptr;
    errno = 0; // errno only gets set on errors
    *value = strto32(str, &endptr, base);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    return *str != '\0' && *endptr == '\0' && errno == 0;
}

bool safe_strto64_base(const char* str, int64* value, int base) {
    char* endptr;
    errno = 0; // errno only gets set on errors
    *value = strto64(str, &endptr, base);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    return *str != '\0' && *endptr == '\0' && errno == 0;
}

bool safe_strtou32_base(const char* str, uint32* value, int base) {
    // strtoul does not give any errors on negative numbers, so we have to
    // search the string for '-' manually.
    while (ascii_isspace(*str)) ++str;
    if (*str == '-') return false;

    char* endptr;
    errno = 0; // errno only gets set on errors
    *value = strtou32(str, &endptr, base);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    return *str != '\0' && *endptr == '\0' && errno == 0;
}

bool safe_strtou64_base(const char* str, uint64* value, int base) {
    // strtou64 does not give any errors on negative numbers, so we have to
    // search the string for '-' manually.
    while (ascii_isspace(*str)) ++str;
    if (*str == '-') return false;

    char* endptr;
    errno = 0; // errno only gets set on errors
    *value = strtou64(str, &endptr, base);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    return *str != '\0' && *endptr == '\0' && errno == 0;
}

// ----------------------------------------------------------------------
// u64tostr_base36()
//    Converts unsigned number to string representation in base-36.
// --------------------------------------------------------------------
size_t u64tostr_base36(uint64 number, size_t buf_size, char* buffer) {
    CHECK_GT(buf_size, 0);
    CHECK(buffer);
    static const char kAlphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";

    buffer[buf_size - 1] = '\0';
    size_t result_size = 1;

    do {
        if (buf_size == result_size) { // Ran out of space.
            return 0;
        }
        int remainder = number % 36;
        number /= 36;
        buffer[buf_size - result_size - 1] = kAlphabet[remainder];
        result_size++;
    } while (number);

    memmove(buffer, buffer + buf_size - result_size, result_size);

    return result_size - 1;
}

// Generate functions that wrap safe_strtoXXX_base.
#define GEN_SAFE_STRTO(name, type)                                                  \
    bool name##_base(const string& str, type* value, int base) {                    \
        return name##_base(str.c_str(), value, base);                               \
    }                                                                               \
    bool name(const char* str, type* value) { return name##_base(str, value, 10); } \
    bool name(const string& str, type* value) { return name##_base(str.c_str(), value, 10); }
GEN_SAFE_STRTO(safe_strto32, int32);
GEN_SAFE_STRTO(safe_strtou32, uint32);
GEN_SAFE_STRTO(safe_strto64, int64);
GEN_SAFE_STRTO(safe_strtou64, uint64);
#undef GEN_SAFE_STRTO

bool safe_strtof(const char* str, float* value) {
    char* endptr;
#ifdef _MSC_VER // has no strtof()
    *value = strtod(str, &endptr);
#else
    *value = strtof(str, &endptr);
#endif
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    // Ignore range errors from strtod/strtof.
    // The values it returns on underflow and
    // overflow are the right fallback in a
    // robust setting.
    return *str != '\0' && *endptr == '\0';
}

bool safe_strtod(const char* str, double* value) {
    char* endptr;
    *value = strtod(str, &endptr);
    if (endptr != str) {
        while (ascii_isspace(*endptr)) ++endptr;
    }
    // Ignore range errors from strtod.  The values it
    // returns on underflow and overflow are the right
    // fallback in a robust setting.
    return *str != '\0' && *endptr == '\0';
}

bool safe_strtof(const string& str, float* value) {
    return safe_strtof(str.c_str(), value);
}

bool safe_strtod(const string& str, double* value) {
    return safe_strtod(str.c_str(), value);
}

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    We want to print the value without losing precision, but we also do
//    not want to print more digits than necessary.  This turns out to be
//    trickier than it sounds.  Numbers like 0.2 cannot be represented
//    exactly in binary.  If we print 0.2 with a very large precision,
//    e.g. "%.50g", we get "0.2000000000000000111022302462515654042363167".
//    On the other hand, if we set the precision too low, we lose
//    significant digits when printing numbers that actually need them.
//    It turns out there is no precision value that does the right thing
//    for all numbers.
//
//    Our strategy is to first try printing with a precision that is never
//    over-precise, then parse the result with strtod() to see if it
//    matches.  If not, we print again with a precision that will always
//    give a precise result, but may use more digits than necessary.
//
//    An arguably better strategy would be to use the algorithm described
//    in "How to Print Floating-Point Numbers Accurately" by Steele &
//    White, e.g. as implemented by David M. Gay's dtoa().  It turns out,
//    however, that the following implementation is about as fast as
//    DMG's code.  Furthermore, DMG's code locks mutexes, which means it
//    will not scale well on multi-core machines.  DMG's code is slightly
//    more accurate (in that it will never use more digits than
//    necessary), but this is probably irrelevant for most users.
//
//    Rob Pike and Ken Thompson also have an implementation of dtoa() in
//    third_party/fmt/fltfmt.cc.  Their implementation is similar to this
//    one in that it makes guesses and then uses strtod() to check them.
//    Their implementation is faster because they use their own code to
//    generate the digits in the first place rather than use snprintf(),
//    thus avoiding format string parsing overhead.  However, this makes
//    it considerably more complicated than the following implementation,
//    and it is embedded in a larger library.  If speed turns out to be
//    an issue, we could re-implement this in terms of their
//    implementation.
// ----------------------------------------------------------------------
int DoubleToBuffer(double value, int width, char* buffer) {
    // DBL_DIG is 15 for IEEE-754 doubles, which are used on almost all
    // platforms these days.  Just in case some system exists where DBL_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(DBL_DIG < 20, DBL_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, width, "%.*g", DBL_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < width);

    if (strtod(buffer, nullptr) != value) {
        snprintf_result = snprintf(buffer, width, "%.*g", DBL_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < width);
    }

    return snprintf_result;
}

int FloatToBuffer(float value, int width, char* buffer) {
    // FLT_DIG is 6 for IEEE-754 floats, which are used on almost all
    // platforms these days.  Just in case some system exists where FLT_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(FLT_DIG < 10, FLT_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, width, "%.*g", FLT_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < width);

    float parsed_value;
    if (!safe_strtof(buffer, &parsed_value) || parsed_value != value) {
        snprintf_result = snprintf(buffer, width, "%.*g", FLT_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < width);
    }

    return snprintf_result;
}

int FastDoubleToBuffer(double value, char* buffer) {
    auto end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    *end = '\0';
    return end - buffer;
}

int FastFloatToBuffer(float value, char* buffer) {
    auto* end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    *end = '\0';
    return end - buffer;
}

// ----------------------------------------------------------------------
// SimpleItoaWithCommas()
//    Description: converts an integer to a string.
//    Puts commas every 3 spaces.
//    Faster than printf("%d")?
//
//    Return value: string
// ----------------------------------------------------------------------

char* SimpleItoaWithCommas(int64_t i, char* buffer, int32_t buffer_size) {
    // 19 digits, 6 commas, and sign are good for 64-bit or smaller ints.
    char* p = buffer + buffer_size;
    // Need to use uint64 instead of int64 to correctly handle
    // -9,223,372,036,854,775,808.
    uint64 n = i;
    if (i < 0) n = 0 - n;
    *--p = '0' + n % 10; // this case deals with the number "0"
    n /= 10;
    while (n) {
        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = ',';
        *--p = '0' + n % 10;
        n /= 10;
        // For this unrolling, we check if n == 0 in the main while loop
    }
    if (i < 0) *--p = '-';
    return p;
}

char* SimpleItoaWithCommas(__int128_t i, char* buffer, int32_t buffer_size) {
    // 39 digits, 12 commas, and sign are good for 128-bit or smaller ints.
    char* p = buffer + buffer_size;
    // Need to use uint128 instead of int128 to correctly handle
    // -170,141,183,460,469,231,731,687,303,715,884,105,728.
    __uint128_t n = i;
    if (i < 0) n = 0 - n;
    *--p = '0' + n % 10; // this case deals with the number "0"
    n /= 10;
    while (n) {
        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) break;

        *--p = ',';
        *--p = '0' + n % 10;
        n /= 10;
        // For this unrolling, we check if n == 0 in the main while loop
    }
    if (i < 0) *--p = '-';
    return p;
}
