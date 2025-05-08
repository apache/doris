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
#include "gutil/stringprintf.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strtoint.h"

// ----------------------------------------------------------------------
// ConsumeStrayLeadingZeroes
//    Eliminates all leading zeroes (unless the string itself is composed
//    of nothing but zeroes, in which case one is kept: 0...0 becomes 0).
// --------------------------------------------------------------------

void ConsumeStrayLeadingZeroes(string* const str) {
    const string::size_type len(str->size());
    if (len > 1 && (*str)[0] == '0') {
        const char *const begin(str->c_str()), *const end(begin + len), *ptr(begin + 1);
        while (ptr != end && *ptr == '0') {
            ++ptr;
        }
        string::size_type remove(ptr - begin);
        DCHECK_GT(ptr, begin);
        if (remove == len) {
            --remove; // if they are all zero, leave one...
        }
        str->erase(0, remove);
    }
}

// ----------------------------------------------------------------------
// ParseLeadingInt32Value()
// ParseLeadingUInt32Value()
//    A simple parser for [u]int32 values. Returns the parsed value
//    if a valid value is found; else returns deflt
//    This cannot handle decimal numbers with leading 0s.
// --------------------------------------------------------------------

int32 ParseLeadingInt32Value(const char* str, int32 deflt) {
    char* error = nullptr;
    long value = strtol(str, &error, 0);
    // Limit long values to int32 min/max.  Needed for lp64; no-op on 32 bits.
    if (value > numeric_limits<int32>::max()) {
        value = numeric_limits<int32>::max();
    } else if (value < numeric_limits<int32>::min()) {
        value = numeric_limits<int32>::min();
    }
    return (error == str) ? deflt : value;
}

uint32 ParseLeadingUInt32Value(const char* str, uint32 deflt) {
    if (numeric_limits<unsigned long>::max() == numeric_limits<uint32>::max()) {
        // When long is 32 bits, we can use strtoul.
        char* error = nullptr;
        const uint32 value = strtoul(str, &error, 0);
        return (error == str) ? deflt : value;
    } else {
        // When long is 64 bits, we must use strto64 and handle limits
        // by hand.  The reason we cannot use a 64-bit strtoul is that
        // it would be impossible to differentiate "-2" (that should wrap
        // around to the value UINT_MAX-1) from a string with ULONG_MAX-1
        // (that should be pegged to UINT_MAX due to overflow).
        char* error = nullptr;
        int64 value = strto64(str, &error, 0);
        if (value > numeric_limits<uint32>::max() ||
            value < -static_cast<int64>(numeric_limits<uint32>::max())) {
            value = numeric_limits<uint32>::max();
        }
        // Within these limits, truncation to 32 bits handles negatives correctly.
        return (error == str) ? deflt : value;
    }
}

// ----------------------------------------------------------------------
// ParseLeadingDec32Value
// ParseLeadingUDec32Value
//    A simple parser for [u]int32 values. Returns the parsed value
//    if a valid value is found; else returns deflt
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
// --------------------------------------------------------------------

int32 ParseLeadingDec32Value(const char* str, int32 deflt) {
    char* error = nullptr;
    long value = strtol(str, &error, 10);
    // Limit long values to int32 min/max.  Needed for lp64; no-op on 32 bits.
    if (value > numeric_limits<int32>::max()) {
        value = numeric_limits<int32>::max();
    } else if (value < numeric_limits<int32>::min()) {
        value = numeric_limits<int32>::min();
    }
    return (error == str) ? deflt : value;
}

uint32 ParseLeadingUDec32Value(const char* str, uint32 deflt) {
    if (numeric_limits<unsigned long>::max() == numeric_limits<uint32>::max()) {
        // When long is 32 bits, we can use strtoul.
        char* error = nullptr;
        const uint32 value = strtoul(str, &error, 10);
        return (error == str) ? deflt : value;
    } else {
        // When long is 64 bits, we must use strto64 and handle limits
        // by hand.  The reason we cannot use a 64-bit strtoul is that
        // it would be impossible to differentiate "-2" (that should wrap
        // around to the value UINT_MAX-1) from a string with ULONG_MAX-1
        // (that should be pegged to UINT_MAX due to overflow).
        char* error = nullptr;
        int64 value = strto64(str, &error, 10);
        if (value > numeric_limits<uint32>::max() ||
            value < -static_cast<int64>(numeric_limits<uint32>::max())) {
            value = numeric_limits<uint32>::max();
        }
        // Within these limits, truncation to 32 bits handles negatives correctly.
        return (error == str) ? deflt : value;
    }
}

// ----------------------------------------------------------------------
// ParseLeadingUInt64Value
// ParseLeadingInt64Value
// ParseLeadingHex64Value
//    A simple parser for 64-bit values. Returns the parsed value if a
//    valid integer is found; else returns deflt
//    UInt64 and Int64 cannot handle decimal numbers with leading 0s.
// --------------------------------------------------------------------
uint64 ParseLeadingUInt64Value(const char* str, uint64 deflt) {
    char* error = nullptr;
    const uint64 value = strtou64(str, &error, 0);
    return (error == str) ? deflt : value;
}

int64 ParseLeadingInt64Value(const char* str, int64 deflt) {
    char* error = nullptr;
    const int64 value = strto64(str, &error, 0);
    return (error == str) ? deflt : value;
}

uint64 ParseLeadingHex64Value(const char* str, uint64 deflt) {
    char* error = nullptr;
    const uint64 value = strtou64(str, &error, 16);
    return (error == str) ? deflt : value;
}

// ----------------------------------------------------------------------
// ParseLeadingDec64Value
// ParseLeadingUDec64Value
//    A simple parser for [u]int64 values. Returns the parsed value
//    if a valid value is found; else returns deflt
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
// --------------------------------------------------------------------

int64 ParseLeadingDec64Value(const char* str, int64 deflt) {
    char* error = nullptr;
    const int64 value = strto64(str, &error, 10);
    return (error == str) ? deflt : value;
}

uint64 ParseLeadingUDec64Value(const char* str, uint64 deflt) {
    char* error = nullptr;
    const uint64 value = strtou64(str, &error, 10);
    return (error == str) ? deflt : value;
}

// ----------------------------------------------------------------------
// ParseLeadingDoubleValue()
//    A simple parser for double values. Returns the parsed value
//    if a valid value is found; else returns deflt
// --------------------------------------------------------------------

double ParseLeadingDoubleValue(const char* str, double deflt) {
    char* error = nullptr;
    errno = 0;
    const double value = strtod(str, &error);
    if (errno != 0 ||   // overflow/underflow happened
        error == str) { // no valid parse
        return deflt;
    } else {
        return value;
    }
}

// ----------------------------------------------------------------------
// ParseLeadingBoolValue()
//    A recognizer of boolean string values. Returns the parsed value
//    if a valid value is found; else returns deflt.  This skips leading
//    whitespace, is case insensitive, and recognizes these forms:
//    0/1, false/true, no/yes, n/y
// --------------------------------------------------------------------
bool ParseLeadingBoolValue(const char* str, bool deflt) {
    static const int kMaxLen = 5;
    char value[kMaxLen + 1];
    // Skip whitespace
    while (ascii_isspace(*str)) {
        ++str;
    }
    int len = 0;
    for (; len <= kMaxLen && ascii_isalnum(*str); ++str) value[len++] = ascii_tolower(*str);
    if (len == 0 || len > kMaxLen) return deflt;
    value[len] = '\0';
    switch (len) {
    case 1:
        if (value[0] == '0' || value[0] == 'n') return false;
        if (value[0] == '1' || value[0] == 'y') return true;
        break;
    case 2:
        if (!strcmp(value, "no")) return false;
        break;
    case 3:
        if (!strcmp(value, "yes")) return true;
        break;
    case 4:
        if (!strcmp(value, "true")) return true;
        break;
    case 5:
        if (!strcmp(value, "false")) return false;
        break;
    }
    return deflt;
}

// ----------------------------------------------------------------------
// Uint64ToString()
// FloatToString()
// IntToString()
//    Convert various types to their string representation, possibly padded
//    with spaces, using snprintf format specifiers.
// ----------------------------------------------------------------------

string Uint64ToString(uint64 fp) {
    char buf[17];
    snprintf(buf, sizeof(buf), "%016" PRIx64, fp);
    return string(buf);
}
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

uint64 atoi_kmgt(const char* s) {
    char* endptr;
    uint64 n = strtou64(s, &endptr, 10);
    uint64 scale = 1;
    char c = *endptr;
    if (c != '\0') {
        c = ascii_toupper(c);
        switch (c) {
        case 'K':
            scale = GG_ULONGLONG(1) << 10;
            break;
        case 'M':
            scale = GG_ULONGLONG(1) << 20;
            break;
        case 'G':
            scale = GG_ULONGLONG(1) << 30;
            break;
        case 'T':
            scale = GG_ULONGLONG(1) << 40;
            break;
        default:
            throw doris::Exception(doris::Status::FatalError(
                    "Invalid mnemonic: `{}'; should be one of `K', `M', `G', and `T'.", c));
        }
    }
    return n * scale;
}

// ----------------------------------------------------------------------
// AutoDigitStrCmp
// AutoDigitLessThan
// StrictAutoDigitLessThan
// autodigit_less
// autodigit_greater
// strict_autodigit_less
// strict_autodigit_greater
//    These are like less<string> and greater<string>, except when a
//    run of digits is encountered at corresponding points in the two
//    arguments.  Such digit strings are compared numerically instead
//    of lexicographically.  Therefore if you sort by
//    "autodigit_less", some machine names might get sorted as:
//        exaf1
//        exaf2
//        exaf10
//    When using "strict" comparison (AutoDigitStrCmp with the strict flag
//    set to true, or the strict version of the other functions),
//    strings that represent equal numbers will not be considered equal if
//    the string representations are not identical.  That is, "01" < "1" in
//    strict mode, but "01" == "1" otherwise.
// ----------------------------------------------------------------------

int AutoDigitStrCmp(const char* a, int alen, const char* b, int blen, bool strict) {
    int aindex = 0;
    int bindex = 0;
    while ((aindex < alen) && (bindex < blen)) {
        if (isdigit(a[aindex]) && isdigit(b[bindex])) {
            // Compare runs of digits.  Instead of extracting numbers, we
            // just skip leading zeroes, and then get the run-lengths.  This
            // allows us to handle arbitrary precision numbers.  We remember
            // how many zeroes we found so that we can differentiate between
            // "1" and "01" in strict mode.

            // Skip leading zeroes, but remember how many we found
            int azeroes = aindex;
            int bzeroes = bindex;
            while ((aindex < alen) && (a[aindex] == '0')) aindex++;
            while ((bindex < blen) && (b[bindex] == '0')) bindex++;
            azeroes = aindex - azeroes;
            bzeroes = bindex - bzeroes;

            // Count digit lengths
            int astart = aindex;
            int bstart = bindex;
            while ((aindex < alen) && isdigit(a[aindex])) aindex++;
            while ((bindex < blen) && isdigit(b[bindex])) bindex++;
            if (aindex - astart < bindex - bstart) {
                // a has shorter run of digits: so smaller
                return -1;
            } else if (aindex - astart > bindex - bstart) {
                // a has longer run of digits: so larger
                return 1;
            } else {
                // Same lengths, so compare digit by digit
                for (int i = 0; i < aindex - astart; i++) {
                    if (a[astart + i] < b[bstart + i]) {
                        return -1;
                    } else if (a[astart + i] > b[bstart + i]) {
                        return 1;
                    }
                }
                // Equal: did one have more leading zeroes?
                if (strict && azeroes != bzeroes) {
                    if (azeroes > bzeroes) {
                        // a has more leading zeroes: a < b
                        return -1;
                    } else {
                        // b has more leading zeroes: a > b
                        return 1;
                    }
                }
                // Equal: so continue scanning
            }
        } else if (a[aindex] < b[bindex]) {
            return -1;
        } else if (a[aindex] > b[bindex]) {
            return 1;
        } else {
            aindex++;
            bindex++;
        }
    }

    if (aindex < alen) {
        // b is prefix of a
        return 1;
    } else if (bindex < blen) {
        // a is prefix of b
        return -1;
    } else {
        // a is equal to b
        return 0;
    }
}

bool AutoDigitLessThan(const char* a, int alen, const char* b, int blen) {
    return AutoDigitStrCmp(a, alen, b, blen, false) < 0;
}

bool StrictAutoDigitLessThan(const char* a, int alen, const char* b, int blen) {
    return AutoDigitStrCmp(a, alen, b, blen, true) < 0;
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

string SimpleDtoa(double value) {
    char buffer[kDoubleToBufferSize];
    return DoubleToBuffer(value, buffer);
}

string SimpleFtoa(float value) {
    char buffer[kFloatToBufferSize];
    return FloatToBuffer(value, buffer);
}

char* DoubleToBuffer(double value, char* buffer) {
    // DBL_DIG is 15 for IEEE-754 doubles, which are used on almost all
    // platforms these days.  Just in case some system exists where DBL_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(DBL_DIG < 20, DBL_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, kDoubleToBufferSize, "%.*g", DBL_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < kDoubleToBufferSize);

    if (strtod(buffer, nullptr) != value) {
        snprintf_result = snprintf(buffer, kDoubleToBufferSize, "%.*g", DBL_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < kDoubleToBufferSize);
    }
    return buffer;
}

char* FloatToBuffer(float value, char* buffer) {
    // FLT_DIG is 6 for IEEE-754 floats, which are used on almost all
    // platforms these days.  Just in case some system exists where FLT_DIG
    // is significantly larger -- and risks overflowing our buffer -- we have
    // this assert.
    COMPILE_ASSERT(FLT_DIG < 10, FLT_DIG_is_too_big);

    int snprintf_result = snprintf(buffer, kFloatToBufferSize, "%.*g", FLT_DIG, value);

    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < kFloatToBufferSize);

    float parsed_value;
    if (!safe_strtof(buffer, &parsed_value) || parsed_value != value) {
        snprintf_result = snprintf(buffer, kFloatToBufferSize, "%.*g", FLT_DIG + 2, value);

        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < kFloatToBufferSize);
    }
    return buffer;
}

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
string SimpleItoaWithCommas(int32 i) {
    // 10 digits, 3 commas, and sign are good for 32-bit or smaller ints.
    // Longest is -2,147,483,648.
    char local[14];
    char* p = local + sizeof(local);
    // Need to use uint32 instead of int32 to correctly handle
    // -2,147,483,648.
    uint32 n = i;
    if (i < 0) n = 0 - n; // negate the unsigned value to avoid overflow
    *--p = '0' + n % 10;  // this case deals with the number "0"
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
    return string(p, local + sizeof(local));
}

// We need this overload because otherwise SimpleItoaWithCommas(5U) wouldn't
// compile.
string SimpleItoaWithCommas(uint32 i) {
    // 10 digits and 3 commas are good for 32-bit or smaller ints.
    // Longest is 4,294,967,295.
    char local[13];
    char* p = local + sizeof(local);
    *--p = '0' + i % 10; // this case deals with the number "0"
    i /= 10;
    while (i) {
        *--p = '0' + i % 10;
        i /= 10;
        if (i == 0) break;

        *--p = '0' + i % 10;
        i /= 10;
        if (i == 0) break;

        *--p = ',';
        *--p = '0' + i % 10;
        i /= 10;
        // For this unrolling, we check if i == 0 in the main while loop
    }
    return string(p, local + sizeof(local));
}

string SimpleItoaWithCommas(int64 i) {
    // 19 digits, 6 commas, and sign are good for 64-bit or smaller ints.
    char local[26];
    char* p = SimpleItoaWithCommas(i, local, sizeof(local));
    return string(p, local + sizeof(local));
}

// We need this overload because otherwise SimpleItoaWithCommas(5ULL) wouldn't
// compile.
string SimpleItoaWithCommas(uint64 i) {
    // 20 digits and 6 commas are good for 64-bit or smaller ints.
    // Longest is 18,446,744,073,709,551,615.
    char local[26];
    char* p = local + sizeof(local);
    *--p = '0' + i % 10; // this case deals with the number "0"
    i /= 10;
    while (i) {
        *--p = '0' + i % 10;
        i /= 10;
        if (i == 0) break;

        *--p = '0' + i % 10;
        i /= 10;
        if (i == 0) break;

        *--p = ',';
        *--p = '0' + i % 10;
        i /= 10;
        // For this unrolling, we check if i == 0 in the main while loop
    }
    return string(p, local + sizeof(local));
}

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

// ----------------------------------------------------------------------
// ItoaKMGT()
//    Description: converts an integer to a string
//    Truncates values to a readable unit: K, G, M or T
//    Opposite of atoi_kmgt()
//    e.g. 100 -> "100" 1500 -> "1500"  4000 -> "3K"   57185920 -> "45M"
//
//    Return value: string
// ----------------------------------------------------------------------
string ItoaKMGT(int64 i) {
    const char *sign = "", *suffix = "";
    if (i < 0) {
        // We lose some accuracy if the caller passes LONG_LONG_MIN, but
        // that's OK as this function is only for human readability
        if (i == numeric_limits<int64>::min()) i++;
        sign = "-";
        i = -i;
    }

    int64 val;

    if ((val = (i >> 40)) > 1) {
        suffix = "T";
    } else if ((val = (i >> 30)) > 1) {
        suffix = "G";
    } else if ((val = (i >> 20)) > 1) {
        suffix = "M";
    } else if ((val = (i >> 10)) > 1) {
        suffix = "K";
    } else {
        val = i;
    }

    return StringPrintf("%s%" PRId64 "%s", sign, val, suffix);
}

string AccurateItoaKMGT(int64 i) {
    const char* sign = "";
    if (i < 0) {
        // We lose some accuracy if the caller passes LONG_LONG_MIN, but
        // that's OK as this function is only for human readability
        if (i == numeric_limits<int64>::min()) i++;
        sign = "-";
        i = -i;
    }

    string ret = StringPrintf("%s", sign);
    int64 val;
    if ((val = (i >> 40)) > 1) {
        ret += StringPrintf("%" PRId64
                            "%s"
                            ",",
                            val, "T");
        i = i - (val << 40);
    }
    if ((val = (i >> 30)) > 1) {
        ret += StringPrintf("%" PRId64
                            "%s"
                            ",",
                            val, "G");
        i = i - (val << 30);
    }
    if ((val = (i >> 20)) > 1) {
        ret += StringPrintf("%" PRId64
                            "%s"
                            ",",
                            val, "M");
        i = i - (val << 20);
    }
    if ((val = (i >> 10)) > 1) {
        ret += StringPrintf("%" PRId64 "%s", val, "K");
        i = i - (val << 10);
    } else {
        ret += StringPrintf("%" PRId64 "%s", i, "K");
    }

    return ret;
}

// DEPRECATED(wadetregaskis).
// These are non-inline because some BUILD files turn on -Wformat-non-literal.

string FloatToString(float f, const char* format) {
    return StringPrintf(format, f);
}

string IntToString(int i, const char* format) {
    return StringPrintf(format, i);
}

string Int64ToString(int64 i64, const char* format) {
    return StringPrintf(format, i64);
}

string UInt64ToString(uint64 ui64, const char* format) {
    return StringPrintf(format, ui64);
}
