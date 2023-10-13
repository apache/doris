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
#include "common/config.h"

using std::numeric_limits;
#include <string>

using std::string;

#include "common/logging.h"
#include <fmt/format.h>

#include "dragonbox/dragonbox_to_chars.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/int128.h"
#include "gutil/integral_types.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strtoint.h"

// Reads a <double> in *text, which may not be whitespace-initiated.
// *len is the length, or -1 if text is '\0'-terminated, which is more
// efficient.  Sets *text to the end of the double, and val to the
// converted value, and the length of the double is subtracted from
// *len. <double> may also be a '?', in which case val will be
// unchanged. Returns true upon success.  If initial_minus is
// non-NULL, then *initial_minus will indicate whether the first
// symbol seen was a '-', which will be ignored. Similarly, if
// final_period is non-NULL, then *final_period will indicate whether
// the last symbol seen was a '.', which will be ignored. This is
// useful in case that an initial '-' or final '.' would have another
// meaning (as a separator, e.g.).
static inline bool EatADouble(const char** text, int* len, bool allow_question, double* val,
                              bool* initial_minus, bool* final_period) {
    const char* pos = *text;
    int rem = *len; // remaining length, or -1 if null-terminated

    if (pos == nullptr || rem == 0) return false;

    if (allow_question && (*pos == '?')) {
        *text = pos + 1;
        if (rem != -1) *len = rem - 1;
        return true;
    }

    if (initial_minus) {
        if ((*initial_minus = (*pos == '-'))) { // Yes, we want assignment.
            if (rem == 1) return false;
            ++pos;
            if (rem != -1) --rem;
        }
    }

    // a double has to begin one of these (we don't allow 'inf' or whitespace)
    // this also serves as an optimization.
    if (!strchr("-+.0123456789", *pos)) return false;

    // strtod is evil in that the second param is a non-const char**
    char* end_nonconst;
    double retval;
    if (rem == -1) {
        retval = strtod(pos, &end_nonconst);
    } else {
        // not '\0'-terminated & no obvious terminator found. must copy.
        gscoped_array<char> buf(new char[rem + 1]);
        memcpy(buf.get(), pos, rem);
        buf[rem] = '\0';
        retval = strtod(buf.get(), &end_nonconst);
        end_nonconst = const_cast<char*>(pos) + (end_nonconst - buf.get());
    }

    if (pos == end_nonconst) return false;

    if (final_period) {
        *final_period = (end_nonconst[-1] == '.');
        if (*final_period) {
            --end_nonconst;
        }
    }

    *text = end_nonconst;
    *val = retval;
    if (rem != -1) *len = rem - (end_nonconst - pos);
    return true;
}

// If update, consume one of acceptable_chars from string *text of
// length len and return that char, or '\0' otherwise. If len is -1,
// *text is null-terminated. If update is false, don't alter *text and
// *len. If null_ok, then update must be false, and, if text has no
// more chars, then return '\1' (arbitrary nonzero).
static inline char EatAChar(const char** text, int* len, const char* acceptable_chars, bool update,
                            bool null_ok) {
    assert(!(update && null_ok));
    if ((*len == 0) || (**text == '\0'))
        return (null_ok ? '\1' : '\0'); // if null_ok, we're in predicate mode.

    if (strchr(acceptable_chars, **text)) {
        char result = **text;
        if (update) {
            ++(*text);
            if (*len != -1) --(*len);
        }
        return result;
    }

    return '\0'; // no match; no update
}

// Parse an expression in 'text' of the form: <comparator><double> or
// <double><sep><double> See full comments in header file.
bool ParseDoubleRange(const char* text, int len, const char** end, double* from, double* to,
                      bool* is_currency, const DoubleRangeOptions& opts) {
    const double from_default = opts.dont_modify_unbounded ? *from : -HUGE_VAL;

    if (!opts.dont_modify_unbounded) {
        *from = -HUGE_VAL;
        *to = HUGE_VAL;
    }
    if (opts.allow_currency && (is_currency != nullptr)) *is_currency = false;

    assert(len >= -1);
    assert(opts.separators && (*opts.separators != '\0'));
    // these aren't valid separators
    assert(strlen(opts.separators) == strcspn(opts.separators, "+0123456789eE$"));
    assert(opts.num_required_bounds <= 2);

    // Handle easier cases of comparators (<, >) first
    if (opts.allow_comparators) {
        char comparator = EatAChar(&text, &len, "<>", true, false);
        if (comparator) {
            double* dest = (comparator == '>') ? from : to;
            EatAChar(&text, &len, "=", true, false);
            if (opts.allow_currency && EatAChar(&text, &len, "$", true, false))
                if (is_currency != nullptr) *is_currency = true;
            if (!EatADouble(&text, &len, opts.allow_unbounded_markers, dest, nullptr, nullptr))
                return false;
            *end = text;
            return EatAChar(&text, &len, opts.acceptable_terminators, false,
                            opts.null_terminator_ok);
        }
    }

    bool seen_dollar = (opts.allow_currency && EatAChar(&text, &len, "$", true, false));

    // If we see a '-', two things could be happening: -<to> or
    // <from>... where <from> is negative. Treat initial minus sign as a
    // separator if '-' is a valid separator.
    // Similarly, we prepare for the possibility of seeing a '.' at the
    // end of the number, in case '.' (which really means '..') is a
    // separator.
    bool initial_minus_sign = false;
    bool final_period = false;
    bool* check_initial_minus =
            (strchr(opts.separators, '-') && !seen_dollar && (opts.num_required_bounds < 2))
                    ? (&initial_minus_sign)
                    : nullptr;
    bool* check_final_period = strchr(opts.separators, '.') ? (&final_period) : nullptr;
    bool double_seen = EatADouble(&text, &len, opts.allow_unbounded_markers, from,
                                  check_initial_minus, check_final_period);

    // if 2 bounds required, must see a double (or '?' if allowed)
    if ((opts.num_required_bounds == 2) && !double_seen) return false;

    if (seen_dollar && !double_seen) {
        --text;
        if (len != -1) ++len;
        seen_dollar = false;
    }
    // If we're here, we've read the first double and now expect a
    // separator and another <double>.
    char separator = EatAChar(&text, &len, opts.separators, true, false);
    if (separator == '.') {
        // seen one '.' as separator; must check for another; perhaps set seplen=2
        if (EatAChar(&text, &len, ".", true, false)) {
            if (final_period) {
                // We may have three periods in a row. The first is part of the
                // first number, the others are a separator. Policy: 234...567
                // is "234." to "567", not "234" to ".567".
                EatAChar(&text, &len, ".", true, false);
            }
        } else if (!EatAChar(&text, &len, opts.separators, true, false)) {
            // just one '.' and no other separator; uneat the first '.' we saw
            --text;
            if (len != -1) ++len;
            separator = '\0';
        }
    }
    // By now, we've consumed whatever separator there may have been,
    // and separator is true iff there was one.
    if (!separator) {
        if (final_period) // final period now considered part of first double
            EatAChar(&text, &len, ".", true, false);
        if (initial_minus_sign && double_seen) {
            *to = *from;
            *from = from_default;
        } else if (opts.require_separator || (opts.num_required_bounds > 0 && !double_seen) ||
                   (opts.num_required_bounds > 1)) {
            return false;
        }
    } else {
        if (initial_minus_sign && double_seen) *from = -(*from);
        // read second <double>
        bool second_dollar_seen = (seen_dollar || (opts.allow_currency && !double_seen)) &&
                                  EatAChar(&text, &len, "$", true, false);
        bool second_double_seen =
                EatADouble(&text, &len, opts.allow_unbounded_markers, to, nullptr, nullptr);
        if (opts.num_required_bounds > double_seen + second_double_seen) return false;
        if (second_dollar_seen && !second_double_seen) {
            --text;
            if (len != -1) ++len;
            second_dollar_seen = false;
        }
        seen_dollar = seen_dollar || second_dollar_seen;
    }

    if (seen_dollar && (is_currency != nullptr)) *is_currency = true;
    // We're done. But we have to check that the next char is a proper
    // terminator.
    *end = text;
    char terminator =
            EatAChar(&text, &len, opts.acceptable_terminators, false, opts.null_terminator_ok);
    if (terminator == '.') --(*end);
    return terminator;
}

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

// Default arguments
string Uint128ToHexString(uint128 ui128) {
    char buf[33];
    snprintf(buf, sizeof(buf), "%016" PRIx64, Uint128High64(ui128));
    snprintf(buf + 16, sizeof(buf) - 16, "%016" PRIx64, Uint128Low64(ui128));
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
            LOG(FATAL) << "Invalid mnemonic: `" << c << "';"
                       << " should be one of `K', `M', `G', and `T'.";
        }
    }
    return n * scale;
}

// ----------------------------------------------------------------------
// FastIntToBuffer()
// FastInt64ToBuffer()
// FastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// FastTimeToBuffer()
//    These are intended for speed.  FastHexToBuffer() assumes the
//    integer is non-negative.  FastHexToBuffer() puts output in
//    hex rather than decimal.  FastTimeToBuffer() puts the output
//    into RFC822 format.  If time is 0, uses the current time.
//
//    FastHex64ToBuffer() puts a 64-bit unsigned value in hex-format,
//    padded to exactly 16 bytes (plus one byte for '\0')
//
//    FastHex32ToBuffer() puts a 32-bit unsigned value in hex-format,
//    padded to exactly 8 bytes (plus one byte for '\0')
//
//       All functions take the output buffer as an arg.  FastInt()
//    uses at most 22 bytes, FastTime() uses exactly 30 bytes.
//    They all return a pointer to the beginning of the output,
//    which may not be the beginning of the input buffer.  (Though
//    for FastTimeToBuffer(), we guarantee that it is.)
// ----------------------------------------------------------------------

char* FastInt64ToBuffer(int64 i, char* buffer) {
    FastInt64ToBufferLeft(i, buffer);
    return buffer;
}

char* FastInt32ToBuffer(int32 i, char* buffer) {
    FastInt32ToBufferLeft(i, buffer);
    return buffer;
}

char* FastHexToBuffer(int i, char* buffer) {
    CHECK_GE(i, 0) << "FastHexToBuffer() wants non-negative integers, not " << i;

    static const char* hexdigits = "0123456789abcdef";
    char* p = buffer + 21;
    *p-- = '\0';
    do {
        *p-- = hexdigits[i & 15]; // mod by 16
        i >>= 4;                  // divide by 16
    } while (i > 0);
    return p + 1;
}

char* InternalFastHexToBuffer(uint64 value, char* buffer, int num_byte) {
    static const char* hexdigits = "0123456789abcdef";
    buffer[num_byte] = '\0';
    for (int i = num_byte - 1; i >= 0; i--) {
        buffer[i] = hexdigits[value & 0xf];
        value >>= 4;
    }
    return buffer;
}

char* FastHex64ToBuffer(uint64 value, char* buffer) {
    return InternalFastHexToBuffer(value, buffer, 16);
}

char* FastHex32ToBuffer(uint32 value, char* buffer) {
    return InternalFastHexToBuffer(value, buffer, 8);
}

// TODO(user): revisit the two_ASCII_digits optimization.
//
// Several converters use this table to reduce
// division and modulo operations.
extern const char two_ASCII_digits[100][2]; // from strutil.cc

// ----------------------------------------------------------------------
// FastInt32ToBufferLeft()
// FastUInt32ToBufferLeft()
// FastInt64ToBufferLeft()
// FastUInt64ToBufferLeft()
//
// Like the Fast*ToBuffer() functions above, these are intended for speed.
// Unlike the Fast*ToBuffer() functions, however, these functions write
// their output to the beginning of the buffer (hence the name, as the
// output is left-aligned).  The caller is responsible for ensuring that
// the buffer has enough space to hold the output.
//
// Returns a pointer to the end of the string (i.e. the null character
// terminating the string).
// ----------------------------------------------------------------------

char* FastUInt32ToBufferLeft(uint32 u, char* buffer) {
    uint digits;
    const char* ASCII_digits = nullptr;
    // The idea of this implementation is to trim the number of divides to as few
    // as possible by using multiplication and subtraction rather than mod (%),
    // and by outputting two digits at a time rather than one.
    // The huge-number case is first, in the hopes that the compiler will output
    // that case in one branch-free block of code, and only output conditional
    // branches into it from below.
    if (u >= 1000000000) {      // >= 1,000,000,000
        digits = u / 100000000; // 100,000,000
        ASCII_digits = two_ASCII_digits[digits];
        buffer[0] = ASCII_digits[0];
        buffer[1] = ASCII_digits[1];
        buffer += 2;
    sublt100_000_000:
        u -= digits * 100000000; // 100,000,000
    lt100_000_000:
        digits = u / 1000000; // 1,000,000
        ASCII_digits = two_ASCII_digits[digits];
        buffer[0] = ASCII_digits[0];
        buffer[1] = ASCII_digits[1];
        buffer += 2;
    sublt1_000_000:
        u -= digits * 1000000; // 1,000,000
    lt1_000_000:
        digits = u / 10000; // 10,000
        ASCII_digits = two_ASCII_digits[digits];
        buffer[0] = ASCII_digits[0];
        buffer[1] = ASCII_digits[1];
        buffer += 2;
    sublt10_000:
        u -= digits * 10000; // 10,000
    lt10_000:
        digits = u / 100;
        ASCII_digits = two_ASCII_digits[digits];
        buffer[0] = ASCII_digits[0];
        buffer[1] = ASCII_digits[1];
        buffer += 2;
    sublt100:
        u -= digits * 100;
    lt100:
        digits = u;
        ASCII_digits = two_ASCII_digits[digits];
        buffer[0] = ASCII_digits[0];
        buffer[1] = ASCII_digits[1];
        buffer += 2;
    done:
        *buffer = 0;
        return buffer;
    }

    if (u < 100) {
        digits = u;
        if (u >= 10) goto lt100;
        *buffer++ = '0' + digits;
        goto done;
    }
    if (u < 10000) { // 10,000
        if (u >= 1000) goto lt10_000;
        digits = u / 100;
        *buffer++ = '0' + digits;
        goto sublt100;
    }
    if (u < 1000000) { // 1,000,000
        if (u >= 100000) goto lt1_000_000;
        digits = u / 10000; //    10,000
        *buffer++ = '0' + digits;
        goto sublt10_000;
    }
    if (u < 100000000) { // 100,000,000
        if (u >= 10000000) goto lt100_000_000;
        digits = u / 1000000; //   1,000,000
        *buffer++ = '0' + digits;
        goto sublt1_000_000;
    }
    // we already know that u < 1,000,000,000
    digits = u / 100000000; // 100,000,000
    *buffer++ = '0' + digits;
    goto sublt100_000_000;
}

char* FastInt32ToBufferLeft(int32 i, char* buffer) {
    uint32 u = i;
    if (i < 0) {
        *buffer++ = '-';
        // We need to do the negation in modular (i.e., "unsigned")
        // arithmetic; MSVC++ apprently warns for plain "-u", so
        // we write the equivalent expression "0 - u" instead.
        u = 0 - u;
    }
    return FastUInt32ToBufferLeft(u, buffer);
}

char* FastUInt64ToBufferLeft(uint64 u64, char* buffer) {
    uint digits;
    const char* ASCII_digits = nullptr;

    uint32 u = static_cast<uint32>(u64);
    if (u == u64) return FastUInt32ToBufferLeft(u, buffer);

    uint64 top_11_digits = u64 / 1000000000;
    buffer = FastUInt64ToBufferLeft(top_11_digits, buffer);
    u = u64 - (top_11_digits * 1000000000);

    digits = u / 10000000; // 10,000,000
    DCHECK_LT(digits, 100);
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
    u -= digits * 10000000; // 10,000,000
    digits = u / 100000;    // 100,000
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
    u -= digits * 100000; // 100,000
    digits = u / 1000;    // 1,000
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
    u -= digits * 1000; // 1,000
    digits = u / 10;
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
    u -= digits * 10;
    digits = u;
    *buffer++ = '0' + digits;
    *buffer = 0;
    return buffer;
}

char* FastInt64ToBufferLeft(int64 i, char* buffer) {
    uint64 u = i;
    if (i < 0) {
        *buffer++ = '-';
        u = 0 - u;
    }
    return FastUInt64ToBufferLeft(u, buffer);
}

int HexDigitsPrefix(const char* buf, int num_digits) {
    for (int i = 0; i < num_digits; i++)
        if (!ascii_isxdigit(buf[i]))
            return 0; // This also detects end of string as '\0' is not xdigit.
    return 1;
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

int FastDoubleToBuffer(double value, char* buffer, bool faster_float_convert) {
    if (faster_float_convert) {
        return jkj::dragonbox::to_chars_n(value, buffer) - buffer;
    }
 
    auto end = fmt::format_to(buffer, "{:.15g}", value);
    *end = '\0';
    if (strtod(buffer, nullptr) != value) {
        end = fmt::format_to(buffer, "{:.17g}", value);
    }
    return end - buffer;
}

int FastFloatToBuffer(float value, char* buffer, bool faster_float_convert) {
    if (faster_float_convert) {
        return jkj::dragonbox::to_chars_n(value, buffer) - buffer;
    }

    auto end = fmt::format_to(buffer, "{:.6g}", value);
    *end = '\0';
#ifdef _MSC_VER // has no strtof()
    if (strtod(buffer, nullptr) != value) {
#else
    if (strtof(buffer, nullptr) != value) {
#endif
        end = fmt::format_to(buffer, "{:.8g}", value);
    }
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
