// Copyright 2010 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.cc
//
// This file contains string processing functions related to
// numeric values.

#include "gutil/strings/numbers.h"

#include <fmt/compile.h>
#include <fmt/format.h>

#include <cfloat>

#include "absl/strings/ascii.h"
#include "butil/macros.h"
#include "common/logging.h"

bool safe_strtof(const char* str, float* value) {
    char* endptr;
#ifdef _MSC_VER // has no strtof()
    *value = strtod(str, &endptr);
#else
    *value = strtof(str, &endptr);
#endif
    if (endptr != str) {
        while (absl::ascii_isspace(*endptr)) ++endptr;
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
        while (absl::ascii_isspace(*endptr)) ++endptr;
    }
    // Ignore range errors from strtod.  The values it
    // returns on underflow and overflow are the right
    // fallback in a robust setting.
    return *str != '\0' && *endptr == '\0';
}

bool safe_strtof(const std::string& str, float* value) {
    return safe_strtof(str.c_str(), value);
}

bool safe_strtod(const std::string& str, double* value) {
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

// refer to: https://en.cppreference.com/w/cpp/types/numeric_limits/max_digits10.html
int FastDoubleToBuffer(double value, char* buffer) {
    char* end = nullptr;
    // output NaN and Infinity to be compatible with most of the implementations
    if (std::isnan(value)) {
        static constexpr char nan_str[] = "NaN";
        static constexpr int nan_str_len = sizeof(nan_str) - 1;
        memcpy(buffer, nan_str, nan_str_len);
        end = buffer + nan_str_len;
    } else if (std::isinf(value)) {
        static constexpr char inf_str[] = "Infinity";
        static constexpr int inf_str_len = sizeof(inf_str) - 1;
        static constexpr char neg_inf_str[] = "-Infinity";
        static constexpr int neg_inf_str_len = sizeof(neg_inf_str) - 1;
        if (value > 0) {
            memcpy(buffer, inf_str, inf_str_len);
            end = buffer + inf_str_len;
        } else {
            memcpy(buffer, neg_inf_str, neg_inf_str_len);
            end = buffer + neg_inf_str_len;
        }
    } else {
        end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    }
    *end = '\0';
    return end - buffer;
}

int FastFloatToBuffer(float value, char* buffer) {
    char* end = nullptr;
    // output NaN and Infinity to be compatible with most of the implementations
    if (std::isnan(value)) {
        static constexpr char nan_str[] = "NaN";
        static constexpr int nan_str_len = sizeof(nan_str) - 1;
        memcpy(buffer, nan_str, nan_str_len);
        end = buffer + nan_str_len;
    } else if (std::isinf(value)) {
        static constexpr char inf_str[] = "Infinity";
        static constexpr int inf_str_len = sizeof(inf_str) - 1;
        static constexpr char neg_inf_str[] = "-Infinity";
        static constexpr int neg_inf_str_len = sizeof(neg_inf_str) - 1;
        if (value > 0) {
            memcpy(buffer, inf_str, inf_str_len);
            end = buffer + inf_str_len;
        } else {
            memcpy(buffer, neg_inf_str, neg_inf_str_len);
            end = buffer + neg_inf_str_len;
        }
    } else {
        end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    }
    *end = '\0';
    return end - buffer;
}
