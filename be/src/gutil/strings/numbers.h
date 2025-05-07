// Copyright 2010 Google Inc. All Rights Reserved.
// Maintainer: mec@google.com (Michael Chastain)
//
// Convert strings to numbers or numbers to strings.

#pragma once

#include <stddef.h>
#include <time.h>
#include <stdint.h>
#include <functional>

using std::less;
#include <limits>

using std::numeric_limits;
#include <string>

using std::string;
#include <vector>

using std::vector;

#include "gutil/integral_types.h"
// IWYU pragma: no_include <butil/macros.h>
#include "gutil/macros.h" // IWYU pragma: keep
#include "gutil/stringprintf.h"

// START DOXYGEN NumbersFunctions grouping
/* @defgroup NumbersFunctions
 * @{ */

// Convert a fingerprint to 16 hex digits.
string Uint64ToString(uint64 fp);

// Convert strings to numeric values, with strict error checking.
// Leading and trailing spaces are allowed.
// Negative inputs are not allowed for unsigned ints (unlike strtoul).
// Numbers must be in base 10; see the _base variants below for other bases.
// Returns false on errors (including overflow/underflow).
bool safe_strto32(const char* str, int32* value);
bool safe_strto64(const char* str, int64* value);
bool safe_strtou32(const char* str, uint32* value);
bool safe_strtou64(const char* str, uint64* value);
// Convert strings to floating point values.
// Leading and trailing spaces are allowed.
// Values may be rounded on over- and underflow.
bool safe_strtof(const char* str, float* value);
bool safe_strtod(const char* str, double* value);

bool safe_strto32(const string& str, int32* value);
bool safe_strto64(const string& str, int64* value);
bool safe_strtou32(const string& str, uint32* value);
bool safe_strtou64(const string& str, uint64* value);
bool safe_strtof(const string& str, float* value);
bool safe_strtod(const string& str, double* value);

// Parses buffer_size many characters from startptr into value.
bool safe_strto32(const char* startptr, int buffer_size, int32* value);
bool safe_strto64(const char* startptr, int buffer_size, int64* value);

// Parses with a fixed base between 2 and 36. For base 16, leading "0x" is ok.
// If base is set to 0, its value is inferred from the beginning of str:
// "0x" means base 16, "0" means base 8, otherwise base 10 is used.
bool safe_strto32_base(const char* str, int32* value, int base);
bool safe_strto64_base(const char* str, int64* value, int base);
bool safe_strtou32_base(const char* str, uint32* value, int base);
bool safe_strtou64_base(const char* str, uint64* value, int base);

bool safe_strto32_base(const string& str, int32* value, int base);
bool safe_strto64_base(const string& str, int64* value, int base);
bool safe_strtou32_base(const string& str, uint32* value, int base);
bool safe_strtou64_base(const string& str, uint64* value, int base);

bool safe_strto32_base(const char* startptr, int buffer_size, int32* value, int base);
bool safe_strto64_base(const char* startptr, int buffer_size, int64* value, int base);

// u64tostr_base36()
//    The inverse of safe_strtou64_base, converts the number agument to
//    a string representation in base-36.
//    Conversion fails if buffer is too small to to hold the string and
//    terminating NUL.
//    Returns number of bytes written, not including terminating NUL.
//    Return value 0 indicates error.
size_t u64tostr_base36(uint64 number, size_t buf_size, char* buffer);

// Similar to atoi(s), except s could be like "16k", "32M", "2G", "4t".
uint64 atoi_kmgt(const char* s);
inline uint64 atoi_kmgt(const string& s) {
    return atoi_kmgt(s.c_str());
}

// ----------------------------------------------------------------------
// FastIntToBuffer()
// FastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// FastTimeToBuffer()
//    These are intended for speed.  FastIntToBuffer() assumes the
//    integer is non-negative.  FastHexToBuffer() puts output in
//    hex rather than decimal.  FastTimeToBuffer() puts the output
//    into RFC822 format.
//
//    FastHex64ToBuffer() puts a 64-bit unsigned value in hex-format,
//    padded to exactly 16 bytes (plus one byte for '\0')
//
//    FastHex32ToBuffer() puts a 32-bit unsigned value in hex-format,
//    padded to exactly 8 bytes (plus one byte for '\0')
//
//    All functions take the output buffer as an arg.  FastInt() uses
//    at most 22 bytes, FastTime() uses exactly 30 bytes.  They all
//    return a pointer to the beginning of the output, which for
//    FastHex() may not be the beginning of the input buffer.  (For
//    all others, we guarantee that it is.)
//
//    NOTE: In 64-bit land, sizeof(time_t) is 8, so it is possible
//    to pass to FastTimeToBuffer() a time whose year cannot be
//    represented in 4 digits. In this case, the output buffer
//    will contain the string "Invalid:<value>"
// ----------------------------------------------------------------------

// Previously documented minimums -- the buffers provided must be at least this
// long, though these numbers are subject to change:
//     Int32, UInt32:        12 bytes
//     Int64, UInt64, Hex:   22 bytes
//     Time:                 30 bytes
//     Hex32:                 9 bytes
//     Hex64:                17 bytes
// Use kFastToBufferSize rather than hardcoding constants.
static const int kFastToBufferSize = 32;

char* FastInt32ToBuffer(int32 i, char* buffer);
char* FastInt64ToBuffer(int64 i, char* buffer);
char* FastUInt32ToBuffer(uint32 i, char* buffer);
char* FastUInt64ToBuffer(uint64 i, char* buffer);
char* FastHexToBuffer(int i, char* buffer) MUST_USE_RESULT;
char* FastTimeToBuffer(time_t t, char* buffer);
char* FastHex64ToBuffer(uint64 i, char* buffer);
char* FastHex32ToBuffer(uint32 i, char* buffer);

// at least 22 bytes long
inline char* FastIntToBuffer(int i, char* buffer) {
    return (sizeof(i) == 4 ? FastInt32ToBuffer(i, buffer) : FastInt64ToBuffer(i, buffer));
}
inline char* FastUIntToBuffer(unsigned int i, char* buffer) {
    return (sizeof(i) == 4 ? FastUInt32ToBuffer(i, buffer) : FastUInt64ToBuffer(i, buffer));
}


// ----------------------------------------------------------------------
// HexDigitsPrefix()
//  returns 1 if buf is prefixed by "num_digits" of hex digits
//  returns 0 otherwise.
//  The function checks for '\0' for string termination.
// ----------------------------------------------------------------------
int HexDigitsPrefix(const char* buf, int num_digits);

// ----------------------------------------------------------------------
// ConsumeStrayLeadingZeroes
//    Eliminates all leading zeroes (unless the string itself is composed
//    of nothing but zeroes, in which case one is kept: 0...0 becomes 0).
void ConsumeStrayLeadingZeroes(string* str);

// ----------------------------------------------------------------------
// ParseLeadingInt32Value
//    A simple parser for int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use ParseLeadingDec32Value.
// --------------------------------------------------------------------
int32 ParseLeadingInt32Value(const char* str, int32 deflt);
inline int32 ParseLeadingInt32Value(const string& str, int32 deflt) {
    return ParseLeadingInt32Value(str.c_str(), deflt);
}

// ParseLeadingUInt32Value
//    A simple parser for uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use ParseLeadingUDec32Value.
// --------------------------------------------------------------------
uint32 ParseLeadingUInt32Value(const char* str, uint32 deflt);
inline uint32 ParseLeadingUInt32Value(const string& str, uint32 deflt) {
    return ParseLeadingUInt32Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// ParseLeadingDec32Value
//    A simple parser for decimal int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: ParseLeadingDec64Value
// --------------------------------------------------------------------
int32 ParseLeadingDec32Value(const char* str, int32 deflt);
inline int32 ParseLeadingDec32Value(const string& str, int32 deflt) {
    return ParseLeadingDec32Value(str.c_str(), deflt);
}

// ParseLeadingUDec32Value
//    A simple parser for decimal uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: ParseLeadingUDec64Value
// --------------------------------------------------------------------
uint32 ParseLeadingUDec32Value(const char* str, uint32 deflt);
inline uint32 ParseLeadingUDec32Value(const string& str, uint32 deflt) {
    return ParseLeadingUDec32Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// ParseLeadingUInt64Value
// ParseLeadingInt64Value
// ParseLeadingHex64Value
// ParseLeadingDec64Value
// ParseLeadingUDec64Value
//    A simple parser for long long values.
//    Returns the parsed value if a
//    valid integer is found; else returns deflt
// --------------------------------------------------------------------
uint64 ParseLeadingUInt64Value(const char* str, uint64 deflt);
inline uint64 ParseLeadingUInt64Value(const string& str, uint64 deflt) {
    return ParseLeadingUInt64Value(str.c_str(), deflt);
}
int64 ParseLeadingInt64Value(const char* str, int64 deflt);
inline int64 ParseLeadingInt64Value(const string& str, int64 deflt) {
    return ParseLeadingInt64Value(str.c_str(), deflt);
}
uint64 ParseLeadingHex64Value(const char* str, uint64 deflt);
inline uint64 ParseLeadingHex64Value(const string& str, uint64 deflt) {
    return ParseLeadingHex64Value(str.c_str(), deflt);
}
int64 ParseLeadingDec64Value(const char* str, int64 deflt);
inline int64 ParseLeadingDec64Value(const string& str, int64 deflt) {
    return ParseLeadingDec64Value(str.c_str(), deflt);
}
uint64 ParseLeadingUDec64Value(const char* str, uint64 deflt);
inline uint64 ParseLeadingUDec64Value(const string& str, uint64 deflt) {
    return ParseLeadingUDec64Value(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// ParseLeadingDoubleValue
//    A simple parser for double values. Returns the parsed value
//    if a valid double is found; else returns deflt. It does not
//    check if str is entirely consumed.
// --------------------------------------------------------------------
double ParseLeadingDoubleValue(const char* str, double deflt);
inline double ParseLeadingDoubleValue(const string& str, double deflt) {
    return ParseLeadingDoubleValue(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// ParseLeadingBoolValue()
//    A recognizer of boolean string values. Returns the parsed value
//    if a valid value is found; else returns deflt.  This skips leading
//    whitespace, is case insensitive, and recognizes these forms:
//    0/1, false/true, no/yes, n/y
// --------------------------------------------------------------------
bool ParseLeadingBoolValue(const char* str, bool deflt);
inline bool ParseLeadingBoolValue(const string& str, bool deflt) {
    return ParseLeadingBoolValue(str.c_str(), deflt);
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

int AutoDigitStrCmp(const char* a, int alen, const char* b, int blen, bool strict);

bool AutoDigitLessThan(const char* a, int alen, const char* b, int blen);

bool StrictAutoDigitLessThan(const char* a, int alen, const char* b, int blen);

struct autodigit_less {
    bool operator()(const string& a, const string& b) const {
        return AutoDigitLessThan(a.data(), a.size(), b.data(), b.size());
    }
};

struct autodigit_greater {
    bool operator()(const string& a, const string& b) const {
        return AutoDigitLessThan(b.data(), b.size(), a.data(), a.size());
    }
};

struct strict_autodigit_less {
    bool operator()(const string& a, const string& b) const {
        return StrictAutoDigitLessThan(a.data(), a.size(), b.data(), b.size());
    }
};

struct strict_autodigit_greater {
    bool operator()(const string& a, const string& b) const {
        return StrictAutoDigitLessThan(b.data(), b.size(), a.data(), a.size());
    }
};


// SimpleAtoi converts a string to an integer.
// Uses safe_strto?() for actual parsing, so strict checking is
// applied, which is to say, the string must be a base-10 integer, optionally
// followed or preceded by whitespace, and value has to be in the range of
// the corresponding integer type.
//
// Returns true if parsing was successful.
template <typename int_type>
bool MUST_USE_RESULT SimpleAtoi(const char* s, int_type* out) {
    // Must be of integer type (not pointer type), with more than 16-bitwidth.
    COMPILE_ASSERT(sizeof(*out) == 4 || sizeof(*out) == 8, SimpleAtoiWorksWith32Or64BitInts);
    if (std::numeric_limits<int_type>::is_signed) { // Signed
        if (sizeof(*out) == 64 / 8) {               // 64-bit
            return safe_strto64(s, reinterpret_cast<int64*>(out));
        } else { // 32-bit
            return safe_strto32(s, reinterpret_cast<int32*>(out));
        }
    } else {                          // Unsigned
        if (sizeof(*out) == 64 / 8) { // 64-bit
            return safe_strtou64(s, reinterpret_cast<uint64*>(out));
        } else { // 32-bit
            return safe_strtou32(s, reinterpret_cast<uint32*>(out));
        }
    }
}

template <typename int_type>
bool MUST_USE_RESULT SimpleAtoi(const string& s, int_type* out) {
    return SimpleAtoi(s.c_str(), out);
}

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    Description: converts a double or float to a string which, if
//    passed to strtod(), will produce the exact same original double
//    (except in case of NaN; all NaNs are considered the same value).
//    We try to keep the string short but it's not guaranteed to be as
//    short as possible.
//
//    DoubleToBuffer() and FloatToBuffer() write the text to the given
//    buffer and return it.  The buffer must be at least
//    kDoubleToBufferSize bytes for doubles and kFloatToBufferSize
//    bytes for floats.  kFastToBufferSize is also guaranteed to be large
//    enough to hold either.
//
//    Return value: string
// ----------------------------------------------------------------------
string SimpleDtoa(double value);
string SimpleFtoa(float value);

int DoubleToBuffer(double i, int width, char* buffer);
int FloatToBuffer(float i, int width, char* buffer);

char* DoubleToBuffer(double i, char* buffer);
char* FloatToBuffer(float i, char* buffer);

int FastDoubleToBuffer(double i, char* buffer);
int FastFloatToBuffer(float i, char* buffer);
// In practice, doubles should never need more than 24 bytes and floats
// should never need more than 14 (including null terminators), but we
// overestimate to be safe.
static const int kDoubleToBufferSize = 32;
static const int kFloatToBufferSize = 24;

// ----------------------------------------------------------------------
// SimpleItoaWithCommas()
//    Description: converts an integer to a string.
//    Puts commas every 3 spaces.
//    Faster than printf("%d")?
//
//    Return value: string
// ----------------------------------------------------------------------
string SimpleItoaWithCommas(int32 i);
string SimpleItoaWithCommas(uint32 i);
string SimpleItoaWithCommas(int64 i);
string SimpleItoaWithCommas(uint64 i);

char* SimpleItoaWithCommas(int64_t i, char* buffer, int32_t buffer_size);
char* SimpleItoaWithCommas(__int128_t i, char* buffer, int32_t buffer_size);

// ----------------------------------------------------------------------
// ItoaKMGT()
//    Description: converts an integer to a string
//    Truncates values to K, G, M or T as appropriate
//    Opposite of atoi_kmgt()
//    e.g. 3000 -> 2K   57185920 -> 45M
//
//    Return value: string
//
// AccurateItoaKMGT()
//    Description: preserve accuracy
// ----------------------------------------------------------------------
string ItoaKMGT(int64 i);
string AccurateItoaKMGT(int64 i);

// END DOXYGEN SplitFunctions grouping
/* @} */

// These functions are deprecated.
// Do not use in new code.

// // DEPRECATED(wadetregaskis).  Just call StringPrintf or SimpleFtoa.
// string FloatToString(float f, const char* format);

// // DEPRECATED(wadetregaskis).  Just call StringPrintf or SimpleItoa.
// string IntToString(int i, const char* format);

// // DEPRECATED(wadetregaskis).  Just call StringPrintf or SimpleItoa.
// string Int64ToString(int64 i64, const char* format);

// // DEPRECATED(wadetregaskis).  Just call StringPrintf or SimpleItoa.
// string UInt64ToString(uint64 ui64, const char* format);

// // DEPRECATED(wadetregaskis).  Just call StringPrintf.
// inline string FloatToString(float f) {
//   return StringPrintf("%7f", f);
// }

// // DEPRECATED(wadetregaskis).  Just call StringPrintf.
// inline string IntToString(int i) {
//   return StringPrintf("%7d", i);
// }

// // DEPRECATED(wadetregaskis).  Just call StringPrintf.
// inline string Int64ToString(int64 i64) {
//   return StringPrintf("%7" PRId64, i64);
// }

// // DEPRECATED(wadetregaskis).  Just call StringPrintf.
// inline string UInt64ToString(uint64 ui64) {
//   return StringPrintf("%7" PRIu64, ui64);
// }
