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

int DoubleToBuffer(double i, int width, char* buffer);
int FloatToBuffer(float i, int width, char* buffer);

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

char* SimpleItoaWithCommas(int64_t i, char* buffer, int32_t buffer_size);
char* SimpleItoaWithCommas(__int128_t i, char* buffer, int32_t buffer_size);

