// Copyright 2010 Google Inc. All Rights Reserved.
// Maintainer: mec@google.com (Michael Chastain)
//
// Convert strings to numbers or numbers to strings.

#pragma once

#include <cstdint>

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
