// Copyright 2006, Google Inc.  All rights reserved.
//
// Paranoid helpers.  This is separate from basictypes.h so that it
// can use logging.

#ifndef BASE_PARANOID_H_
#define BASE_PARANOID_H_

#include <common/logging.h>

#include "gutil/logging-inl.h"

// Sanitize a bool value which might be sour.
//
// I made up the term "sour bool".  It means a bool that is not false (0x0)
// and not true (0x1) but has one of the other 2^N-2 states.  A common way
// to create a sour bool is to read an uninitialized bool object.
//
// The Standard says:
// [dcl.init] 8.5 -9- "Otherwise, if no initializer is specified for an
// object, the object and its subobjects, if any, have an indeterminate
// initial value."
// [basic.fundamental] 3.9.1 -5- footnote 42: "Using a bool value in ways
// described by this International standard as "undefined", such as by
// examining the value of an uninitialized automatic variable, might cause
// it to behave as if niether true nor false."
//
// Specifically, this program fragment:
//   bool b;
//   printf("%d\n", b ? 1 : 0);
// can print any value at all, not just 1 or 0!  gcc-4.1.0-piii-linux-opt
// generates code tantamount to "static_cast<int>(b)" with no comparison
// operators.  This is harmful for invalid values of b, but fast for all
// valid values.
//
// The original bug was a sour bool that confused the protobuf runtime.
// RawOutputToArray wrote a single byte with the sour bool value and
// ReadBool read a ReadVarint32.  If the sour bool did not look like a
// single-byte varint32, then the serialized protobuf would be unreadable.
//
// ===
//
// If you run into a compiler where the volatile pointer does not work, try
// a bit_cast.  Just plain "static_cast<unsigned char>(b) ? 1 : 0" does not
// work with gcc-4.1.0-piii-linux-opt, but bit_cast does.
//
// ===
//
// If the assert fires, you probably have an uninitialized bool value.  The
// original case of this was an auto struct with an uninitialized bool
// field.  It might also be memory corruption, or it might be a new C++
// compiler that has found a new way to hurt us.
//
// ===
//
// Props to Apurv Gupta for the report, Ian Lance Taylor for volatile,
// and Sanjay Ghemawat for general guidance.
//
// -- mec 2006-07-06

inline bool SanitizeBool(bool b) {
  unsigned char c = static_cast<unsigned char>(b);
  volatile unsigned char* p = &c;
  DCHECK_LT(*p, 2);
  return (*p != '\0') ? true : false;
}

// Returns true iff. a given bool is either true (0x1) or false (0x0).
// Mainly used for sanity checking for set_field(bool) in Protocol Buffer.
//
// This sanity checking is necessary since a sour bool might confuse the
// Protocol Buffer runtime as mentioned above.
//
// Uses an assembler sequence so as not to be compiler-optimization sensitive.
inline bool IsSaneBool(bool b) {
#if (defined __i386__ || defined __x86_64__) && defined __GNUC__
  bool result;
  // Set result to true if b is below or equal to 0x1.
  __asm__("cmpb  $0x1, %1\n\t"
          "setbe %0"
          : "=m" (result)  // Output spec
          : "m" (b)        // Input spec
          : "cc");         // Clobbers condition-codes
  return result;
#else
  unsigned char c = static_cast<unsigned char>(b);
  volatile unsigned char* p = &c;
  return *p <= 1;
#endif
}

#endif  // BASE_PARANOID_H_
