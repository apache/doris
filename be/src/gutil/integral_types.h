// Copyright 2010 Google Inc. All Rights Reserved.
//
// Basic integer type definitions for various platforms
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//

#ifndef BASE_INTEGRAL_TYPES_H_
#define BASE_INTEGRAL_TYPES_H_

#include <inttypes.h>

// These typedefs are also defined in base/google.swig. In the
// SWIG environment, we use those definitions and avoid duplicate
// definitions here with an ifdef. The definitions should be the
// same in both files, and ideally be only defined in this file.
#ifndef SWIG
// Standard typedefs
// All Google2 code is compiled with -funsigned-char to make "char"
// unsigned.  Google2 code therefore doesn't need a "uchar" type.
typedef int8_t schar;
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
#ifdef _MSC_VER
typedef __int64 int64;
#else
typedef int64_t int64;
#endif /* _MSC_VER */

// NOTE: unsigned types are DANGEROUS in loops and other arithmetical
// places.  Use the signed types unless your variable represents a bit
// pattern (eg a hash value) or you really need the extra bit.  Do NOT
// use 'unsigned' to express "this value should always be positive";
// use assertions for this.

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
#ifdef _MSC_VER
typedef unsigned __int64 uint64;
#else
typedef uint64_t uint64;
#endif /* _MSC_VER */

// A type to represent a Unicode code-point value. As of Unicode 4.0,
// such values require up to 21 bits.
// (For type-checking on pointers, make this explicitly signed,
// and it should always be the signed version of whatever int32 is.)
typedef signed int char32;

//  A type to represent a natural machine word (for e.g. efficiently
// scanning through memory for checksums or index searching). Don't use
// this for storing normal integers. Ideally this would be just
// unsigned int, but our 64-bit architectures use the LP64 model
// (http://www.opengroup.org/public/tech/aspen/lp64_wp.htm), hence
// their ints are only 32 bits. We want to use the same fundamental
// type on all archs if possible to preserve *printf() compatability.
typedef unsigned long uword_t;

#endif /* SWIG */

// long long macros to be used because gcc and vc++ use different suffixes,
// and different size specifiers in format strings
#undef GG_LONGLONG
#undef GG_ULONGLONG
#undef GG_LL_FORMAT

#ifdef _MSC_VER /* if Visual C++ */

// VC++ long long suffixes
#define GG_LONGLONG(x) x##I64
#define GG_ULONGLONG(x) x##UI64

#else /* not Visual C++ */

#define GG_LONGLONG(x) x##LL
#define GG_ULONGLONG(x) x##ULL

#endif // _MSC_VER

#endif // BASE_INTEGRAL_TYPES_H_
