// Copyright 2011 Google Inc. All Rights Reserved.
//
// Legacy implementation of the core Jenkins lookup2 algorithm. This is used in
// many older hash functions which we are unable to remove or change due to the
// values being recorded. New code should not use any of these routines and
// should not include this header file. It pollutes the global namespace with
// the 'mix' function.
//
// This file contains the basic hash "mix" code which is widely referenced.
//
// This file also contains routines used to load an unaligned little-endian
// word from memory.  This relatively generic functionality probably
// shouldn't live in this file.

#pragma once

#include "gutil/integral_types.h"

// ----------------------------------------------------------------------
// mix()
//    The hash function I use is due to Bob Jenkins (see
//    http://burtleburtle.net/bob/hash/index.html).
//    Each mix takes 36 instructions, in 18 cycles if you're lucky.
//
//    On x86 architectures, this requires 45 instructions in 27 cycles,
//    if you're lucky.
// ----------------------------------------------------------------------

static inline void mix(uint32& a, uint32& b, uint32& c) { // 32bit version
    a -= b;
    a -= c;
    a ^= (c >> 13);
    b -= c;
    b -= a;
    b ^= (a << 8);
    c -= a;
    c -= b;
    c ^= (b >> 13);
    a -= b;
    a -= c;
    a ^= (c >> 12);
    b -= c;
    b -= a;
    b ^= (a << 16);
    c -= a;
    c -= b;
    c ^= (b >> 5);
    a -= b;
    a -= c;
    a ^= (c >> 3);
    b -= c;
    b -= a;
    b ^= (a << 10);
    c -= a;
    c -= b;
    c ^= (b >> 15);
}

static inline void mix(uint64& a, uint64& b, uint64& c) { // 64bit version
    a -= b;
    a -= c;
    a ^= (c >> 43);
    b -= c;
    b -= a;
    b ^= (a << 9);
    c -= a;
    c -= b;
    c ^= (b >> 8);
    a -= b;
    a -= c;
    a ^= (c >> 38);
    b -= c;
    b -= a;
    b ^= (a << 23);
    c -= a;
    c -= b;
    c ^= (b >> 5);
    a -= b;
    a -= c;
    a ^= (c >> 35);
    b -= c;
    b -= a;
    b ^= (a << 49);
    c -= a;
    c -= b;
    c ^= (b >> 11);
    a -= b;
    a -= c;
    a ^= (c >> 12);
    b -= c;
    b -= a;
    b ^= (a << 18);
    c -= a;
    c -= b;
    c ^= (b >> 22);
}
