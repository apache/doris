// Copyright 2011 Google Inc. All Rights Reserved.
//
// Contains the legacy Bob Jenkins Lookup2-based hashing routines. These need to
// always return the same results as their values have been recorded in various
// places and cannot easily be updated.
//
// Original Author: Sanjay Ghemawat
//
// This is based on Bob Jenkins newhash function
// see: http://burtleburtle.net/bob/hash/evahash.html
// According to http://burtleburtle.net/bob/c/lookup2.c,
// his implementation is public domain.
//
// The implementation here is backwards compatible with the google1
// implementation.  The google1 implementation used a 'signed char *'
// to load words from memory a byte at a time.  See gwshash.cc for an
// implementation that is compatible with Bob Jenkins' lookup2.c.

#include "gutil/hash/jenkins.h"

#include <common/logging.h>

#include "gutil/hash/jenkins_lookup2.h"
#include "gutil/integral_types.h"

static inline uint32 char2unsigned(char c) {
    return static_cast<uint32>(static_cast<unsigned char>(c));
}

static inline uint64 char2unsigned64(char c) {
    return static_cast<uint64>(static_cast<unsigned char>(c));
}

uint32 Hash32StringWithSeedReferenceImplementation(const char* s, uint32 len, uint32 c) {
    uint32 a, b;
    uint32 keylen;

    a = b = 0x9e3779b9UL; // the golden ratio; an arbitrary value

    for (keylen = len; keylen >= 3 * sizeof(a);
         keylen -= static_cast<uint32>(3 * sizeof(a)), s += 3 * sizeof(a)) {
        a += Google1At(s);
        b += Google1At(s + sizeof(a));
        c += Google1At(s + sizeof(a) * 2);
        mix(a, b, c);
    }

    c += len;
    switch (keylen) { // deal with rest.  Cases fall through
    case 11:
        c += char2unsigned(s[10]) << 24;
    case 10:
        c += char2unsigned(s[9]) << 16;
    case 9:
        c += char2unsigned(s[8]) << 8;
        // the first byte of c is reserved for the length
    case 8:
        b += Google1At(s + 4);
        a += Google1At(s);
        break;
    case 7:
        b += char2unsigned(s[6]) << 16;
    case 6:
        b += char2unsigned(s[5]) << 8;
    case 5:
        b += char2unsigned(s[4]);
    case 4:
        a += Google1At(s);
        break;
    case 3:
        a += char2unsigned(s[2]) << 16;
    case 2:
        a += char2unsigned(s[1]) << 8;
    case 1:
        a += char2unsigned(s[0]);
        // case 0 : nothing left to add
    }
    mix(a, b, c);
    return c;
}

uint32 Hash32StringWithSeed(const char* s, uint32 len, uint32 c) {
    uint32 a, b;
    uint32 keylen;

    a = b = 0x9e3779b9UL; // the golden ratio; an arbitrary value

    keylen = len;
    if (keylen >= 4 * sizeof(a)) {
        uint32 word32AtOffset0 = Google1At(s);
        do {
            a += word32AtOffset0;
            b += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            word32AtOffset0 = Google1At(s);
            mix(a, b, c);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
        } while (keylen >= 4 * sizeof(a));
        if (keylen >= 3 * sizeof(a)) {
            a += word32AtOffset0;
            b += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            mix(a, b, c);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
            DCHECK_LT(keylen, sizeof(a));
            c += len;
            switch (keylen) { // deal with rest.  Cases fall through
            case 3:
                a += char2unsigned(s[2]) << 16;
            case 2:
                a += char2unsigned(s[1]) << 8;
            case 1:
                a += char2unsigned(s[0]);
            }
        } else {
            DCHECK(sizeof(a) <= keylen && keylen < 3 * sizeof(a));
            c += len;
            switch (keylen) { // deal with rest.  Cases fall through
            case 11:
                c += char2unsigned(s[10]) << 24;
            case 10:
                c += char2unsigned(s[9]) << 16;
            case 9:
                c += char2unsigned(s[8]) << 8;
            case 8:
                b += Google1At(s + 4);
                a += word32AtOffset0;
                break;
            case 7:
                b += char2unsigned(s[6]) << 16;
            case 6:
                b += char2unsigned(s[5]) << 8;
            case 5:
                b += char2unsigned(s[4]);
            case 4:
                a += word32AtOffset0;
                break;
            }
        }
    } else {
        if (keylen >= 3 * sizeof(a)) {
            a += Google1At(s);
            b += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            mix(a, b, c);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
        }
        c += len;
        switch (keylen) { // deal with rest.  Cases fall through
        case 11:
            c += char2unsigned(s[10]) << 24;
        case 10:
            c += char2unsigned(s[9]) << 16;
        case 9:
            c += char2unsigned(s[8]) << 8;
        case 8:
            b += Google1At(s + 4);
            a += Google1At(s);
            break;
        case 7:
            b += char2unsigned(s[6]) << 16;
        case 6:
            b += char2unsigned(s[5]) << 8;
        case 5:
            b += char2unsigned(s[4]);
        case 4:
            a += Google1At(s);
            break;
        case 3:
            a += char2unsigned(s[2]) << 16;
        case 2:
            a += char2unsigned(s[1]) << 8;
        case 1:
            a += char2unsigned(s[0]);
        }
    }
    mix(a, b, c);
    return c;
}

uint64 Hash64StringWithSeed(const char* s, uint32 len, uint64 c) {
    uint64 a, b;
    uint32 keylen;

    a = b = GG_ULONGLONG(0xe08c1d668b756f82); // the golden ratio; an arbitrary value

    for (keylen = len; keylen >= 3 * sizeof(a);
         keylen -= 3 * static_cast<uint32>(sizeof(a)), s += 3 * sizeof(a)) {
        a += Word64At(s);
        b += Word64At(s + sizeof(a));
        c += Word64At(s + sizeof(a) * 2);
        mix(a, b, c);
    }

    c += len;
    switch (keylen) { // deal with rest.  Cases fall through
    case 23:
        c += char2unsigned64(s[22]) << 56;
    case 22:
        c += char2unsigned64(s[21]) << 48;
    case 21:
        c += char2unsigned64(s[20]) << 40;
    case 20:
        c += char2unsigned64(s[19]) << 32;
    case 19:
        c += char2unsigned64(s[18]) << 24;
    case 18:
        c += char2unsigned64(s[17]) << 16;
    case 17:
        c += char2unsigned64(s[16]) << 8;
        // the first byte of c is reserved for the length
    case 16:
        b += Word64At(s + 8);
        a += Word64At(s);
        break;
    case 15:
        b += char2unsigned64(s[14]) << 48;
    case 14:
        b += char2unsigned64(s[13]) << 40;
    case 13:
        b += char2unsigned64(s[12]) << 32;
    case 12:
        b += char2unsigned64(s[11]) << 24;
    case 11:
        b += char2unsigned64(s[10]) << 16;
    case 10:
        b += char2unsigned64(s[9]) << 8;
    case 9:
        b += char2unsigned64(s[8]);
    case 8:
        a += Word64At(s);
        break;
    case 7:
        a += char2unsigned64(s[6]) << 48;
    case 6:
        a += char2unsigned64(s[5]) << 40;
    case 5:
        a += char2unsigned64(s[4]) << 32;
    case 4:
        a += char2unsigned64(s[3]) << 24;
    case 3:
        a += char2unsigned64(s[2]) << 16;
    case 2:
        a += char2unsigned64(s[1]) << 8;
    case 1:
        a += char2unsigned64(s[0]);
        // case 0: nothing left to add
    }
    mix(a, b, c);
    return c;
}
