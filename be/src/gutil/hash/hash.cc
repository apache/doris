// Copyright 2011 Google Inc. All Rights Reserved.
//
// This is the legacy unified hash library implementation. Its components are
// being split up into smaller, dedicated libraries. What remains here are
// things still being migrated.
//
// To find the implementation of the core Bob Jenkins lookup2 hash, look in
// jenkins.cc.

#include "gutil/hash/hash.h"

#include <common/logging.h>

#include "gutil/hash/jenkins.h"
#include "gutil/hash/jenkins_lookup2.h"
#include "gutil/integral_types.h"
#include "gutil/logging-inl.h"

// For components that ship code externally (notably the Google Search
// Appliance) we want to change the fingerprint function so that
// attackers cannot mount offline attacks to find collisions with
// google.com internal fingerprints (most importantly, for URL
// fingerprints).
#ifdef GOOGLECLIENT
#error Do not compile this into binaries that we deliver to users!
#error Instead, use
#endif
#ifdef EXTERNAL_FP
static const uint32 kFingerprintSeed0 = 0xabc;
static const uint32 kFingerprintSeed1 = 0xdef;
#else
static const uint32 kFingerprintSeed0 = 0;
static const uint32 kFingerprintSeed1 = 102072;
#endif

static inline uint32 char2unsigned(char c) {
    return static_cast<uint32>(static_cast<unsigned char>(c));
}

uint64 FingerprintReferenceImplementation(const char* s, uint32 len) {
    uint32 hi = Hash32StringWithSeed(s, len, kFingerprintSeed0);
    uint32 lo = Hash32StringWithSeed(s, len, kFingerprintSeed1);
    return CombineFingerprintHalves(hi, lo);
}

// This is a faster version of FingerprintReferenceImplementation(),
// making use of the fact that we're hashing the same string twice.
// The code is tedious to read, but it's just two interleaved copies of
// Hash32StringWithSeed().
uint64 FingerprintInterleavedImplementation(const char* s, uint32 len) {
    uint32 a, b, c = kFingerprintSeed0, d, e, f = kFingerprintSeed1;
    uint32 keylen;

    a = b = d = e = 0x9e3779b9UL; // the golden ratio; an arbitrary value

    keylen = len;
    if (keylen >= 4 * sizeof(a)) {
        uint32 word32AtOffset0 = Google1At(s);
        do {
            a += word32AtOffset0;
            d += word32AtOffset0;
            b += Google1At(s + sizeof(a));
            e += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            f += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            word32AtOffset0 = Google1At(s);
            mix(a, b, c);
            mix(d, e, f);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
        } while (keylen >= 4 * sizeof(a));
        if (keylen >= 3 * sizeof(a)) {
            a += word32AtOffset0;
            d += word32AtOffset0;
            b += Google1At(s + sizeof(a));
            e += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            f += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            mix(a, b, c);
            mix(d, e, f);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
            DCHECK_LT(keylen, sizeof(a));
            c += len;
            f += len;
            switch (keylen) { // deal with rest.  Cases fall through
            case 3:
                a += char2unsigned(s[2]) << 16;
                d += char2unsigned(s[2]) << 16;
            case 2:
                a += char2unsigned(s[1]) << 8;
                d += char2unsigned(s[1]) << 8;
            case 1:
                a += char2unsigned(s[0]);
                d += char2unsigned(s[0]);
            }
        } else {
            DCHECK(sizeof(a) <= keylen && keylen < 3 * sizeof(a));
            c += len;
            f += len;
            switch (keylen) { // deal with rest.  Cases fall through
            case 11:
                c += char2unsigned(s[10]) << 24;
                f += char2unsigned(s[10]) << 24;
            case 10:
                c += char2unsigned(s[9]) << 16;
                f += char2unsigned(s[9]) << 16;
            case 9:
                c += char2unsigned(s[8]) << 8;
                f += char2unsigned(s[8]) << 8;
            case 8:
                b += Google1At(s + 4);
                a += word32AtOffset0;
                e += Google1At(s + 4);
                d += word32AtOffset0;
                break;
            case 7:
                b += char2unsigned(s[6]) << 16;
                e += char2unsigned(s[6]) << 16;
            case 6:
                b += char2unsigned(s[5]) << 8;
                e += char2unsigned(s[5]) << 8;
            case 5:
                b += char2unsigned(s[4]);
                e += char2unsigned(s[4]);
            case 4:
                a += word32AtOffset0;
                d += word32AtOffset0;
            }
        }
    } else {
        if (keylen >= 3 * sizeof(a)) {
            a += Google1At(s);
            d += Google1At(s);
            b += Google1At(s + sizeof(a));
            e += Google1At(s + sizeof(a));
            c += Google1At(s + sizeof(a) * 2);
            f += Google1At(s + sizeof(a) * 2);
            s += 3 * sizeof(a);
            mix(a, b, c);
            mix(d, e, f);
            keylen -= 3 * static_cast<uint32>(sizeof(a));
        }
        c += len;
        f += len;
        switch (keylen) { // deal with rest.  Cases fall through
        case 11:
            c += char2unsigned(s[10]) << 24;
            f += char2unsigned(s[10]) << 24;
        case 10:
            c += char2unsigned(s[9]) << 16;
            f += char2unsigned(s[9]) << 16;
        case 9:
            c += char2unsigned(s[8]) << 8;
            f += char2unsigned(s[8]) << 8;
        case 8:
            b += Google1At(s + 4);
            a += Google1At(s);
            e += Google1At(s + 4);
            d += Google1At(s);
            break;
        case 7:
            b += char2unsigned(s[6]) << 16;
            e += char2unsigned(s[6]) << 16;
        case 6:
            b += char2unsigned(s[5]) << 8;
            e += char2unsigned(s[5]) << 8;
        case 5:
            b += char2unsigned(s[4]);
            e += char2unsigned(s[4]);
        case 4:
            a += Google1At(s);
            d += Google1At(s);
            break;
        case 3:
            a += char2unsigned(s[2]) << 16;
            d += char2unsigned(s[2]) << 16;
        case 2:
            a += char2unsigned(s[1]) << 8;
            d += char2unsigned(s[1]) << 8;
        case 1:
            a += char2unsigned(s[0]);
            d += char2unsigned(s[0]);
        }
    }
    mix(a, b, c);
    mix(d, e, f);
    return CombineFingerprintHalves(c, f);
}
