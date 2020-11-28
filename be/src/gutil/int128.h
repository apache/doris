// Copyright 2004 Google Inc.
// All Rights Reserved.
//

#ifndef BASE_INT128_H_
#define BASE_INT128_H_

#include <iosfwd>
using std::ostream;
#include "gutil/integral_types.h"

struct uint128_pod;

// An unsigned 128-bit integer type. Thread-compatible.
class uint128 {
public:
    uint128(); // Sets to 0, but don't trust on this behavior.
    uint128(uint64 top, uint64 bottom);
#ifndef SWIG
    uint128(int bottom);
    uint128(uint32 bottom); // Top 96 bits = 0
#endif
    uint128(uint64 bottom); // hi_ = 0
    uint128(const uint128& val);
    uint128(const uint128_pod& val);

    void Initialize(uint64 top, uint64 bottom);

    uint128& operator=(const uint128& b);

    // Arithmetic operators.
    // TODO: division, etc.
    uint128& operator+=(const uint128& b);
    uint128& operator-=(const uint128& b);
    uint128& operator*=(const uint128& b);
    uint128 operator++(int);
    uint128 operator--(int);
    uint128& operator<<=(int);
    uint128& operator>>=(int);
    uint128& operator&=(const uint128& b);
    uint128& operator|=(const uint128& b);
    uint128& operator^=(const uint128& b);
    uint128& operator++();
    uint128& operator--();

    friend uint64 Uint128Low64(const uint128& v);
    friend uint64 Uint128High64(const uint128& v);

    // We add "std::" to avoid including all of port.h.
    friend std::ostream& operator<<(std::ostream& o, const uint128& b);

private:
    // Little-endian memory order optimizations can benefit from
    // having lo_ first, hi_ last.
    // See util/endian/endian.h and Load128/Store128 for storing a uint128.
    uint64 lo_;
    uint64 hi_;

    // Not implemented, just declared for catching automatic type conversions.
    uint128(uint8);
    uint128(uint16);
    uint128(float v);
    uint128(double v);
};

// This is a POD form of uint128 which can be used for static variables which
// need to be operated on as uint128.
struct uint128_pod {
    // Note: The ordering of fields is different than 'class uint128' but the
    // same as its 2-arg constructor.  This enables more obvious initialization
    // of static instances, which is the primary reason for this struct in the
    // first place.  This does not seem to defeat any optimizations wrt
    // operations involving this struct.
    uint64 hi;
    uint64 lo;
};

extern const uint128_pod kuint128max;

// allow uint128 to be logged
extern std::ostream& operator<<(std::ostream& o, const uint128& b);

// Methods to access low and high pieces of 128-bit value.
// Defined externally from uint128 to facilitate conversion
// to native 128-bit types when compilers support them.
inline uint64 Uint128Low64(const uint128& v) {
    return v.lo_;
}
inline uint64 Uint128High64(const uint128& v) {
    return v.hi_;
}

// TODO: perhaps it would be nice to have int128, a signed 128-bit type?

// --------------------------------------------------------------------------
//                      Implementation details follow
// --------------------------------------------------------------------------
inline bool operator==(const uint128& lhs, const uint128& rhs) {
    return (Uint128Low64(lhs) == Uint128Low64(rhs) && Uint128High64(lhs) == Uint128High64(rhs));
}
inline bool operator!=(const uint128& lhs, const uint128& rhs) {
    return !(lhs == rhs);
}
inline uint128& uint128::operator=(const uint128& b) {
    lo_ = b.lo_;
    hi_ = b.hi_;
    return *this;
}

inline uint128::uint128() : lo_(0), hi_(0) {}
inline uint128::uint128(uint64 top, uint64 bottom) : lo_(bottom), hi_(top) {}
inline uint128::uint128(const uint128& v) : lo_(v.lo_), hi_(v.hi_) {}
inline uint128::uint128(const uint128_pod& v) : lo_(v.lo), hi_(v.hi) {}
inline uint128::uint128(uint64 bottom) : lo_(bottom), hi_(0) {}
#ifndef SWIG
inline uint128::uint128(uint32 bottom) : lo_(bottom), hi_(0) {}
inline uint128::uint128(int bottom) : lo_(bottom), hi_(0) {
    if (bottom < 0) {
        --hi_;
    }
}
#endif
inline void uint128::Initialize(uint64 top, uint64 bottom) {
    hi_ = top;
    lo_ = bottom;
}

// Comparison operators.

#define CMP128(op)                                                    \
    inline bool operator op(const uint128& lhs, const uint128& rhs) { \
        return (Uint128High64(lhs) == Uint128High64(rhs))             \
                       ? (Uint128Low64(lhs) op Uint128Low64(rhs))     \
                       : (Uint128High64(lhs) op Uint128High64(rhs));  \
    }

CMP128(<)
CMP128(>)
CMP128(>=)
CMP128(<=)

#undef CMP128

// Unary operators

inline uint128 operator-(const uint128& val) {
    const uint64 hi_flip = ~Uint128High64(val);
    const uint64 lo_flip = ~Uint128Low64(val);
    const uint64 lo_add = lo_flip + 1;
    if (lo_add < lo_flip) {
        return uint128(hi_flip + 1, lo_add);
    }
    return uint128(hi_flip, lo_add);
}

inline bool operator!(const uint128& val) {
    return !Uint128High64(val) && !Uint128Low64(val);
}

// Logical operators.

inline uint128 operator~(const uint128& val) {
    return uint128(~Uint128High64(val), ~Uint128Low64(val));
}

#define LOGIC128(op)                                                     \
    inline uint128 operator op(const uint128& lhs, const uint128& rhs) { \
        return uint128(Uint128High64(lhs) op Uint128High64(rhs),         \
                       Uint128Low64(lhs) op Uint128Low64(rhs));          \
    }

LOGIC128(|)
LOGIC128(&)
LOGIC128(^)

#undef LOGIC128

#define LOGICASSIGN128(op)                                       \
    inline uint128& uint128::operator op(const uint128& other) { \
        hi_ op other.hi_;                                        \
        lo_ op other.lo_;                                        \
        return *this;                                            \
    }

LOGICASSIGN128(|=)
LOGICASSIGN128(&=)
LOGICASSIGN128(^=)

#undef LOGICASSIGN128

// Shift operators.

inline uint128 operator<<(const uint128& val, int amount) {
    // uint64 shifts of >= 64 are undefined, so we will need some special-casing.
    if (amount < 64) {
        if (amount == 0) {
            return val;
        }
        uint64 new_hi = (Uint128High64(val) << amount) | (Uint128Low64(val) >> (64 - amount));
        uint64 new_lo = Uint128Low64(val) << amount;
        return uint128(new_hi, new_lo);
    } else if (amount < 128) {
        return uint128(Uint128Low64(val) << (amount - 64), 0);
    } else {
        return uint128(0, 0);
    }
}

inline uint128 operator>>(const uint128& val, int amount) {
    // uint64 shifts of >= 64 are undefined, so we will need some special-casing.
    if (amount < 64) {
        if (amount == 0) {
            return val;
        }
        uint64 new_hi = Uint128High64(val) >> amount;
        uint64 new_lo = (Uint128Low64(val) >> amount) | (Uint128High64(val) << (64 - amount));
        return uint128(new_hi, new_lo);
    } else if (amount < 128) {
        return uint128(0, Uint128High64(val) >> (amount - 64));
    } else {
        return uint128(0, 0);
    }
}

inline uint128& uint128::operator<<=(int amount) {
    // uint64 shifts of >= 64 are undefined, so we will need some special-casing.
    if (amount < 64) {
        if (amount != 0) {
            hi_ = (hi_ << amount) | (lo_ >> (64 - amount));
            lo_ = lo_ << amount;
        }
    } else if (amount < 128) {
        hi_ = lo_ << (amount - 64);
        lo_ = 0;
    } else {
        hi_ = 0;
        lo_ = 0;
    }
    return *this;
}

inline uint128& uint128::operator>>=(int amount) {
    // uint64 shifts of >= 64 are undefined, so we will need some special-casing.
    if (amount < 64) {
        if (amount != 0) {
            lo_ = (lo_ >> amount) | (hi_ << (64 - amount));
            hi_ = hi_ >> amount;
        }
    } else if (amount < 128) {
        hi_ = 0;
        lo_ = hi_ >> (amount - 64);
    } else {
        hi_ = 0;
        lo_ = 0;
    }
    return *this;
}

inline uint128 operator+(const uint128& lhs, const uint128& rhs) {
    return uint128(lhs) += rhs;
}

inline uint128 operator-(const uint128& lhs, const uint128& rhs) {
    return uint128(lhs) -= rhs;
}

inline uint128 operator*(const uint128& lhs, const uint128& rhs) {
    return uint128(lhs) *= rhs;
}

inline uint128& uint128::operator+=(const uint128& b) {
    hi_ += b.hi_;
    uint64 lolo = lo_ + b.lo_;
    if (lolo < lo_) ++hi_;
    lo_ = lolo;
    return *this;
}

inline uint128& uint128::operator-=(const uint128& b) {
    hi_ -= b.hi_;
    if (b.lo_ > lo_) --hi_;
    lo_ -= b.lo_;
    return *this;
}

inline uint128& uint128::operator*=(const uint128& b) {
    uint64 a96 = hi_ >> 32;
    uint64 a64 = hi_ & 0xffffffffu;
    uint64 a32 = lo_ >> 32;
    uint64 a00 = lo_ & 0xffffffffu;
    uint64 b96 = b.hi_ >> 32;
    uint64 b64 = b.hi_ & 0xffffffffu;
    uint64 b32 = b.lo_ >> 32;
    uint64 b00 = b.lo_ & 0xffffffffu;
    // multiply [a96 .. a00] x [b96 .. b00]
    // terms higher than c96 disappear off the high side
    // terms c96 and c64 are safe to ignore carry bit
    uint64 c96 = a96 * b00 + a64 * b32 + a32 * b64 + a00 * b96;
    uint64 c64 = a64 * b00 + a32 * b32 + a00 * b64;
    this->hi_ = (c96 << 32) + c64;
    this->lo_ = 0;
    // add terms after this one at a time to capture carry
    *this += uint128(a32 * b00) << 32;
    *this += uint128(a00 * b32) << 32;
    *this += a00 * b00;
    return *this;
}

inline uint128 uint128::operator++(int) {
    uint128 tmp(*this);
    *this += 1;
    return tmp;
}

inline uint128 uint128::operator--(int) {
    uint128 tmp(*this);
    *this -= 1;
    return tmp;
}

inline uint128& uint128::operator++() {
    *this += 1;
    return *this;
}

inline uint128& uint128::operator--() {
    *this -= 1;
    return *this;
}

#endif // BASE_INT128_H_
