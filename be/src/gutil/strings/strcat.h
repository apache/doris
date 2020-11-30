// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Merges strings or numbers with no delimiter.
//
#ifndef STRINGS_STRCAT_H_
#define STRINGS_STRCAT_H_

#include <string>
using std::string;

#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/stringpiece.h"

// The AlphaNum type was designed to be used as the parameter type for StrCat().
// I suppose that any routine accepting either a string or a number could accept
// it.  The basic idea is that by accepting a "const AlphaNum &" as an argument
// to your function, your callers will automagically convert bools, integers,
// and floating point values to strings for you.
//
// Conversion from 8-bit values is not accepted because if it were, then an
// attempt to pass ':' instead of ":" might result in a 58 ending up in your
// result.
//
// Bools convert to "0" or "1".
//
// Floating point values are converted to a string which, if passed to strtod(),
// would produce the exact same original double (except in case of NaN; all NaNs
// are considered the same value). We try to keep the string short but it's not
// guaranteed to be as short as possible.
//
// This class has implicit constructors.
// Style guide exception granted:
// http://goto/style-guide-exception-20978288
//
struct AlphaNum {
    StringPiece piece;
    char digits[kFastToBufferSize];

    // No bool ctor -- bools convert to an integral type.
    // A bool ctor would also convert incoming pointers (bletch).

    AlphaNum(int32 i32) // NOLINT(runtime/explicit)
            : piece(digits, FastInt32ToBufferLeft(i32, digits) - &digits[0]) {}
    AlphaNum(uint32 u32) // NOLINT(runtime/explicit)
            : piece(digits, FastUInt32ToBufferLeft(u32, digits) - &digits[0]) {}
    AlphaNum(int64 i64) // NOLINT(runtime/explicit)
            : piece(digits, FastInt64ToBufferLeft(i64, digits) - &digits[0]) {}
    AlphaNum(uint64 u64) // NOLINT(runtime/explicit)
            : piece(digits, FastUInt64ToBufferLeft(u64, digits) - &digits[0]) {}

#if defined(__APPLE__)
    AlphaNum(size_t size) // NOLINT(runtime/explicit)
            : piece(digits, FastUInt64ToBufferLeft(size, digits) - &digits[0]) {}
#endif

    AlphaNum(float f) // NOLINT(runtime/explicit)
            : piece(digits, strlen(FloatToBuffer(f, digits))) {}
    AlphaNum(double f) // NOLINT(runtime/explicit)
            : piece(digits, strlen(DoubleToBuffer(f, digits))) {}

    AlphaNum(const char* c_str) : piece(c_str) {}      // NOLINT(runtime/explicit)
    AlphaNum(StringPiece pc) : piece(std::move(pc)) {} // NOLINT(runtime/explicit)
    AlphaNum(const string& s) : piece(s) {}            // NOLINT(runtime/explicit)

    StringPiece::size_type size() const { return piece.size(); }
    const char* data() const { return piece.data(); }

private:
    // Use ":" not ':'
    AlphaNum(char c); // NOLINT(runtime/explicit)
};

extern AlphaNum gEmptyAlphaNum;

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or numbers, with no delimiter.  This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, StringPieces, strings, bool values,
//    and numeric values.
//
//    Don't use this for user-visible strings.  The localization process
//    works poorly on strings built up out of fragments.
//
//    For clarity and performance, don't use StrCat when appending to a
//    string.  In particular, avoid using any of these (anti-)patterns:
//      str.append(StrCat(...)
//      str += StrCat(...)
//      str = StrCat(str, ...)
//    where the last is the worse, with the potential to change a loop
//    from a linear time operation with O(1) dynamic allocations into a
//    quadratic time operation with O(n) dynamic allocations.  StrAppend
//    is a better choice than any of the above, subject to the restriction
//    of StrAppend(&str, a, b, c, ...) that none of the a, b, c, ... may
//    be a reference into str.
// ----------------------------------------------------------------------

string StrCat(const AlphaNum& a);
string StrCat(const AlphaNum& a, const AlphaNum& b);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f, const AlphaNum& g);
string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h);

namespace strings {
namespace internal {

// Do not call directly - this is not part of the public API.
string StrCatNineOrMore(const AlphaNum* a1, ...);

} // namespace internal
} // namespace strings

// Support 9 or more arguments
inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u, const AlphaNum& v) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, &v, null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u, const AlphaNum& v, const AlphaNum& w) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, &v, &w,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u, const AlphaNum& v, const AlphaNum& w, const AlphaNum& x) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, &v, &w, &x,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u, const AlphaNum& v, const AlphaNum& w, const AlphaNum& x,
                     const AlphaNum& y) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, &v, &w, &x, &y,
                                               null_alphanum);
}

inline string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
                     const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h,
                     const AlphaNum& i, const AlphaNum& j, const AlphaNum& k, const AlphaNum& l,
                     const AlphaNum& m, const AlphaNum& n, const AlphaNum& o, const AlphaNum& p,
                     const AlphaNum& q, const AlphaNum& r, const AlphaNum& s, const AlphaNum& t,
                     const AlphaNum& u, const AlphaNum& v, const AlphaNum& w, const AlphaNum& x,
                     const AlphaNum& y, const AlphaNum& z) {
    const AlphaNum* null_alphanum = NULL;
    return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j, &k, &l, &m,
                                               &n, &o, &p, &q, &r, &s, &t, &u, &v, &w, &x, &y, &z,
                                               null_alphanum);
}

// ----------------------------------------------------------------------
// StrAppend()
//    Same as above, but adds the output to the given string.
//    WARNING: For speed, StrAppend does not try to check each of its input
//    arguments to be sure that they are not a subset of the string being
//    appended to.  That is, while this will work:
//
//    string s = "foo";
//    s += s;
//
//    This will not (necessarily) work:
//
//    string s = "foo";
//    StrAppend(&s, s);
//
//    Note: while StrCat supports appending up to 12 arguments, StrAppend
//    is currently limited to 9.  That's rarely an issue except when
//    automatically transforming StrCat to StrAppend, and can easily be
//    worked around as consecutive calls to StrAppend are quite efficient.
// ----------------------------------------------------------------------

void StrAppend(string* dest, const AlphaNum& a);
void StrAppend(string* dest, const AlphaNum& a, const AlphaNum& b);
void StrAppend(string* dest, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c);
void StrAppend(string* dest, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
               const AlphaNum& d);

// Support up to 9 params by using a default empty AlphaNum.
void StrAppend(string* dest, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
               const AlphaNum& d, const AlphaNum& e, const AlphaNum& f = gEmptyAlphaNum,
               const AlphaNum& g = gEmptyAlphaNum, const AlphaNum& h = gEmptyAlphaNum,
               const AlphaNum& i = gEmptyAlphaNum);

#endif // STRINGS_STRCAT_H_
