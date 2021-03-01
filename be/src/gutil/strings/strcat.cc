// Copyright 2008 and onwards Google Inc.  All rights reserved.

#include "gutil/strings/strcat.h"

#include <common/logging.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/stl_util.h"
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/escaping.h"

AlphaNum gEmptyAlphaNum("");

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or integers, with no delimiter.  This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, StringPieces, strings, and integer values.
// ----------------------------------------------------------------------

// Append is merely a version of memcpy that returns the address of the byte
// after the area just overwritten.  It comes in multiple flavors to minimize
// call overhead.
static char* Append1(char* out, const AlphaNum& x) {
    memcpy(out, x.data(), x.size());
    return out + x.size();
}

static char* Append2(char* out, const AlphaNum& x1, const AlphaNum& x2) {
    memcpy(out, x1.data(), x1.size());
    out += x1.size();

    memcpy(out, x2.data(), x2.size());
    return out + x2.size();
}

static char* Append4(char* out, const AlphaNum& x1, const AlphaNum& x2, const AlphaNum& x3,
                     const AlphaNum& x4) {
    memcpy(out, x1.data(), x1.size());
    out += x1.size();

    memcpy(out, x2.data(), x2.size());
    out += x2.size();

    memcpy(out, x3.data(), x3.size());
    out += x3.size();

    memcpy(out, x4.data(), x4.size());
    return out + x4.size();
}

string StrCat(const AlphaNum& a) {
    return string(a.data(), a.size());
}

string StrCat(const AlphaNum& a, const AlphaNum& b) {
    string result;
    STLStringResizeUninitialized(&result, a.size() + b.size());
    char* const begin = &*result.begin();
    char* out = Append2(begin, a, b);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c) {
    string result;
    STLStringResizeUninitialized(&result, a.size() + b.size() + c.size());
    char* const begin = &*result.begin();
    char* out = Append2(begin, a, b);
    out = Append1(out, c);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d) {
    string result;
    STLStringResizeUninitialized(&result, a.size() + b.size() + c.size() + d.size());
    char* const begin = &*result.begin();
    char* out = Append4(begin, a, b, c, d);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e) {
    string result;
    STLStringResizeUninitialized(&result, a.size() + b.size() + c.size() + d.size() + e.size());
    char* const begin = &*result.begin();
    char* out = Append4(begin, a, b, c, d);
    out = Append1(out, e);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f) {
    string result;
    STLStringResizeUninitialized(&result,
                                 a.size() + b.size() + c.size() + d.size() + e.size() + f.size());
    char* const begin = &*result.begin();
    char* out = Append4(begin, a, b, c, d);
    out = Append2(out, e, f);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f, const AlphaNum& g) {
    string result;
    STLStringResizeUninitialized(
            &result, a.size() + b.size() + c.size() + d.size() + e.size() + f.size() + g.size());
    char* const begin = &*result.begin();
    char* out = Append4(begin, a, b, c, d);
    out = Append2(out, e, f);
    out = Append1(out, g);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

string StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c, const AlphaNum& d,
              const AlphaNum& e, const AlphaNum& f, const AlphaNum& g, const AlphaNum& h) {
    string result;
    STLStringResizeUninitialized(&result, a.size() + b.size() + c.size() + d.size() + e.size() +
                                                  f.size() + g.size() + h.size());
    char* const begin = &*result.begin();
    char* out = Append4(begin, a, b, c, d);
    out = Append4(out, e, f, g, h);
    DCHECK_EQ(out, begin + result.size());
    return result;
}

namespace strings {
namespace internal {

// StrCat with this many params is exceedingly rare, but it has been
// requested...  therefore we'll rely on default arguments to make calling
// slightly less efficient, to preserve code size.
string StrCatNineOrMore(const AlphaNum* a, ...) {
    string result;

    va_list args;
    va_start(args, a);
    size_t size = a->size();
    while (const AlphaNum* arg = va_arg(args, const AlphaNum*)) {
        size += arg->size();
    }
    STLStringResizeUninitialized(&result, size);
    va_end(args);
    va_start(args, a);
    char* const begin = &*result.begin();
    char* out = Append1(begin, *a);
    while (const AlphaNum* arg = va_arg(args, const AlphaNum*)) {
        out = Append1(out, *arg);
    }
    va_end(args);
    DCHECK_EQ(out, begin + size);
    return result;
}

} // namespace internal
} // namespace strings

// It's possible to call StrAppend with a StringPiece that is itself a fragment
// of the string we're appending to.  However the results of this are random.
// Therefore, check for this in debug mode.  Use unsigned math so we only have
// to do one comparison.
#define DCHECK_NO_OVERLAP(dest, src) \
    DCHECK_GT(uintptr_t((src).data() - (dest).data()), uintptr_t((dest).size()))

void StrAppend(string* result, const AlphaNum& a) {
    DCHECK_NO_OVERLAP(*result, a);
    result->append(a.data(), a.size());
}

void StrAppend(string* result, const AlphaNum& a, const AlphaNum& b) {
    DCHECK_NO_OVERLAP(*result, a);
    DCHECK_NO_OVERLAP(*result, b);
    string::size_type old_size = result->size();
    STLStringResizeUninitialized(result, old_size + a.size() + b.size());
    char* const begin = &*result->begin();
    char* out = Append2(begin + old_size, a, b);
    DCHECK_EQ(out, begin + result->size());
}

void StrAppend(string* result, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c) {
    DCHECK_NO_OVERLAP(*result, a);
    DCHECK_NO_OVERLAP(*result, b);
    DCHECK_NO_OVERLAP(*result, c);
    string::size_type old_size = result->size();
    STLStringResizeUninitialized(result, old_size + a.size() + b.size() + c.size());
    char* const begin = &*result->begin();
    char* out = Append2(begin + old_size, a, b);
    out = Append1(out, c);
    DCHECK_EQ(out, begin + result->size());
}

void StrAppend(string* result, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
               const AlphaNum& d) {
    DCHECK_NO_OVERLAP(*result, a);
    DCHECK_NO_OVERLAP(*result, b);
    DCHECK_NO_OVERLAP(*result, c);
    DCHECK_NO_OVERLAP(*result, d);
    string::size_type old_size = result->size();
    STLStringResizeUninitialized(result, old_size + a.size() + b.size() + c.size() + d.size());
    char* const begin = &*result->begin();
    char* out = Append4(begin + old_size, a, b, c, d);
    DCHECK_EQ(out, begin + result->size());
}

// StrAppend with this many params is even rarer than with StrCat.
// Therefore we'll again rely on default arguments to make calling
// slightly less efficient, to preserve code size.
void StrAppend(string* result, const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
               const AlphaNum& d, const AlphaNum& e, const AlphaNum& f, const AlphaNum& g,
               const AlphaNum& h, const AlphaNum& i) {
    DCHECK_NO_OVERLAP(*result, a);
    DCHECK_NO_OVERLAP(*result, b);
    DCHECK_NO_OVERLAP(*result, c);
    DCHECK_NO_OVERLAP(*result, d);
    DCHECK_NO_OVERLAP(*result, e);
    DCHECK_NO_OVERLAP(*result, f);
    DCHECK_NO_OVERLAP(*result, g);
    DCHECK_NO_OVERLAP(*result, h);
    DCHECK_NO_OVERLAP(*result, i);
    string::size_type old_size = result->size();
    STLStringResizeUninitialized(result, old_size + a.size() + b.size() + c.size() + d.size() +
                                                 e.size() + f.size() + g.size() + h.size() +
                                                 i.size());
    char* const begin = &*result->begin();
    char* out = Append4(begin + old_size, a, b, c, d);
    out = Append4(out, e, f, g, h);
    out = Append1(out, i);
    DCHECK_EQ(out, begin + result->size());
}
