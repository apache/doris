// Copyright 2007 Google Inc. All Rights Reserved.
//
// Character classification functions similar to standard <ctype.h>.
// Some C++ implementations provide locale-sensitive implementations
// of some <ctype.h> functions.  These ascii_* functions are
// hard-wired for ASCII.  Hard-wired for ASCII is much faster.
//
// ascii_isalnum, ascii_isalpha, ascii_isascii, ascii_isblank,
// ascii_iscntrl, ascii_isdigit, ascii_isgraph, ascii_islower,
// ascii_isprint, ascii_ispunct, ascii_isspace, ascii_isupper,
// ascii_isxdigit
//   Similar to the <ctype.h> functions with similar names.
//   Input parameter is an unsigned char.  Return value is a bool.
//   If the input has a numerical value greater than 127
//   then the output is "false".
//
// ascii_tolower, ascii_toupper
//   Similar to the <ctype.h> functions with similar names.
//   Input parameter is an unsigned char.  Return value is a char.
//   If the input is not an ascii {lower,upper}-case letter
//   (including numerical values greater than 127)
//   then the output is the same as the input.

#pragma once

// Array of character information.  This is an implementation detail.
// The individual bits do not have names because the array definition is
// already tightly coupled to these functions.  Names would just make it
// harder to read and debug.

#define kApb kAsciiPropertyBits
extern const unsigned char kAsciiPropertyBits[256];

// Public functions.

static inline bool ascii_isalpha(unsigned char c) {
    return kApb[c] & 0x01;
}
static inline bool ascii_isalnum(unsigned char c) {
    return kApb[c] & 0x04;
}
static inline bool ascii_isspace(unsigned char c) {
    return kApb[c] & 0x08;
}
static inline bool ascii_ispunct(unsigned char c) {
    return kApb[c] & 0x10;
}
static inline bool ascii_isblank(unsigned char c) {
    return kApb[c] & 0x20;
}
static inline bool ascii_iscntrl(unsigned char c) {
    return kApb[c] & 0x40;
}
static inline bool ascii_isxdigit(unsigned char c) {
    return kApb[c] & 0x80;
}

static inline bool ascii_isdigit(unsigned char c) {
    return c >= '0' && c <= '9';
}

static inline bool ascii_isprint(unsigned char c) {
    return c >= 32 && c < 127;
}

static inline bool ascii_isgraph(unsigned char c) {
    return c > 32 && c < 127;
}

static inline bool ascii_isupper(unsigned char c) {
    return c >= 'A' && c <= 'Z';
}

static inline bool ascii_islower(unsigned char c) {
    return c >= 'a' && c <= 'z';
}

static inline bool ascii_isascii(unsigned char c) {
    return c < 128;
}
#undef kApb

extern const unsigned char kAsciiToLower[256];
static inline char ascii_tolower(unsigned char c) {
    return kAsciiToLower[c];
}
extern const unsigned char kAsciiToUpper[256];
static inline char ascii_toupper(unsigned char c) {
    return kAsciiToUpper[c];
}
