// Copyright 2008 Google Inc. All Rights Reserved.
//
// Various Google-specific macros.
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//

#pragma once

#include <stddef.h> // For size_t

#include "butil/macros.h"
#include "gutil/port.h"

// Macro that allows definition of a variable appended with the current line
// number in the source file. Typically for use by other macros to allow the
// user to declare multiple variables with the same "base" name inside the same
// lexical block.
#define VARNAME_LINENUM(varname) VARNAME_LINENUM_INTERNAL(varname##_L, __LINE__)
#define VARNAME_LINENUM_INTERNAL(v, line) VARNAME_LINENUM_INTERNAL2(v, line)
#define VARNAME_LINENUM_INTERNAL2(v, line) v##line

// Retry on EINTR for functions like read() that return -1 on error.
#define RETRY_ON_EINTR(err, expr)                                                              \
    do {                                                                                       \
        static_assert(std::is_signed<decltype(err)>::value, #err " must be a signed integer"); \
        (err) = (expr);                                                                        \
    } while ((err) == -1 && errno == EINTR)
