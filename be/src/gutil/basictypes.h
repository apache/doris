// Copyright 2001 - 2003 Google, Inc.
//
// Google-specific types

#ifndef BASE_BASICTYPES_H_
#define BASE_BASICTYPES_H_

#include "gutil/integral_types.h"
#include "gutil/macros.h"

// Argument type used in interfaces that can optionally take ownership
// of a passed in argument.  If TAKE_OWNERSHIP is passed, the called
// object takes ownership of the argument.  Otherwise it does not.
enum Ownership { DO_NOT_TAKE_OWNERSHIP, TAKE_OWNERSHIP };

// Used to explicitly mark the return value of a function as unused. If you are
// really sure you don't want to do anything with the return value of a function
// that has been marked WARN_UNUSED_RESULT, wrap it with this. Example:
//
//   scoped_ptr<MyType> my_var = ...;
//   if (TakeOwnership(my_var.get()) == SUCCESS)
//     ignore_result(my_var.release());
//
template <typename T>
inline void ignore_result(const T&) {}

#endif // BASE_BASICTYPES_H_
