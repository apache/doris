// Copyright 2008 Google Inc. All Rights Reserved.
//
// Various Google-specific macros.
//
// This code is compiled directly on many platforms, including client
// platforms like Windows, Mac, and embedded systems.  Before making
// any changes here, make sure that you're not breaking any platforms.
//

#ifndef BASE_MACROS_H_
#define BASE_MACROS_H_

#include <stddef.h> // For size_t

#include "gutil/port.h"

// The swigged version of an abstract class must be concrete if any methods
// return objects of the abstract type. We keep it abstract in C++ and
// concrete for swig.
#ifndef SWIG
#define ABSTRACT = 0
#endif

// The COMPILE_ASSERT macro can be used to verify that a compile time
// expression is true. For example, you could use it to verify the
// size of a static array:
//
//   COMPILE_ASSERT(ARRAYSIZE(content_type_names) == CONTENT_NUM_TYPES,
//                  content_type_names_incorrect_size);
//
// or to make sure a struct is smaller than a certain size:
//
//   COMPILE_ASSERT(sizeof(foo) < 128, foo_too_large);
//
// The second argument to the macro is the name of the variable. If
// the expression is false, most compilers will issue a warning/error
// containing the name of the variable.

template <bool>
struct CompileAssert {};

#ifndef COMPILE_ASSERT

#define COMPILE_ASSERT(expr, msg) \
    typedef CompileAssert<(bool(expr))> msg[bool(expr) ? 1 : -1] ATTRIBUTE_UNUSED

// Implementation details of COMPILE_ASSERT:
//
// - COMPILE_ASSERT works by defining an array type that has -1
//   elements (and thus is invalid) when the expression is false.
//
// - The simpler definition
//
//     #define COMPILE_ASSERT(expr, msg) typedef char msg[(expr) ? 1 : -1]
//
//   does not work, as gcc supports variable-length arrays whose sizes
//   are determined at run-time (this is gcc's extension and not part
//   of the C++ standard).  As a result, gcc fails to reject the
//   following code with the simple definition:
//
//     int foo;
//     COMPILE_ASSERT(foo, msg); // not supposed to compile as foo is
//                               // not a compile-time constant.
//
// - By using the type CompileAssert<(bool(expr))>, we ensures that
//   expr is a compile-time constant.  (Template arguments must be
//   determined at compile-time.)
//
// - The outer parentheses in CompileAssert<(bool(expr))> are necessary
//   to work around a bug in gcc 3.4.4 and 4.0.1.  If we had written
//
//     CompileAssert<bool(expr)>
//
//   instead, these compilers will refuse to compile
//
//     COMPILE_ASSERT(5 > 0, some_message);
//
//   (They seem to think the ">" in "5 > 0" marks the end of the
//   template argument list.)
//
// - The array size is (bool(expr) ? 1 : -1), instead of simply
//
//     ((expr) ? 1 : -1).
//
//   This is to avoid running into a bug in MS VC 7.1, which
//   causes ((0.0) ? 1 : -1) to incorrectly evaluate to 1.
#endif // COMPILE_ASSERT

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
//
// For disallowing only assign or copy, write the code directly, but declare
// the intend in a comment, for example:
// void operator=(const TypeName&);  // DISALLOW_ASSIGN
// Note, that most uses of DISALLOW_ASSIGN and DISALLOW_COPY are broken
// semantically, one should either use disallow both or neither. Try to
// avoid these in new code.
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;    \
    void operator=(const TypeName&) = delete
#endif

// An older, politically incorrect name for the above.
// Prefer DISALLOW_COPY_AND_ASSIGN for new code.
#define DISALLOW_EVIL_CONSTRUCTORS(TypeName) DISALLOW_COPY_AND_ASSIGN(TypeName)

// A macro to disallow all the implicit constructors, namely the
// default constructor, copy constructor and operator= functions.
//
// This should be used in the private: declarations for a class
// that wants to prevent anyone from instantiating it. This is
// especially useful for classes containing only static methods.
#define DISALLOW_IMPLICIT_CONSTRUCTORS(TypeName) \
    TypeName() = delete;                         \
    DISALLOW_COPY_AND_ASSIGN(TypeName)

// The arraysize(arr) macro returns the # of elements in an array arr.
// The expression is a compile-time constant, and therefore can be
// used in defining new arrays, for example.  If you use arraysize on
// a pointer by mistake, you will get a compile-time error.
//
// One caveat is that, for C++03, arraysize() doesn't accept any array of
// an anonymous type or a type defined inside a function.  In these rare
// cases, you have to use the unsafe ARRAYSIZE() macro below.  This is
// due to a limitation in C++03's template system.  The limitation has
// been removed in C++11.

// This template function declaration is used in defining arraysize.
// Note that the function doesn't need an implementation, as we only
// use its type.
template <typename T, size_t N>
char (&ArraySizeHelper(T (&array)[N]))[N];

// That gcc wants both of these prototypes seems mysterious. VC, for
// its part, can't decide which to use (another mystery). Matching of
// template overloads: the final frontier.
#ifndef _MSC_VER
template <typename T, size_t N>
char (&ArraySizeHelper(const T (&array)[N]))[N];
#endif

#define arraysize(array) (sizeof(ArraySizeHelper(array)))

// ARRAYSIZE performs essentially the same calculation as arraysize,
// but can be used on anonymous types or types defined inside
// functions.  It's less safe than arraysize as it accepts some
// (although not all) pointers.  Therefore, you should use arraysize
// whenever possible.
//
// The expression ARRAYSIZE(a) is a compile-time constant of type
// size_t.
//
// ARRAYSIZE catches a few type errors.  If you see a compiler error
//
//   "warning: division by zero in ..."
//
// when using ARRAYSIZE, you are (wrongfully) giving it a pointer.
// You should only use ARRAYSIZE on statically allocated arrays.
//
// The following comments are on the implementation details, and can
// be ignored by the users.
//
// ARRAYSIZE(arr) works by inspecting sizeof(arr) (the # of bytes in
// the array) and sizeof(*(arr)) (the # of bytes in one array
// element).  If the former is divisible by the latter, perhaps arr is
// indeed an array, in which case the division result is the # of
// elements in the array.  Otherwise, arr cannot possibly be an array,
// and we generate a compiler error to prevent the code from
// compiling.
//
// Since the size of bool is implementation-defined, we need to cast
// !(sizeof(a) & sizeof(*(a))) to size_t in order to ensure the final
// result has type size_t.
//
// This macro is not perfect as it wrongfully accepts certain
// pointers, namely where the pointer size is divisible by the pointee
// size.  For code that goes through a 32-bit compiler, where a pointer
// is 4 bytes, this means all pointers to a type whose size is 3 or
// greater than 4 will be (righteously) rejected.
//
// Kudos to Jorg Brown for this simple and elegant implementation.
//
// - wan 2005-11-16
//
// Starting with Visual C++ 2005, WinNT.h includes ARRAYSIZE.
#if !defined(_MSC_VER) || (defined(_MSC_VER) && _MSC_VER < 1400)
#define ARRAYSIZE(a) ((sizeof(a) / sizeof(*(a))) / static_cast<size_t>(!(sizeof(a) % sizeof(*(a)))))
#endif

// A macro to turn a symbol into a string
#define AS_STRING(x) AS_STRING_INTERNAL(x)
#define AS_STRING_INTERNAL(x) #x

// Macro that allows definition of a variable appended with the current line
// number in the source file. Typically for use by other macros to allow the
// user to declare multiple variables with the same "base" name inside the same
// lexical block.
#define VARNAME_LINENUM(varname) VARNAME_LINENUM_INTERNAL(varname##_L, __LINE__)
#define VARNAME_LINENUM_INTERNAL(v, line) VARNAME_LINENUM_INTERNAL2(v, line)
#define VARNAME_LINENUM_INTERNAL2(v, line) v##line

// The following enum should be used only as a constructor argument to indicate
// that the variable has static storage class, and that the constructor should
// do nothing to its state.  It indicates to the reader that it is legal to
// declare a static instance of the class, provided the constructor is given
// the base::LINKER_INITIALIZED argument.  Normally, it is unsafe to declare a
// static variable that has a constructor or a destructor because invocation
// order is undefined.  However, IF the type can be initialized by filling with
// zeroes (which the loader does for static variables), AND the type's
// destructor does nothing to the storage, then a constructor for static
// initialization can be declared as
//       explicit MyClass(base::LinkerInitialized x) {}
// and invoked as
//       static MyClass my_variable_name(base::LINKER_INITIALIZED);
namespace base {
enum LinkerInitialized { LINKER_INITIALIZED };
}

// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels:
//  switch (x) {
//    case 40:
//    case 41:
//      if (truth_is_out_there) {
//        ++x;
//        FALLTHROUGH_INTENDED;  // Use instead of/along with annotations in
//                               // comments.
//      } else {
//        return x;
//      }
//    case 42:
//      ...
//
//  As shown in the example above, the FALLTHROUGH_INTENDED macro should be
//  followed by a semicolon. It is designed to mimic control-flow statements
//  like 'break;', so it can be placed in most places where 'break;' can, but
//  only if there are no statements on the execution path between it and the
//  next switch label.
//
//  When compiled with clang in C++11 mode, the FALLTHROUGH_INTENDED macro is
//  expanded to [[clang::fallthrough]] attribute, which is analysed when
//  performing switch labels fall-through diagnostic ('-Wimplicit-fallthrough').
//  See clang documentation on language extensions for details:
//  http://clang.llvm.org/docs/LanguageExtensions.html#clang__fallthrough
//
//  When used with unsupported compilers, the FALLTHROUGH_INTENDED macro has no
//  effect on diagnostics.
//
//  In either case this macro has no effect on runtime behavior and performance
//  of code.
#if defined(__clang__) && defined(LANG_CXX11) && defined(__has_warning)
#if __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
#define FALLTHROUGH_INTENDED [[clang::fallthrough]] // NOLINT
#endif
#endif

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
    do {                     \
    } while (0)
#endif

// Retry on EINTR for functions like read() that return -1 on error.
#define RETRY_ON_EINTR(err, expr)                                                              \
    do {                                                                                       \
        static_assert(std::is_signed<decltype(err)>::value, #err " must be a signed integer"); \
        (err) = (expr);                                                                        \
    } while ((err) == -1 && errno == EINTR)

// Same as above but for stream API calls like fread() and fwrite().
#define STREAM_RETRY_ON_EINTR(nread, stream, expr)                      \
    do {                                                                \
        static_assert(std::is_unsigned<decltype(nread)>::value == true, \
                      #nread " must be an unsigned integer");           \
        (nread) = (expr);                                               \
    } while ((nread) == 0 && ferror(stream) == EINTR)

// Same as above but for functions that return pointer types (like
// fopen() and freopen()).
#define POINTER_RETRY_ON_EINTR(ptr, expr)                                                        \
    do {                                                                                         \
        static_assert(std::is_pointer<decltype(ptr)>::value == true, #ptr " must be a pointer"); \
        (ptr) = (expr);                                                                          \
    } while ((ptr) == nullptr && errno == EINTR)

#endif // BASE_MACROS_H_
