// Copyright (c) 2006, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// ---
//
//
// ManualConstructor statically-allocates space in which to store some
// object, but does not initialize it.  You can then call the constructor
// and destructor for the object yourself as you see fit.  This is useful
// for memory management optimizations, where you want to initialize and
// destroy an object multiple times but only allocate it once.
//
// (When I say ManualConstructor statically allocates space, I mean that
// the ManualConstructor object itself is forced to be the right size.)
//
// For example usage, check out util/gtl/small_map.h.

#ifndef UTIL_GTL_MANUAL_CONSTRUCTOR_H_
#define UTIL_GTL_MANUAL_CONSTRUCTOR_H_

#include <stddef.h>

#include "gutil/port.h"

namespace base {

namespace util {
namespace gtl {
namespace internal {

//
// Provides a char array with the exact same alignment as another type. The
// first parameter must be a complete type, the second parameter is how many
// of that type to provide space for.
//
//   UTIL_GTL_ALIGNED_CHAR_ARRAY(struct stat, 16) storage_;
//
// Because MSVC and older GCCs require that the argument to their alignment
// construct to be a literal constant integer, we use a template instantiated
// at all the possible powers of two.
#ifndef SWIG
template<int alignment, int size> struct AlignType { };
template<int size> struct AlignType<0, size> { typedef char result[size]; };
#if defined(_MSC_VER)
#define UTIL_GTL_ALIGN_ATTRIBUTE(X) __declspec(align(X))
#define UTIL_GTL_ALIGN_OF(T) __alignof(T)
#elif defined(__GNUC__) || defined(__APPLE__) || defined(__INTEL_COMPILER) \
  || defined(__nacl__)
#define UTIL_GTL_ALIGN_ATTRIBUTE(X) __attribute__((aligned(X)))
#define UTIL_GTL_ALIGN_OF(T) __alignof__(T)
#endif

#if defined(UTIL_GTL_ALIGN_ATTRIBUTE)

#define UTIL_GTL_ALIGNTYPE_TEMPLATE(X) \
  template<int size> struct AlignType<X, size> { \
    typedef UTIL_GTL_ALIGN_ATTRIBUTE(X) char result[size]; \
  }

UTIL_GTL_ALIGNTYPE_TEMPLATE(1);
UTIL_GTL_ALIGNTYPE_TEMPLATE(2);
UTIL_GTL_ALIGNTYPE_TEMPLATE(4);
UTIL_GTL_ALIGNTYPE_TEMPLATE(8);
UTIL_GTL_ALIGNTYPE_TEMPLATE(16);
UTIL_GTL_ALIGNTYPE_TEMPLATE(32);
UTIL_GTL_ALIGNTYPE_TEMPLATE(64);
UTIL_GTL_ALIGNTYPE_TEMPLATE(128);
UTIL_GTL_ALIGNTYPE_TEMPLATE(256);
UTIL_GTL_ALIGNTYPE_TEMPLATE(512);
UTIL_GTL_ALIGNTYPE_TEMPLATE(1024);
UTIL_GTL_ALIGNTYPE_TEMPLATE(2048);
UTIL_GTL_ALIGNTYPE_TEMPLATE(4096);
UTIL_GTL_ALIGNTYPE_TEMPLATE(8192);
// Any larger and MSVC++ will complain.

#define UTIL_GTL_ALIGNED_CHAR_ARRAY(T, Size) \
  typename util::gtl::internal::AlignType<UTIL_GTL_ALIGN_OF(T), \
                                          sizeof(T) * Size>::result

#undef UTIL_GTL_ALIGNTYPE_TEMPLATE
#undef UTIL_GTL_ALIGN_ATTRIBUTE

#else  // defined(UTIL_GTL_ALIGN_ATTRIBUTE)
#error "You must define UTIL_GTL_ALIGNED_CHAR_ARRAY for your compiler."
#endif  // defined(UTIL_GTL_ALIGN_ATTRIBUTE)

#else  // !SWIG

// SWIG can't represent alignment and doesn't care about alignment on data
// members (it works fine without it).
template<typename Size>
struct AlignType { typedef char result[Size]; };
#define UTIL_GTL_ALIGNED_CHAR_ARRAY(T, Size) \
    util::gtl::internal::AlignType<Size * sizeof(T)>::result

// Enough to parse with SWIG, will never be used by running code.
#define UTIL_GTL_ALIGN_OF(Type) 16

#endif  // !SWIG

}  // namespace internal
}  // namespace gtl
}  // namespace util

template <typename Type>
class ManualConstructor {
 public:
  // No constructor or destructor because one of the most useful uses of
  // this class is as part of a union, and members of a union cannot have
  // constructors or destructors.  And, anyway, the whole point of this
  // class is to bypass these.

  // Support users creating arrays of ManualConstructor<>s.  This ensures that
  // the array itself has the correct alignment.
  static void* operator new[](size_t size) {
    return aligned_malloc(size, UTIL_GTL_ALIGN_OF(Type));
  }
  static void operator delete[](void* mem) {
    aligned_free(mem);
  }

  inline Type* get() {
    return reinterpret_cast<Type*>(space_);
  }
  inline const Type* get() const  {
    return reinterpret_cast<const Type*>(space_);
  }

  inline Type* operator->() { return get(); }
  inline const Type* operator->() const { return get(); }

  inline Type& operator*() { return *get(); }
  inline const Type& operator*() const { return *get(); }

  // You can pass up to four constructor arguments as arguments of Init().
  inline void Init() {
    new(space_) Type;
  }

  template <typename T1>
  inline void Init(const T1& p1) {
    new(space_) Type(p1);
  }

  template <typename T1, typename T2>
  inline void Init(const T1& p1, const T2& p2) {
    new(space_) Type(p1, p2);
  }

  template <typename T1, typename T2, typename T3>
  inline void Init(const T1& p1, const T2& p2, const T3& p3) {
    new(space_) Type(p1, p2, p3);
  }

  template <typename T1, typename T2, typename T3, typename T4>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4) {
    new(space_) Type(p1, p2, p3, p4);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5) {
    new(space_) Type(p1, p2, p3, p4, p5);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6) {
    new(space_) Type(p1, p2, p3, p4, p5, p6);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6, typename T7>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6, const T7& p7) {
    new(space_) Type(p1, p2, p3, p4, p5, p6, p7);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6, typename T7, typename T8>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6, const T7& p7, const T8& p8) {
    new(space_) Type(p1, p2, p3, p4, p5, p6, p7, p8);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6, typename T7, typename T8, typename T9>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6, const T7& p7, const T8& p8,
                   const T9& p9) {
    new(space_) Type(p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6, typename T7, typename T8, typename T9, typename T10>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6, const T7& p7, const T8& p8,
                   const T9& p9, const T10& p10) {
    new(space_) Type(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10);
  }

  template <typename T1, typename T2, typename T3, typename T4, typename T5,
            typename T6, typename T7, typename T8, typename T9, typename T10,
            typename T11>
  inline void Init(const T1& p1, const T2& p2, const T3& p3, const T4& p4,
                   const T5& p5, const T6& p6, const T7& p7, const T8& p8,
                   const T9& p9, const T10& p10, const T11& p11) {
    new(space_) Type(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11);
  }

  inline void Destroy() {
    get()->~Type();
  }

 private:
  UTIL_GTL_ALIGNED_CHAR_ARRAY(Type, 1) space_;
};

#undef UTIL_GTL_ALIGNED_CHAR_ARRAY
#undef UTIL_GTL_ALIGN_OF

}

#endif  // UTIL_GTL_MANUAL_CONSTRUCTOR_H_
