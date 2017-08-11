// Copyright 2005 Google Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// ---
//

#ifndef UTIL_GTL_FIXEDARRAY_H__
#define UTIL_GTL_FIXEDARRAY_H__

#include <stddef.h>

#include <common/logging.h>

#include "gutil/logging-inl.h"
#include "gutil/macros.h"
#include "gutil/manual_constructor.h"

// A FixedArray<T> represents a non-resizable array of T where the
// length of the array does not need to be a compile time constant.
//
// FixedArray allocates small arrays inline, and large arrays on
// the heap.  It is a good replacement for non-standard and deprecated
// uses of alloca() and variable length arrays (a GCC extension).
//
// FixedArray keeps performance fast for small arrays, because it
// avoids heap operations.  It also helps reduce the chances of
// accidentally overflowing your stack if large input is passed to
// your function.
//
// Also, FixedArray is useful for writing portable code.  Not all
// compilers support arrays of dynamic size.

// Most users should not specify an inline_elements argument and let
// FixedArray<> automatically determine the number of elements
// to store inline based on sizeof(T).
//
// If inline_elements is specified, the FixedArray<> implementation
// will store arrays of length <= inline_elements inline.
//
// Finally note that unlike vector<T> FixedArray<T> will not zero-initialize
// simple types like int, double, bool, etc.
//
// Non-POD types will be default-initialized just like regular vectors or
// arrays.

template <typename T, ssize_t inline_elements = -1>
class FixedArray {
 public:
  // For playing nicely with stl:
  typedef T value_type;
  typedef T* iterator;
  typedef T const* const_iterator;
  typedef T& reference;
  typedef T const& const_reference;
  typedef T* pointer;
  typedef std::ptrdiff_t difference_type;
  typedef size_t size_type;

  // REQUIRES: n >= 0
  // Creates an array object that can store "n" elements.
  //
  // FixedArray<T> will not zero-initialiaze POD (simple) types like int,
  // double, bool, etc.
  // Non-POD types will be default-initialized just like regular vectors or
  // arrays.
  explicit FixedArray(size_type n);

  // Releases any resources.
  ~FixedArray();

  // Returns the length of the array.
  inline size_type size() const { return size_; }

  // Returns the memory size of the array in bytes.
  inline size_t memsize() const { return size_ * sizeof(T); }

  // Returns a pointer to the underlying element array.
  inline const T* get() const { return reinterpret_cast<T *>(array_); }
  inline T* get() { return reinterpret_cast<T *>(array_); }

  // REQUIRES: 0 <= i < size()
  // Returns a reference to the "i"th element.
  inline T& operator[](size_type i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, size_);
    return array_[i].element;
  }

  // REQUIRES: 0 <= i < size()
  // Returns a reference to the "i"th element.
  inline const T& operator[](size_type i) const {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, size_);
    return array_[i].element;
  }

  inline iterator begin() { return get(); }
  inline iterator end() { return get() + size_; }

  inline const_iterator begin() const { return get(); }
  inline const_iterator end() const { return get() + size_; }

 private:
  // Container to hold elements of type T.  This is necessary to handle
  // the case where T is a a (C-style) array.  The size of InnerContainer
  // and T must be the same, otherwise callers' assumptions about use
  // of this code will be broken.
  struct InnerContainer {
    T element;
  };
  COMPILE_ASSERT(sizeof(InnerContainer) == sizeof(T),
                 fixedarray_inner_container_size_mismatch);

  // How many elements should we store inline?
  //   a. If not specified, use a default of 256 bytes (256 bytes
  //      seems small enough to not cause stack overflow or unnecessary
  //      stack pollution, while still allowing stack allocation for
  //      reasonably long character arrays.
  //   b. Never use 0 length arrays (not ISO C++)
  static const size_type S1 = ((inline_elements < 0)
                               ? (256/sizeof(T)) : inline_elements);
  static const size_type S2 = (S1 <= 0) ? 1 : S1;
  static const size_type kInlineElements = S2;

  size_type const       size_;
  InnerContainer* const array_;

  // Allocate some space, not an array of elements of type T, so that we can
  // skip calling the T constructors and destructors for space we never use.
  base::ManualConstructor<InnerContainer>
      inline_space_[kInlineElements];

  DISALLOW_EVIL_CONSTRUCTORS(FixedArray);
};

// Implementation details follow

template <class T, ssize_t S>
inline FixedArray<T, S>::FixedArray(typename FixedArray<T, S>::size_type n)
    : size_(n),
      array_((n <= kInlineElements
              ? reinterpret_cast<InnerContainer*>(inline_space_)
              : new InnerContainer[n])) {
  DCHECK_GE(n, 0);

  // Construct only the elements actually used.
  if (array_ == reinterpret_cast<InnerContainer*>(inline_space_)) {
    for (int i = 0; i != size_; ++i) {
      inline_space_[i].Init();
    }
  }
}

template <class T, ssize_t S>
inline FixedArray<T, S>::~FixedArray() {
  if (array_ != reinterpret_cast<InnerContainer*>(inline_space_)) {
    delete[] array_;
  } else {
    for (int i = 0; i != size_; ++i) {
      inline_space_[i].Destroy();
    }
  }
}

#endif  // UTIL_GTL_FIXEDARRAY_H__
