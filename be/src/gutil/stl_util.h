// Copyright 2002 Google Inc.
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
//
// STL utility functions.  Usually, these replace built-in, but slow(!),
// STL functions with more efficient versions or provide a more convenient
// and Google friendly API.
//

#pragma once

#include <stddef.h>
#include <string.h> // for memcpy

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <cassert>
#include <deque>
using std::deque;
#include <functional>
using std::less;
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <memory>
#include <string>
using std::string;
#include <vector>
using std::vector;

#include "gutil/integral_types.h"
#include "gutil/macros.h"
#include "gutil/port.h"

// Sort and remove duplicates of an STL vector or deque.
template <class T>
void STLSortAndRemoveDuplicates(T* v) {
    sort(v->begin(), v->end());
    v->erase(unique(v->begin(), v->end()), v->end());
}

// Clear internal memory of an STL object.
// STL clear()/reserve(0) does not always free internal memory allocated
// This function uses swap/destructor to ensure the internal memory is freed.
template <class T>
void STLClearObject(T* obj) {
    T tmp;
    tmp.swap(*obj);
    obj->reserve(0); // this is because sometimes "T tmp" allocates objects with
                     // memory (arena implementation?).  use reserve()
                     // to clear() even if it doesn't always work
}

// Specialization for deque. Same as STLClearObject but doesn't call reserve
// since deque doesn't have reserve.
template <class T, class A>
void STLClearObject(deque<T, A>* obj) {
    deque<T, A> tmp;
    tmp.swap(*obj);
}

// Reduce memory usage on behalf of object if its capacity is greater
// than or equal to "limit", which defaults to 2^20.
template <class T>
 void STLClearIfBig(T* obj, size_t limit = 1 << 20) {
    if (obj->capacity() >= limit) {
        STLClearObject(obj);
    } else {
        obj->clear();
    }
}

// Specialization for deque, which doesn't implement capacity().
template <class T, class A>
 void STLClearIfBig(deque<T, A>* obj, size_t limit = 1 << 20) {
    if (obj->size() >= limit) {
        STLClearObject(obj);
    } else {
        obj->clear();
    }
}

// Reduce the number of buckets in a hash_set or hash_map back to the
// default if the current number of buckets is "limit" or more.
//
// Suppose you repeatedly fill and clear a hash_map or hash_set.  If
// you ever insert a lot of items, then your hash table will have lots
// of buckets thereafter.  (The number of buckets is not reduced when
// the table is cleared.)  Having lots of buckets is good if you
// insert comparably many items in every iteration, because you'll
// reduce collisions and table resizes.  But having lots of buckets is
// bad if you insert few items in most subsequent iterations, because
// repeatedly clearing out all those buckets can get expensive.
//
// One solution is to call STLClearHashIfBig() with a "limit" value
// that is a small multiple of the typical number of items in your
// table.  In the common case, this is equivalent to an ordinary
// clear.  In the rare case where you insert a lot of items, the
// number of buckets is reset to the default to keep subsequent clear
// operations cheap.  Note that the default number of buckets is 193
// in the Gnu library implementation as of Jan '08.
template <class T>
 void STLClearHashIfBig(T* obj, size_t limit) {
    if (obj->bucket_count() >= limit) {
        T tmp;
        tmp.swap(*obj);
    } else {
        obj->clear();
    }
}

// Reserve space for STL object.
// STL's reserve() will always copy.
// This function avoid the copy if we already have capacity
template <class T>
void STLReserveIfNeeded(T* obj, int new_size) {
    if (obj->capacity() < new_size) // increase capacity
        obj->reserve(new_size);
    else if (obj->size() > new_size) // reduce size
        obj->resize(new_size);
}

// STLDeleteContainerPointers()
//  For a range within a container of pointers, calls delete
//  (non-array version) on these pointers.
// NOTE: for these three functions, we could just implement a DeleteObject
// functor and then call for_each() on the range and functor, but this
// requires us to pull in all of <algorithm>, which seems expensive.
// For hash_[multi]set, it is important that this deletes behind the iterator
// because the hash_set may call the hash function on the iterator when it is
// advanced, which could result in the hash function trying to deference a
// stale pointer.
// NOTE: If you're calling this on an entire container, you probably want
// to call STLDeleteElements(&container) instead, or use an ElementDeleter.
template <class ForwardIterator>
void STLDeleteContainerPointers(ForwardIterator begin, ForwardIterator end) {
    while (begin != end) {
        ForwardIterator temp = begin;
        ++begin;
        delete *temp;
    }
}

// STLDeleteContainerPairPointers()
//  For a range within a container of pairs, calls delete
//  (non-array version) on BOTH items in the pairs.
// NOTE: Like STLDeleteContainerPointers, it is important that this deletes
// behind the iterator because if both the key and value are deleted, the
// container may call the hash function on the iterator when it is advanced,
// which could result in the hash function trying to dereference a stale
// pointer.
template <class ForwardIterator>
void STLDeleteContainerPairPointers(ForwardIterator begin, ForwardIterator end) {
    while (begin != end) {
        ForwardIterator temp = begin;
        ++begin;
        delete temp->first;
        delete temp->second;
    }
}

// STLDeleteContainerPairFirstPointers()
//  For a range within a container of pairs, calls delete (non-array version)
//  on the FIRST item in the pairs.
// NOTE: Like STLDeleteContainerPointers, deleting behind the iterator.
template <class ForwardIterator>
void STLDeleteContainerPairFirstPointers(ForwardIterator begin, ForwardIterator end) {
    while (begin != end) {
        ForwardIterator temp = begin;
        ++begin;
        delete temp->first;
    }
}

// STLDeleteContainerPairSecondPointers()
//  For a range within a container of pairs, calls delete
//  (non-array version) on the SECOND item in the pairs.
// NOTE: Like STLDeleteContainerPointers, deleting behind the iterator.
// Deleting the value does not always invalidate the iterator, but it may
// do so if the key is a pointer into the value object.
// NOTE: If you're calling this on an entire container, you probably want
// to call STLDeleteValues(&container) instead, or use ValueDeleter.
template <class ForwardIterator>
void STLDeleteContainerPairSecondPointers(ForwardIterator begin, ForwardIterator end) {
    while (begin != end) {
        ForwardIterator temp = begin;
        ++begin;
        delete temp->second;
    }
}

template <typename T>
inline void STLAssignToVector(vector<T>* vec, const T* ptr, size_t n) {
    vec->resize(n);
    if (n == 0) return;
    memcpy(&vec->front(), ptr, n * sizeof(T));
}

// Not faster; but we need the specialization so the function works at all
// on the vector<bool> specialization.
template <>
inline void STLAssignToVector(vector<bool>* vec, const bool* ptr, size_t n) {
    vec->clear();
    if (n == 0) return;
    vec->insert(vec->begin(), ptr, ptr + n);
}

/***** Hack to allow faster assignment to a vector *****/

// This routine speeds up an assignment of 32 bytes to a vector from
// about 250 cycles per assignment to about 140 cycles.
//
// Usage:
//      STLAssignToVectorChar(&vec, ptr, size);
//      STLAssignToString(&str, ptr, size);

inline void STLAssignToVectorChar(vector<char>* vec, const char* ptr, size_t n) {
    STLAssignToVector(vec, ptr, n);
}

// A struct that mirrors the GCC4 implementation of a string. See:
// /usr/crosstool/v8/gcc-4.1.0-glibc-2.2.2/i686-unknown-linux-gnu/include/c++/4.1.0/ext/sso_string_base.h
struct InternalStringRepGCC4 {
    char* _M_data = nullptr;
    size_t _M_string_length;

    enum { _S_local_capacity = 15 };

    union {
        char _M_local_data[_S_local_capacity + 1];
        size_t _M_allocated_capacity;
    };
};

// Like str->resize(new_size), except any new characters added to
// "*str" as a result of resizing may be left uninitialized, rather
// than being filled with '0' bytes.  Typically used when code is then
// going to overwrite the backing store of the string with known data.
inline void STLStringResizeUninitialized(string* s, size_t new_size) {
    if (sizeof(*s) == sizeof(InternalStringRepGCC4)) {
        if (new_size > s->capacity()) {
            s->reserve(new_size);
        }
        // The line below depends on the layout of 'string'.  THIS IS
        // NON-PORTABLE CODE.  If our STL implementation changes, we will
        // need to change this as well.
        InternalStringRepGCC4* rep = reinterpret_cast<InternalStringRepGCC4*>(s);
        assert(rep->_M_data == s->data());
        assert(rep->_M_string_length == s->size());

        // We have to null-terminate the string for c_str() to work properly.
        // So we leave the actual contents of the string uninitialized, but
        // we set the byte one past the new end of the string to '\0'
        const_cast<char*>(s->data())[new_size] = '\0';
        rep->_M_string_length = new_size;
    } else {
        // Slow path: have to reallocate stuff, or an unknown string rep
        s->resize(new_size);
    }
}

// Returns true if the string implementation supports a resize where
// the new characters added to the string are left untouched.
inline bool STLStringSupportsNontrashingResize(const string& s) {
    return (sizeof(s) == sizeof(InternalStringRepGCC4));
}

inline void STLAssignToString(string* str, const char* ptr, size_t n) {
    STLStringResizeUninitialized(str, n);
    if (n == 0) return;
    memcpy(&*str->begin(), ptr, n);
}

inline void STLAppendToString(string* str, const char* ptr, size_t n) {
    if (n == 0) return;
    size_t old_size = str->size();
    STLStringResizeUninitialized(str, old_size + n);
    memcpy(&*str->begin() + old_size, ptr, n);
}

// To treat a possibly-empty vector as an array, use these functions.
// If you know the array will never be empty, you can use &*v.begin()
// directly, but that is allowed to dump core if v is empty.  This
// function is the most efficient code that will work, taking into
// account how our STL is actually implemented.  THIS IS NON-PORTABLE
// CODE, so call us instead of repeating the nonportable code
// everywhere.  If our STL implementation changes, we will need to
// change this as well.

template <typename T, typename Allocator>
 T* vector_as_array(vector<T, Allocator>* v) {
#ifdef NDEBUG
    return &*v->begin();
#else
    return v->empty() ? NULL : &*v->begin();
#endif
}

template <typename T, typename Allocator>
 const T* vector_as_array(const vector<T, Allocator>* v) {
#ifdef NDEBUG
    return &*v->begin();
#else
    return v->empty() ? NULL : &*v->begin();
#endif
}

// Return a mutable char* pointing to a string's internal buffer,
// which may not be null-terminated. Writing through this pointer will
// modify the string.
//
// string_as_array(&str)[i] is valid for 0 <= i < str.size() until the
// next call to a string method that invalidates iterators.
//
// Prior to C++11, there was no standard-blessed way of getting a mutable
// reference to a string's internal buffer. The requirement that string be
// contiguous is officially part of the C++11 standard [string.require]/5.
// According to Matt Austern, this should already work on all current C++98
// implementations.
inline char* string_as_array(string* str) {
    // DO NOT USE const_cast<char*>(str->data())! See the unittest for why.
    return str->empty() ? NULL : &*str->begin();
}

// These are methods that test two hash maps/sets for equality.  These exist
// because the == operator in the STL can return false when the maps/sets
// contain identical elements.  This is because it compares the internal hash
// tables which may be different if the order of insertions and deletions
// differed.

template <class HashSet>
 bool HashSetEquality(const HashSet& set_a, const HashSet& set_b) {
    if (set_a.size() != set_b.size()) return false;
    for (typename HashSet::const_iterator i = set_a.begin(); i != set_a.end(); ++i)
        if (set_b.find(*i) == set_b.end()) return false;
    return true;
}

template <class HashMap>
 bool HashMapEquality(const HashMap& map_a, const HashMap& map_b) {
    if (map_a.size() != map_b.size()) return false;
    for (typename HashMap::const_iterator i = map_a.begin(); i != map_a.end(); ++i) {
        typename HashMap::const_iterator j = map_b.find(i->first);
        if (j == map_b.end()) return false;
        if (i->second != j->second) return false;
    }
    return true;
}

// The following functions are useful for cleaning up STL containers
// whose elements point to allocated memory.

// STLDeleteElements() deletes all the elements in an STL container and clears
// the container.  This function is suitable for use with a vector, set,
// hash_set, or any other STL container which defines sensible begin(), end(),
// and clear() methods.
//
// If container is NULL, this function is a no-op.
//
// As an alternative to calling STLDeleteElements() directly, consider
// ElementDeleter (defined below), which ensures that your container's elements
// are deleted when the ElementDeleter goes out of scope.
template <class T>
void STLDeleteElements(T* container) {
    if (!container) return;
    STLDeleteContainerPointers(container->begin(), container->end());
    container->clear();
}

// Given an STL container consisting of (key, value) pairs, STLDeleteValues
// deletes all the "value" components and clears the container.  Does nothing
// in the case it's given a NULL pointer.
template <class T>
void STLDeleteValues(T* v) {
    if (!v) return;
    STLDeleteContainerPairSecondPointers(v->begin(), v->end());
    v->clear();
}

// ElementDeleter and ValueDeleter provide a convenient way to delete all
// elements or values from STL containers when they go out of scope.  This
// greatly simplifies code that creates temporary objects and has multiple
// return statements.  Example:
//
// vector<MyProto *> tmp_proto;
// ElementDeleter d(&tmp_proto);
// if (...) return false;
// ...
// return success;

// A very simple interface that simply provides a virtual destructor.  It is
// used as a non-templated base class for the TemplatedElementDeleter and
// TemplatedValueDeleter classes.  Clients should not typically use this class
// directly.
class BaseDeleter {
public:
    virtual ~BaseDeleter() {}

protected:
    BaseDeleter() {}

private:
    DISALLOW_EVIL_CONSTRUCTORS(BaseDeleter);
};

// Given a pointer to an STL container, this class will delete all the element
// pointers when it goes out of scope.  Clients should typically use
// ElementDeleter rather than invoking this class directly.
template <class STLContainer>
class TemplatedElementDeleter : public BaseDeleter {
public:
    explicit TemplatedElementDeleter(STLContainer* ptr) : container_ptr_(ptr) {}

    virtual ~TemplatedElementDeleter() { STLDeleteElements(container_ptr_); }

private:
    STLContainer* container_ptr_ = nullptr;

    DISALLOW_EVIL_CONSTRUCTORS(TemplatedElementDeleter);
};

// Like TemplatedElementDeleter, this class will delete element pointers from a
// container when it goes out of scope.  However, it is much nicer to use,
// since the class itself is not templated.
class ElementDeleter {
public:
    template <class STLContainer>
    explicit ElementDeleter(STLContainer* ptr)
            : deleter_(new TemplatedElementDeleter<STLContainer>(ptr)) {}

    ~ElementDeleter() { delete deleter_; }

private:
    BaseDeleter* deleter_ = nullptr;

    DISALLOW_EVIL_CONSTRUCTORS(ElementDeleter);
};

// Given a pointer to an STL container this class will delete all the value
// pointers when it goes out of scope.  Clients should typically use
// ValueDeleter rather than invoking this class directly.
template <class STLContainer>
class TemplatedValueDeleter : public BaseDeleter {
public:
    explicit TemplatedValueDeleter(STLContainer* ptr) : container_ptr_(ptr) {}

    virtual ~TemplatedValueDeleter() { STLDeleteValues(container_ptr_); }

private:
    STLContainer* container_ptr_ = nullptr;

    DISALLOW_EVIL_CONSTRUCTORS(TemplatedValueDeleter);
};

// Similar to ElementDeleter, but wraps a TemplatedValueDeleter rather than an
// TemplatedElementDeleter.
class ValueDeleter {
public:
    template <class STLContainer>
    explicit ValueDeleter(STLContainer* ptr)
            : deleter_(new TemplatedValueDeleter<STLContainer>(ptr)) {}

    ~ValueDeleter() { delete deleter_; }

private:
    BaseDeleter* deleter_ = nullptr;

    DISALLOW_EVIL_CONSTRUCTORS(ValueDeleter);
};

// STLElementDeleter and STLValueDeleter are similar to ElementDeleter and
// ValueDeleter, except that:
// - The classes are templated, making them less convenient to use.
// - Their destructors are not virtual, making them potentially more efficient.
// New code should typically use ElementDeleter and ValueDeleter unless
// efficiency is a large concern.

template <class STLContainer>
class STLElementDeleter {
public:
    STLElementDeleter(STLContainer* ptr) : container_ptr_(ptr) {}
    ~STLElementDeleter() { STLDeleteElements(container_ptr_); }

private:
    STLContainer* container_ptr_ = nullptr;
};

template <class STLContainer>
class STLValueDeleter {
public:
    STLValueDeleter(STLContainer* ptr) : container_ptr_(ptr) {}
    ~STLValueDeleter() { STLDeleteValues(container_ptr_); }

private:
    STLContainer* container_ptr_ = nullptr;
};

// STLSet{Difference,SymmetricDifference,Union,Intersection}(A a, B b, C *c)
// *APPEND* the set {difference, symmetric difference, union, intersection} of
// the two sets a and b to c.
// STLSet{Difference,SymmetricDifference,Union,Intersection}(T a, T b) do the
// same but return the result by value rather than by the third pointer
// argument.  The result type is the same as both of the inputs in the two
// argument case.
//
// Requires:
//   a and b must be STL like containers that contain sorted data (as defined
//   by the < operator).
//   For the 3 argument version &a == c or &b == c are disallowed.  In those
//   cases the 2 argument version is probably what you want anyway:
//   a = STLSetDifference(a, b);
//
// These function are convenience functions.  The code they implement is
// trivial (at least for now).  The STL incantations they wrap are just too
// verbose for programmers to use then and they are unpleasant to the eye.
// Without these convenience versions people will simply keep writing one-off
// for loops which are harder to read and more error prone.
//
// Note that for initial construction of an object it is just as efficient to
// use the 2 argument version as the 3 version due to RVO (return value
// optimization) of modern C++ compilers:
//   set<int> c = STLSetDifference(a, b);
// is an example of where RVO comes into play.

template <typename SortedSTLContainerA, typename SortedSTLContainerB, typename SortedSTLContainerC>
void STLSetDifference(const SortedSTLContainerA& a, const SortedSTLContainerB& b,
                      SortedSTLContainerC* c) {
    // The qualified name avoids an ambiguity error, particularly with C++11:
    assert(std::is_sorted(a.begin(), a.end()));
    assert(std::is_sorted(b.begin(), b.end()));
    assert(static_cast<const void*>(&a) != static_cast<const void*>(c));
    assert(static_cast<const void*>(&b) != static_cast<const void*>(c));
    std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::inserter(*c, c->end()));
}

template <typename SortedSTLContainer>
SortedSTLContainer STLSetDifference(const SortedSTLContainer& a, const SortedSTLContainer& b) {
    SortedSTLContainer c;
    STLSetDifference(a, b, &c);
    return c;
}

template <typename SortedSTLContainerA, typename SortedSTLContainerB, typename SortedSTLContainerC>
void STLSetUnion(const SortedSTLContainerA& a, const SortedSTLContainerB& b,
                 SortedSTLContainerC* c) {
    assert(std::is_sorted(a.begin(), a.end()));
    assert(std::is_sorted(b.begin(), b.end()));
    assert(static_cast<const void*>(&a) != static_cast<const void*>(c));
    assert(static_cast<const void*>(&b) != static_cast<const void*>(c));
    std::set_union(a.begin(), a.end(), b.begin(), b.end(), std::inserter(*c, c->end()));
}

template <typename SortedSTLContainerA, typename SortedSTLContainerB, typename SortedSTLContainerC>
void STLSetSymmetricDifference(const SortedSTLContainerA& a, const SortedSTLContainerB& b,
                               SortedSTLContainerC* c) {
    assert(std::is_sorted(a.begin(), a.end()));
    assert(std::is_sorted(b.begin(), b.end()));
    assert(static_cast<const void*>(&a) != static_cast<const void*>(c));
    assert(static_cast<const void*>(&b) != static_cast<const void*>(c));
    std::set_symmetric_difference(a.begin(), a.end(), b.begin(), b.end(),
                                  std::inserter(*c, c->end()));
}

template <typename SortedSTLContainer>
SortedSTLContainer STLSetSymmetricDifference(const SortedSTLContainer& a,
                                             const SortedSTLContainer& b) {
    SortedSTLContainer c;
    STLSetSymmetricDifference(a, b, &c);
    return c;
}

template <typename SortedSTLContainer>
SortedSTLContainer STLSetUnion(const SortedSTLContainer& a, const SortedSTLContainer& b) {
    SortedSTLContainer c;
    STLSetUnion(a, b, &c);
    return c;
}

template <typename SortedSTLContainerA, typename SortedSTLContainerB, typename SortedSTLContainerC>
void STLSetIntersection(const SortedSTLContainerA& a, const SortedSTLContainerB& b,
                        SortedSTLContainerC* c) {
    assert(std::is_sorted(a.begin(), a.end()));
    assert(std::is_sorted(b.begin(), b.end()));
    assert(static_cast<const void*>(&a) != static_cast<const void*>(c));
    assert(static_cast<const void*>(&b) != static_cast<const void*>(c));
    std::set_intersection(a.begin(), a.end(), b.begin(), b.end(), std::inserter(*c, c->end()));
}

template <typename SortedSTLContainer>
SortedSTLContainer STLSetIntersection(const SortedSTLContainer& a, const SortedSTLContainer& b) {
    SortedSTLContainer c;
    STLSetIntersection(a, b, &c);
    return c;
}

// Similar to STLSet{Union,Intesection,etc}, but simpler because the result is
// always bool.
template <typename SortedSTLContainerA, typename SortedSTLContainerB>
bool STLIncludes(const SortedSTLContainerA& a, const SortedSTLContainerB& b) {
    assert(std::is_sorted(a.begin(), a.end()));
    assert(std::is_sorted(b.begin(), b.end()));
    return std::includes(a.begin(), a.end(), b.begin(), b.end());
}

// Functors that compose arbitrary unary and binary functions with a
// function that "projects" one of the members of a pair.
// Specifically, if p1 and p2, respectively, are the functions that
// map a pair to its first and second, respectively, members, the
// table below summarizes the functions that can be constructed:
//
// * UnaryOperate1st<pair>(f) returns the function x -> f(p1(x))
// * UnaryOperate2nd<pair>(f) returns the function x -> f(p2(x))
// * BinaryOperate1st<pair>(f) returns the function (x,y) -> f(p1(x),p1(y))
// * BinaryOperate2nd<pair>(f) returns the function (x,y) -> f(p2(x),p2(y))
//
// A typical usage for these functions would be when iterating over
// the contents of an STL map. For other sample usage, see the unittest.

template <typename Pair, typename UnaryOp>
class UnaryOperateOnFirst {
public:
    UnaryOperateOnFirst() {}

    UnaryOperateOnFirst(const UnaryOp& f) : f_(f) { // TODO(user): explicit?
    }

    typename UnaryOp::result_type operator()(const Pair& p) const { return f_(p.first); }

private:
    UnaryOp f_;
};

template <typename Pair, typename UnaryOp>
UnaryOperateOnFirst<Pair, UnaryOp> UnaryOperate1st(const UnaryOp& f) {
    return UnaryOperateOnFirst<Pair, UnaryOp>(f);
}

template <typename Pair, typename UnaryOp>
class UnaryOperateOnSecond {
public:
    UnaryOperateOnSecond() {}

    UnaryOperateOnSecond(const UnaryOp& f) : f_(f) { // TODO(user): explicit?
    }

    typename UnaryOp::result_type operator()(const Pair& p) const { return f_(p.second); }

private:
    UnaryOp f_;
};

template <typename Pair, typename UnaryOp>
UnaryOperateOnSecond<Pair, UnaryOp> UnaryOperate2nd(const UnaryOp& f) {
    return UnaryOperateOnSecond<Pair, UnaryOp>(f);
}

template <typename Pair, typename BinaryOp>
class BinaryOperateOnFirst {
public:
    BinaryOperateOnFirst() {}

    BinaryOperateOnFirst(const BinaryOp& f) : f_(f) { // TODO(user): explicit?
    }

    typename BinaryOp::result_type operator()(const Pair& p1, const Pair& p2) const {
        return f_(p1.first, p2.first);
    }

private:
    BinaryOp f_;
};

// TODO(user): explicit?
template <typename Pair, typename BinaryOp>
BinaryOperateOnFirst<Pair, BinaryOp> BinaryOperate1st(const BinaryOp& f) {
    return BinaryOperateOnFirst<Pair, BinaryOp>(f);
}

template <typename Pair, typename BinaryOp>
class BinaryOperateOnSecond {
public:
    BinaryOperateOnSecond() {}

    BinaryOperateOnSecond(const BinaryOp& f) : f_(f) {}

    typename BinaryOp::result_type operator()(const Pair& p1, const Pair& p2) const {
        return f_(p1.second, p2.second);
    }

private:
    BinaryOp f_;
};

template <typename Pair, typename BinaryOp>
BinaryOperateOnSecond<Pair, BinaryOp> BinaryOperate2nd(const BinaryOp& f) {
    return BinaryOperateOnSecond<Pair, BinaryOp>(f);
}

// Functor that composes a binary functor h from an arbitrary binary functor
// f and two unary functors g1, g2, so that:
//
// BinaryCompose1(f, g) returns function (x, y) -> f(g(x), g(y))
// BinaryCompose2(f, g1, g2) returns function (x, y) -> f(g1(x), g2(y))
//
// This is a generalization of the BinaryOperate* functors for types other
// than pairs.
//
// For sample usage, see the unittest.
//
// F has to be a model of AdaptableBinaryFunction.
// G1 and G2 have to be models of AdabtableUnaryFunction.
template <typename F, typename G1, typename G2>
class BinaryComposeBinary {
public:
    BinaryComposeBinary(F f, G1 g1, G2 g2) : f_(f), g1_(g1), g2_(g2) {}

    typename F::result_type operator()(typename G1::argument_type x,
                                       typename G2::argument_type y) const {
        return f_(g1_(x), g2_(y));
    }

private:
    F f_;
    G1 g1_;
    G2 g2_;
};

template <typename F, typename G>
BinaryComposeBinary<F, G, G> BinaryCompose1(F f, G g) {
    return BinaryComposeBinary<F, G, G>(f, g, g);
}

template <typename F, typename G1, typename G2>
BinaryComposeBinary<F, G1, G2> BinaryCompose2(F f, G1 g1, G2 g2) {
    return BinaryComposeBinary<F, G1, G2>(f, g1, g2);
}

// Even though a struct has no data members, it cannot have zero size
// according to the standard.  However, "empty base-class
// optimization" allows an empty parent class to add no additional
// size to the object.  STLEmptyBaseHandle is a handy way to "stuff"
// objects that are typically empty (e.g., allocators, compare
// objects) into other fields of an object without increasing the size
// of the object.
//
// struct Empty {
//   void Method() { }
// };
// struct OneInt {
//   STLEmptyBaseHandle<Empty, int> i;
// };
//
// In the example above, "i.data" refers to the integer field, whereas
// "i" refers to the empty base class.  sizeof(OneInt) == sizeof(int)
// despite the fact that sizeof(Empty) > 0.
template <typename Base, typename Data>
struct STLEmptyBaseHandle : public Base {
    template <typename U>
    STLEmptyBaseHandle(const U& b, const Data& d) : Base(b), data(d) {}
    Data data;
};

// These functions return true if there is some element in the sorted range
// [begin1, end) which is equal to some element in the sorted range [begin2,
// end2). The iterators do not have to be of the same type, but the value types
// must be less-than comparable. (Two elements a,b are considered equal if
// !(a < b) && !(b < a).
template <typename InputIterator1, typename InputIterator2>
bool SortedRangesHaveIntersection(InputIterator1 begin1, InputIterator1 end1, InputIterator2 begin2,
                                  InputIterator2 end2) {
    assert(std::is_sorted(begin1, end1));
    assert(std::is_sorted(begin2, end2));
    while (begin1 != end1 && begin2 != end2) {
        if (*begin1 < *begin2) {
            ++begin1;
        } else if (*begin2 < *begin1) {
            ++begin2;
        } else {
            return true;
        }
    }
    return false;
}

// This is equivalent to the function above, but using a custom comparison
// function.
template <typename InputIterator1, typename InputIterator2, typename Comp>
bool SortedRangesHaveIntersection(InputIterator1 begin1, InputIterator1 end1, InputIterator2 begin2,
                                  InputIterator2 end2, Comp comparator) {
    assert(std::is_sorted(begin1, end1, comparator));
    assert(std::is_sorted(begin2, end2, comparator));
    while (begin1 != end1 && begin2 != end2) {
        if (comparator(*begin1, *begin2)) {
            ++begin1;
        } else if (comparator(*begin2, *begin1)) {
            ++begin2;
        } else {
            return true;
        }
    }
    return false;
}

// release_ptr is intended to help remove systematic use of gscoped_ptr
// in cases like:
//
// vector<Foo *> v;
// ElementDeleter d(&v);
// ... {
//   int remove_idx = f(v);
//   gscoped_ptr<Foo> t(v[remove_idx]);
//   v[remove_idx] = NULL;  // Save from deleter.
//   return t.release();
// }
//
// This would be replaced by:
// ... {
//   int remove_idx = f(v);
//   return release_ptr(&v[remove_idx]);
// }
template <typename T>
T* release_ptr(T** ptr) MUST_USE_RESULT;
template <typename T>
T* release_ptr(T** ptr) {
    assert(ptr);
    T* tmp = *ptr;
    *ptr = NULL;
    return tmp;
}
