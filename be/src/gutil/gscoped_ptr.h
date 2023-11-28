// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE.txt file.

// Scopers help you manage ownership of a pointer, helping you easily manage the
// a pointer within a scope, and automatically destroying the pointer at the
// end of a scope.  There are two main classes you will use, which correspond
// to the operators new/delete and new[]/delete[].
//
// Example usage (gscoped_ptr):
//   {
//     gscoped_ptr<Foo> foo(new Foo("wee"));
//   }  // foo goes out of scope, releasing the pointer with it.
//
//   {
//     gscoped_ptr<Foo> foo;          // No pointer managed.
//     foo.reset(new Foo("wee"));    // Now a pointer is managed.
//     foo.reset(new Foo("wee2"));   // Foo("wee") was destroyed.
//     foo.reset(new Foo("wee3"));   // Foo("wee2") was destroyed.
//     foo->Method();                // Foo::Method() called.
//     foo.get()->Method();          // Foo::Method() called.
//     SomeFunc(foo.release());      // SomeFunc takes ownership, foo no longer
//                                   // manages a pointer.
//     foo.reset(new Foo("wee4"));   // foo manages a pointer again.
//     foo.reset();                  // Foo("wee4") destroyed, foo no longer
//                                   // manages a pointer.
//   }  // foo wasn't managing a pointer, so nothing was destroyed.
//
// Example usage (gscoped_array):
//   {
//     gscoped_array<Foo> foo(new Foo[100]);
//     foo.get()->Method();  // Foo::Method on the 0th element.
//     foo[10].Method();     // Foo::Method on the 10th element.
//   }
//
// These scopers also implement part of the functionality of C++11 unique_ptr
// in that they are "movable but not copyable."  You can use the scopers in
// the parameter and return types of functions to signify ownership transfer
// in to and out of a function.  When calling a function that has a scoper
// as the argument type, it must be called with the result of an analogous
// scoper's Pass() function or another function that generates a temporary;
// passing by copy will NOT work.  Here is an example using gscoped_ptr:
//
//   void TakesOwnership(gscoped_ptr<Foo> arg) {
//     // Do something with arg
//   }
//   gscoped_ptr<Foo> CreateFoo() {
//     // No need for calling Pass() because we are constructing a temporary
//     // for the return value.
//     return gscoped_ptr<Foo>(new Foo("new"));
//   }
//   gscoped_ptr<Foo> PassThru(gscoped_ptr<Foo> arg) {
//     return std::move(arg);
//   }
//
//   {
//     gscoped_ptr<Foo> ptr(new Foo("yay"));  // ptr manages Foo("yay").
//     TakesOwnership(std::move(ptr));           // ptr no longer owns Foo("yay").
//     gscoped_ptr<Foo> ptr2 = CreateFoo();   // ptr2 owns the return Foo.
//     gscoped_ptr<Foo> ptr3 =                // ptr3 now owns what was in ptr2.
//         PassThru(std::move(ptr2));            // ptr2 is correspondingly NULL.
//   }
//
// Notice that if you do not call Pass() when returning from PassThru(), or
// when invoking TakesOwnership(), the code will not compile because scopers
// are not copyable; they only implement move semantics which require calling
// the Pass() function to signify a destructive transfer of state. CreateFoo()
// is different though because we are constructing a temporary on the return
// line and thus can avoid needing to call Pass().
//
// Pass() properly handles upcast in assignment, i.e. you can assign
// gscoped_ptr<Child> to gscoped_ptr<Parent>:
//
//   gscoped_ptr<Foo> foo(new Foo());
//   gscoped_ptr<FooParent> parent = std::move(foo);
//
// PassAs<>() should be used to upcast return value in return statement:
//
//   gscoped_ptr<Foo> CreateFoo() {
//     gscoped_ptr<FooChild> result(new FooChild());
//     return result.PassAs<Foo>();
//   }
//
// Note that PassAs<>() is implemented only for gscoped_ptr, but not for
// gscoped_array. This is because casting array pointers may not be safe.
//
// -------------------------------------------------------------------------
// Cloudera notes: this should be used in preference to std::unique_ptr since
// it offers a ::release() method like unique_ptr. We unfortunately cannot
// just use unique_ptr because it has an inconsistent implementation in
// some of the older compilers we have to support.
// -------------------------------------------------------------------------

// This is an implementation designed to match the anticipated future TR2
// implementation of the scoped_ptr class, and its closely-related brethren,
// scoped_array, scoped_ptr_malloc.

#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>

#include <algorithm> // For std::swap().
#include <type_traits>

#include "gutil/basictypes.h"
#include "gutil/move.h"

namespace doris {

namespace subtle {
class RefCountedBase;
class RefCountedThreadSafeBase;
} // namespace subtle

// Function object which deletes its parameter, which must be a pointer.
// If C is an array type, invokes 'delete[]' on the parameter; otherwise,
// invokes 'delete'. The default deleter for gscoped_ptr<T>.
template <class T>
struct DefaultDeleter {
    DefaultDeleter() {}
    template <typename U>
    DefaultDeleter(const DefaultDeleter<U>& other) {
        // IMPLEMENTATION NOTE: C++11 20.7.1.1.2p2 only provides this constructor
        // if U* is implicitly convertible to T* and U is not an array type.
        //
        // Correct implementation should use SFINAE to disable this
        // constructor. However, since there are no other 1-argument constructors,
        // using a COMPILE_ASSERT() based on is_convertible<> and requiring
        // complete types is simpler and will cause compile failures for equivalent
        // misuses.
        //
        // Note, the is_convertible<U*, T*> check also ensures that U is not an
        // array. T is guaranteed to be a non-array, so any U* where U is an array
        // cannot convert to T*.
        enum { T_must_be_complete = sizeof(T) };
        enum { U_must_be_complete = sizeof(U) };
        COMPILE_ASSERT((std::is_convertible<U*, T*>::value),
                       U_ptr_must_implicitly_convert_to_T_ptr);
    }
    inline void operator()(T* ptr) const {
        enum { type_must_be_complete = sizeof(T) };
        delete ptr;
    }
};

// Specialization of DefaultDeleter for array types.
template <class T>
struct DefaultDeleter<T[]> {
    inline void operator()(T* ptr) const {
        enum { type_must_be_complete = sizeof(T) };
        delete[] ptr;
    }

private:
    // Disable this operator for any U != T because it is undefined to execute
    // an array delete when the static type of the array mismatches the dynamic
    // type.
    //
    // References:
    //   C++98 [expr.delete]p3
    //   http://cplusplus.github.com/LWG/lwg-defects.html#938
    template <typename U>
    void operator()(U* array) const;
};

template <class T, int n>
struct DefaultDeleter<T[n]> {
    // Never allow someone to declare something like gscoped_ptr<int[10]>.
    COMPILE_ASSERT(sizeof(T) == -1, do_not_use_array_with_size_as_type);
};

// Function object which invokes 'free' on its parameter, which must be
// a pointer. Can be used to store malloc-allocated pointers in gscoped_ptr:
//
// gscoped_ptr<int, doris::FreeDeleter> foo_ptr(
//     static_cast<int*>(malloc(sizeof(int))));
struct FreeDeleter {
    inline void operator()(void* ptr) const { free(ptr); }
};

namespace internal {

template <typename T>
struct IsNotRefCounted {
    enum {
        value = !std::is_convertible<T*, doris::subtle::RefCountedBase*>::value &&
                !std::is_convertible<T*, doris::subtle::RefCountedThreadSafeBase*>::value
    };
};

// Minimal implementation of the core logic of gscoped_ptr, suitable for
// reuse in both gscoped_ptr and its specializations.
template <class T, class D>
class gscoped_ptr_impl {
public:
    explicit gscoped_ptr_impl(T* p) : data_(p) {}

    // Initializer for deleters that have data parameters.
    gscoped_ptr_impl(T* p, const D& d) : data_(p, d) {}

    // Templated constructor that destructively takes the value from another
    // gscoped_ptr_impl.
    template <typename U, typename V>
    gscoped_ptr_impl(gscoped_ptr_impl<U, V>* other)
            : data_(other->release(), other->get_deleter()) {
        // We do not support move-only deleters.  We could modify our move
        // emulation to have base::subtle::move() and base::subtle::forward()
        // functions that are imperfect emulations of their C++11 equivalents,
        // but until there's a requirement, just assume deleters are copyable.
    }

    template <typename U, typename V>
    void TakeState(gscoped_ptr_impl<U, V>* other) {
        // See comment in templated constructor above regarding lack of support
        // for move-only deleters.
        reset(other->release());
        get_deleter() = other->get_deleter();
    }

    ~gscoped_ptr_impl() {
        if (data_.ptr != NULL) {
            // Not using get_deleter() saves one function call in non-optimized
            // builds.
            static_cast<D&>(data_)(data_.ptr);
        }
    }

    void reset(T* p) {
        // This is a self-reset, which is no longer allowed: http://crbug.com/162971
        if (p != NULL && p == data_.ptr) abort();

        // Note that running data_.ptr = p can lead to undefined behavior if
        // get_deleter()(get()) deletes this. In order to pevent this, reset()
        // should update the stored pointer before deleting its old value.
        //
        // However, changing reset() to use that behavior may cause current code to
        // break in unexpected ways. If the destruction of the owned object
        // dereferences the gscoped_ptr when it is destroyed by a call to reset(),
        // then it will incorrectly dispatch calls to |p| rather than the original
        // value of |data_.ptr|.
        //
        // During the transition period, set the stored pointer to NULL while
        // deleting the object. Eventually, this safety check will be removed to
        // prevent the scenario initially described from occuring and
        // http://crbug.com/176091 can be closed.
        T* old = data_.ptr;
        data_.ptr = NULL;
        if (old != NULL) static_cast<D&>(data_)(old);
        data_.ptr = p;
    }

    T* get() const { return data_.ptr; }

    D& get_deleter() { return data_; }
    const D& get_deleter() const { return data_; }

    void swap(gscoped_ptr_impl& p2) {
        // Standard swap idiom: 'using std::swap' ensures that std::swap is
        // present in the overload set, but we call swap unqualified so that
        // any more-specific overloads can be used, if available.
        using std::swap;
        swap(static_cast<D&>(data_), static_cast<D&>(p2.data_));
        swap(data_.ptr, p2.data_.ptr);
    }

    T* release() {
        T* old_ptr = data_.ptr;
        data_.ptr = NULL;
        return old_ptr;
    }

private:
    // Needed to allow type-converting constructor.
    template <typename U, typename V>
    friend class gscoped_ptr_impl;

    // Use the empty base class optimization to allow us to have a D
    // member, while avoiding any space overhead for it when D is an
    // empty class.  See e.g. http://www.cantrip.org/emptyopt.html for a good
    // discussion of this technique.
    struct Data : public D {
        explicit Data(T* ptr_in) : ptr(ptr_in) {}
        Data(T* ptr_in, D other) : D(std::move(other)), ptr(ptr_in) {}
        T* ptr = nullptr;
    };

    Data data_;

    DISALLOW_COPY_AND_ASSIGN(gscoped_ptr_impl);
};

} // namespace internal

} // namespace doris

// A gscoped_ptr<T> is like a T*, except that the destructor of gscoped_ptr<T>
// automatically deletes the pointer it holds (if any).
// That is, gscoped_ptr<T> owns the T object that it points to.
// Like a T*, a gscoped_ptr<T> may hold either NULL or a pointer to a T object.
// Also like T*, gscoped_ptr<T> is thread-compatible, and once you
// dereference it, you get the thread safety guarantees of T.
//
// The size of gscoped_ptr is small. On most compilers, when using the
// DefaultDeleter, sizeof(gscoped_ptr<T>) == sizeof(T*). Custom deleters will
// increase the size proportional to whatever state they need to have. See
// comments inside gscoped_ptr_impl<> for details.
//
// Current implementation targets having a strict subset of  C++11's
// unique_ptr<> features. Known deficiencies include not supporting move-only
// deleteres, function pointers as deleters, and deleters with reference
// types.
template <class T, class D = doris::DefaultDeleter<T>>
class gscoped_ptr {
    MOVE_ONLY_TYPE_FOR_CPP_03(gscoped_ptr, RValue)

    COMPILE_ASSERT(doris::internal::IsNotRefCounted<T>::value,
                   T_is_refcounted_type_and_needs_scoped_refptr);

public:
    // The element and deleter types.
    typedef T element_type;
    typedef D deleter_type;

    // Constructor.  Defaults to initializing with NULL.
    gscoped_ptr() : impl_(NULL) {}

    // Constructor.  Takes ownership of p.
    explicit gscoped_ptr(element_type* p) : impl_(p) {}

    // Constructor.  Allows initialization of a stateful deleter.
    gscoped_ptr(element_type* p, const D& d) : impl_(p, d) {}

    // Constructor.  Allows construction from a gscoped_ptr rvalue for a
    // convertible type and deleter.
    //
    // IMPLEMENTATION NOTE: C++11 unique_ptr<> keeps this constructor distinct
    // from the normal move constructor. By C++11 20.7.1.2.1.21, this constructor
    // has different post-conditions if D is a reference type. Since this
    // implementation does not support deleters with reference type,
    // we do not need a separate move constructor allowing us to avoid one
    // use of SFINAE. You only need to care about this if you modify the
    // implementation of gscoped_ptr.
    template <typename U, typename V>
    gscoped_ptr(gscoped_ptr<U, V> other) : impl_(&other.impl_) {
        COMPILE_ASSERT(!std::is_array<U>::value, U_cannot_be_an_array);
    }

    // Constructor.  Move constructor for C++03 move emulation of this type.
    gscoped_ptr(RValue rvalue) : impl_(&rvalue.object->impl_) {}

    // operator=.  Allows assignment from a gscoped_ptr rvalue for a convertible
    // type and deleter.
    //
    // IMPLEMENTATION NOTE: C++11 unique_ptr<> keeps this operator= distinct from
    // the normal move assignment operator. By C++11 20.7.1.2.3.4, this templated
    // form has different requirements on for move-only Deleters. Since this
    // implementation does not support move-only Deleters, we do not need a
    // separate move assignment operator allowing us to avoid one use of SFINAE.
    // You only need to care about this if you modify the implementation of
    // gscoped_ptr.
    template <typename U, typename V>
    gscoped_ptr& operator=(gscoped_ptr<U, V> rhs) {
        COMPILE_ASSERT(!std::is_array<U>::value, U_cannot_be_an_array);
        impl_.TakeState(&rhs.impl_);
        return *this;
    }

    // Reset.  Deletes the currently owned object, if any.
    // Then takes ownership of a new object, if given.
    void reset(element_type* p = NULL) { impl_.reset(p); }

    // Accessors to get the owned object.
    // operator* and operator-> will assert() if there is no current object.
    element_type& operator*() const {
        assert(impl_.get() != NULL);
        return *impl_.get();
    }
    element_type* operator->() const {
        assert(impl_.get() != NULL);
        return impl_.get();
    }
    element_type* get() const { return impl_.get(); }

    // Access to the deleter.
    deleter_type& get_deleter() { return impl_.get_deleter(); }
    const deleter_type& get_deleter() const { return impl_.get_deleter(); }

    // Allow gscoped_ptr<element_type> to be used in boolean expressions, but not
    // implicitly convertible to a real bool (which is dangerous).
private:
    typedef doris::internal::gscoped_ptr_impl<element_type, deleter_type> gscoped_ptr::*Testable;

public:
    operator Testable() const { return impl_.get() ? &gscoped_ptr::impl_ : NULL; }

    // Comparison operators.
    // These return whether two gscoped_ptr refer to the same object, not just to
    // two different but equal objects.
    bool operator==(const element_type* p) const { return impl_.get() == p; }
    bool operator!=(const element_type* p) const { return impl_.get() != p; }

    // Swap two scoped pointers.
    void swap(gscoped_ptr& p2) { impl_.swap(p2.impl_); }

    // Release a pointer.
    // The return value is the current pointer held by this object.
    // If this object holds a NULL pointer, the return value is NULL.
    // After this operation, this object will hold a NULL pointer,
    // and will not own the object any more.
    element_type* release() WARN_UNUSED_RESULT { return impl_.release(); }

    // C++98 doesn't support functions templates with default parameters which
    // makes it hard to write a PassAs() that understands converting the deleter
    // while preserving simple calling semantics.
    //
    // Until there is a use case for PassAs() with custom deleters, just ignore
    // the custom deleter.
    template <typename PassAsType>
    gscoped_ptr<PassAsType> PassAs() {
        return gscoped_ptr<PassAsType>(Pass());
    }

private:
    // Needed to reach into |impl_| in the constructor.
    template <typename U, typename V>
    friend class gscoped_ptr;
    doris::internal::gscoped_ptr_impl<element_type, deleter_type> impl_;

    // Forbid comparison of gscoped_ptr types.  If U != T, it totally
    // doesn't make sense, and if U == T, it still doesn't make sense
    // because you should never have the same object owned by two different
    // gscoped_ptrs.
    template <class U>
    bool operator==(gscoped_ptr<U> const& p2) const;
    template <class U>
    bool operator!=(gscoped_ptr<U> const& p2) const;
};

template <class T, class D>
class gscoped_ptr<T[], D> {
    MOVE_ONLY_TYPE_FOR_CPP_03(gscoped_ptr, RValue)

public:
    // The element and deleter types.
    typedef T element_type;
    typedef D deleter_type;

    // Constructor.  Defaults to initializing with NULL.
    gscoped_ptr() : impl_(NULL) {}

    // Constructor. Stores the given array. Note that the argument's type
    // must exactly match T*. In particular:
    // - it cannot be a pointer to a type derived from T, because it is
    //   inherently unsafe in the general case to access an array through a
    //   pointer whose dynamic type does not match its static type (eg., if
    //   T and the derived types had different sizes access would be
    //   incorrectly calculated). Deletion is also always undefined
    //   (C++98 [expr.delete]p3). If you're doing this, fix your code.
    // - it cannot be NULL, because NULL is an integral expression, not a
    //   pointer to T. Use the no-argument version instead of explicitly
    //   passing NULL.
    // - it cannot be const-qualified differently from T per unique_ptr spec
    //   (http://cplusplus.github.com/LWG/lwg-active.html#2118). Users wanting
    //   to work around this may use implicit_cast<const T*>().
    //   However, because of the first bullet in this comment, users MUST
    //   NOT use implicit_cast<Base*>() to upcast the static type of the array.
    explicit gscoped_ptr(element_type* array) : impl_(array) {}

    // Constructor.  Move constructor for C++03 move emulation of this type.
    gscoped_ptr(RValue rvalue) : impl_(&rvalue.object->impl_) {}

    // operator=.  Move operator= for C++03 move emulation of this type.
    gscoped_ptr& operator=(RValue rhs) {
        impl_.TakeState(&rhs.object->impl_);
        return *this;
    }

    // Reset.  Deletes the currently owned array, if any.
    // Then takes ownership of a new object, if given.
    void reset(element_type* array = NULL) { impl_.reset(array); }

    // Accessors to get the owned array.
    element_type& operator[](size_t i) const {
        assert(impl_.get() != NULL);
        return impl_.get()[i];
    }
    element_type* get() const { return impl_.get(); }

    // Access to the deleter.
    deleter_type& get_deleter() { return impl_.get_deleter(); }
    const deleter_type& get_deleter() const { return impl_.get_deleter(); }

    // Allow gscoped_ptr<element_type> to be used in boolean expressions, but not
    // implicitly convertible to a real bool (which is dangerous).
private:
    typedef doris::internal::gscoped_ptr_impl<element_type, deleter_type> gscoped_ptr::*Testable;

public:
    operator Testable() const { return impl_.get() ? &gscoped_ptr::impl_ : NULL; }

    // Comparison operators.
    // These return whether two gscoped_ptr refer to the same object, not just to
    // two different but equal objects.
    bool operator==(element_type* array) const { return impl_.get() == array; }
    bool operator!=(element_type* array) const { return impl_.get() != array; }

    // Swap two scoped pointers.
    void swap(gscoped_ptr& p2) { impl_.swap(p2.impl_); }

    // Release a pointer.
    // The return value is the current pointer held by this object.
    // If this object holds a NULL pointer, the return value is NULL.
    // After this operation, this object will hold a NULL pointer,
    // and will not own the object any more.
    element_type* release() WARN_UNUSED_RESULT { return impl_.release(); }

private:
    // Force element_type to be a complete type.
    enum { type_must_be_complete = sizeof(element_type) };

    // Actually hold the data.
    doris::internal::gscoped_ptr_impl<element_type, deleter_type> impl_;

    // Disable initialization from any type other than element_type*, by
    // providing a constructor that matches such an initialization, but is
    // private and has no definition. This is disabled because it is not safe to
    // call delete[] on an array whose static type does not match its dynamic
    // type.
    template <typename U>
    explicit gscoped_ptr(U* array);
    explicit gscoped_ptr(int disallow_construction_from_null);

    // Disable reset() from any type other than element_type*, for the same
    // reasons as the constructor above.
    template <typename U>
    void reset(U* array);
    void reset(int disallow_reset_from_null);

    // Forbid comparison of gscoped_ptr types.  If U != T, it totally
    // doesn't make sense, and if U == T, it still doesn't make sense
    // because you should never have the same object owned by two different
    // gscoped_ptrs.
    template <class U>
    bool operator==(gscoped_ptr<U> const& p2) const;
    template <class U>
    bool operator!=(gscoped_ptr<U> const& p2) const;
};

// Free functions
template <class T, class D>
void swap(gscoped_ptr<T, D>& p1, gscoped_ptr<T, D>& p2) {
    p1.swap(p2);
}

template <class T, class D>
bool operator==(T* p1, const gscoped_ptr<T, D>& p2) {
    return p1 == p2.get();
}

template <class T, class D>
bool operator!=(T* p1, const gscoped_ptr<T, D>& p2) {
    return p1 != p2.get();
}

// DEPRECATED: Use gscoped_ptr<C[]> instead.
//
// gscoped_array<C> is like gscoped_ptr<C>, except that the caller must allocate
// with new [] and the destructor deletes objects with delete [].
//
// As with gscoped_ptr<C>, a gscoped_array<C> either points to an object
// or is NULL.  A gscoped_array<C> owns the object that it points to.
// gscoped_array<T> is thread-compatible, and once you index into it,
// the returned objects have only the thread safety guarantees of T.
//
// Size: sizeof(gscoped_array<C>) == sizeof(C*)
template <class C>
class gscoped_array {
    MOVE_ONLY_TYPE_FOR_CPP_03(gscoped_array, RValue)

public:
    // The element type
    typedef C element_type;

    // Constructor.  Defaults to initializing with NULL.
    // There is no way to create an uninitialized gscoped_array.
    // The input parameter must be allocated with new [].
    explicit gscoped_array(C* p = NULL) : array_(p) {}

    // Constructor.  Move constructor for C++03 move emulation of this type.
    gscoped_array(RValue rvalue) : array_(rvalue.object->release()) {}

    // Destructor.  If there is a C object, delete it.
    // We don't need to test ptr_ == NULL because C++ does that for us.
    ~gscoped_array() {
        enum { type_must_be_complete = sizeof(C) };
        delete[] array_;
    }

    // operator=.  Move operator= for C++03 move emulation of this type.
    gscoped_array& operator=(RValue rhs) {
        reset(rhs.object->release());
        return *this;
    }

    // Reset.  Deletes the current owned object, if any.
    // Then takes ownership of a new object, if given.
    // this->reset(this->get()) works.
    void reset(C* p = NULL) {
        if (p != array_) {
            enum { type_must_be_complete = sizeof(C) };
            delete[] array_;
            array_ = p;
        }
    }

    // Get one element of the current object.
    // Will assert() if there is no current object, or index i is negative.
    C& operator[](ptrdiff_t i) const {
        assert(i >= 0);
        assert(array_ != NULL);
        return array_[i];
    }

    // Get a pointer to the zeroth element of the current object.
    // If there is no current object, return NULL.
    C* get() const { return array_; }

    // Allow gscoped_array<C> to be used in boolean expressions, but not
    // implicitly convertible to a real bool (which is dangerous).
    typedef C* gscoped_array::*Testable;
    operator Testable() const { return array_ ? &gscoped_array::array_ : NULL; }

    // Comparison operators.
    // These return whether two gscoped_array refer to the same object, not just to
    // two different but equal objects.
    bool operator==(C* p) const { return array_ == p; }
    bool operator!=(C* p) const { return array_ != p; }

    // Swap two scoped arrays.
    void swap(gscoped_array& p2) {
        C* tmp = array_;
        array_ = p2.array_;
        p2.array_ = tmp;
    }

    // Release an array.
    // The return value is the current pointer held by this object.
    // If this object holds a NULL pointer, the return value is NULL.
    // After this operation, this object will hold a NULL pointer,
    // and will not own the object any more.
    C* release() WARN_UNUSED_RESULT {
        C* retVal = array_;
        array_ = NULL;
        return retVal;
    }

private:
    C* array_;

    // Forbid comparison of different gscoped_array types.
    template <class C2>
    bool operator==(gscoped_array<C2> const& p2) const;
    template <class C2>
    bool operator!=(gscoped_array<C2> const& p2) const;
};

// Free functions
template <class C>
void swap(gscoped_array<C>& p1, gscoped_array<C>& p2) {
    p1.swap(p2);
}

template <class C>
bool operator==(C* p1, const gscoped_array<C>& p2) {
    return p1 == p2.get();
}

template <class C>
bool operator!=(C* p1, const gscoped_array<C>& p2) {
    return p1 != p2.get();
}

// DEPRECATED: Use gscoped_ptr<C, doris::FreeDeleter> instead.
//
// gscoped_ptr_malloc<> is similar to gscoped_ptr<>, but it accepts a
// second template argument, the functor used to free the object.

template <class C, class FreeProc = doris::FreeDeleter>
class gscoped_ptr_malloc {
    MOVE_ONLY_TYPE_FOR_CPP_03(gscoped_ptr_malloc, RValue)

public:
    // The element type
    typedef C element_type;

    // Constructor.  Defaults to initializing with NULL.
    // There is no way to create an uninitialized gscoped_ptr.
    // The input parameter must be allocated with an allocator that matches the
    // Free functor.  For the default Free functor, this is malloc, calloc, or
    // realloc.
    explicit gscoped_ptr_malloc(C* p = NULL) : ptr_(p) {}

    // Constructor.  Move constructor for C++03 move emulation of this type.
    gscoped_ptr_malloc(RValue rvalue) : ptr_(rvalue.object->release()) {}

    // Destructor.  If there is a C object, call the Free functor.
    ~gscoped_ptr_malloc() { reset(); }

    // operator=.  Move operator= for C++03 move emulation of this type.
    gscoped_ptr_malloc& operator=(RValue rhs) {
        reset(rhs.object->release());
        return *this;
    }

    // Reset.  Calls the Free functor on the current owned object, if any.
    // Then takes ownership of a new object, if given.
    // this->reset(this->get()) works.
    void reset(C* p = NULL) {
        if (ptr_ != p) {
            if (ptr_ != NULL) {
                FreeProc free_proc;
                free_proc(ptr_);
            }
            ptr_ = p;
        }
    }

    // Get the current object.
    // operator* and operator-> will cause an assert() failure if there is
    // no current object.
    C& operator*() const {
        assert(ptr_ != NULL);
        return *ptr_;
    }

    C* operator->() const {
        assert(ptr_ != NULL);
        return ptr_;
    }

    C* get() const { return ptr_; }

    // Allow gscoped_ptr_malloc<C> to be used in boolean expressions, but not
    // implicitly convertible to a real bool (which is dangerous).
    typedef C* gscoped_ptr_malloc::*Testable;
    operator Testable() const { return ptr_ ? &gscoped_ptr_malloc::ptr_ : NULL; }

    // Comparison operators.
    // These return whether a gscoped_ptr_malloc and a plain pointer refer
    // to the same object, not just to two different but equal objects.
    // For compatibility with the boost-derived implementation, these
    // take non-const arguments.
    bool operator==(C* p) const { return ptr_ == p; }

    bool operator!=(C* p) const { return ptr_ != p; }

    // Swap two scoped pointers.
    void swap(gscoped_ptr_malloc& b) {
        C* tmp = b.ptr_;
        b.ptr_ = ptr_;
        ptr_ = tmp;
    }

    // Release a pointer.
    // The return value is the current pointer held by this object.
    // If this object holds a NULL pointer, the return value is NULL.
    // After this operation, this object will hold a NULL pointer,
    // and will not own the object any more.
    C* release() WARN_UNUSED_RESULT {
        C* tmp = ptr_;
        ptr_ = NULL;
        return tmp;
    }

private:
    C* ptr_ = nullptr;

    // no reason to use these: each gscoped_ptr_malloc should have its own object
    template <class C2, class GP>
    bool operator==(gscoped_ptr_malloc<C2, GP> const& p) const;
    template <class C2, class GP>
    bool operator!=(gscoped_ptr_malloc<C2, GP> const& p) const;
};

template <class C, class FP>
void swap(gscoped_ptr_malloc<C, FP>& a, gscoped_ptr_malloc<C, FP>& b) {
    a.swap(b);
}

template <class C, class FP>
bool operator==(C* p, const gscoped_ptr_malloc<C, FP>& b) {
    return p == b.get();
}

template <class C, class FP>
bool operator!=(C* p, const gscoped_ptr_malloc<C, FP>& b) {
    return p != b.get();
}

// A function to convert T* into gscoped_ptr<T>
// Doing e.g. make_gscoped_ptr(new FooBarBaz<type>(arg)) is a shorter notation
// for gscoped_ptr<FooBarBaz<type>>(new FooBarBaz<type>(arg))
template <typename T>
gscoped_ptr<T> make_gscoped_ptr(T* ptr) {
    return gscoped_ptr<T>(ptr);
}
