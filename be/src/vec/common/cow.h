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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/COW.h
// and modified by Doris

#pragma once

#include <atomic>
#include <initializer_list>

/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:
    class Column : public COW<Column>
    {
    private:
        friend class COW<Column>;
        /// Leave all constructors in private section. They will be available through 'create' method.
        Column();
        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.
        virtual ~Column() {}
    };
  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;
    /// Now x and y are shared.
    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = std::move(*x).mutate();
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }
    /// Now x and y are unshared and have different values.
  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COW is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
template <typename Derived>
class COW {
    std::atomic_uint ref_counter;

protected:
    COW() : ref_counter(0) {}

    COW(COW const&) : ref_counter(0) {}

    COW& operator=(COW const&) { return *this; }

    void add_ref() { ++ref_counter; }

    void release_ref() {
        if (--ref_counter == 0) {
            delete static_cast<const Derived*>(this);
        }
    }

    Derived* derived() { return static_cast<Derived*>(this); }

    const Derived* derived() const { return static_cast<const Derived*>(this); }

    template <typename T>
    class intrusive_ptr {
    public:
        intrusive_ptr() : t(nullptr) {}

        intrusive_ptr(T* t, bool add_ref = true) : t(t) {
            if (t && add_ref) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }

        template <typename U>
        intrusive_ptr(intrusive_ptr<U> const& rhs) : t(rhs.get()) {
            if (t) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }

        intrusive_ptr(intrusive_ptr const& rhs) : t(rhs.get()) {
            if (t) {
                ((std::remove_const_t<T>*)t)->add_ref();
            }
        }

        ~intrusive_ptr() {
            if (t) {
                ((std::remove_const_t<T>*)t)->release_ref();
            }
        }

        template <typename U>
        intrusive_ptr& operator=(intrusive_ptr<U> const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        intrusive_ptr(intrusive_ptr&& rhs) : t(rhs.t) { rhs.t = nullptr; }

        intrusive_ptr& operator=(intrusive_ptr&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr&&>(rhs)).swap(*this);
            return *this;
        }

        template <class U>
        friend class intrusive_ptr;

        template <class U>
        intrusive_ptr(intrusive_ptr<U>&& rhs) : t(rhs.t) {
            rhs.t = nullptr;
        }

        template <class U>
        intrusive_ptr& operator=(intrusive_ptr<U>&& rhs) {
            intrusive_ptr(static_cast<intrusive_ptr<U>&&>(rhs)).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(intrusive_ptr const& rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        intrusive_ptr& operator=(T* rhs) {
            intrusive_ptr(rhs).swap(*this);
            return *this;
        }

        void reset() { intrusive_ptr().swap(*this); }

        void reset(T* rhs) { intrusive_ptr(rhs).swap(*this); }

        void reset(T* rhs, bool add_ref) { intrusive_ptr(rhs, add_ref).swap(*this); }

        T* get() const { return t; }

        T* detach() {
            T* ret = t;
            t = nullptr;
            return ret;
        }

        void swap(intrusive_ptr& rhs) {
            T* tmp = t;
            t = rhs.t;
            rhs.t = tmp;
        }

        T& operator*() const& { return *t; }

        T&& operator*() const&& { return const_cast<std::remove_const_t<T>&&>(*t); }

        T* operator->() const { return t; }

        operator bool() const { return t != nullptr; }

        operator T*() const { return t; }

    private:
        T* t;
    };

protected:
    template <typename T>
    class mutable_ptr : public intrusive_ptr<T> {
    private:
        using Base = intrusive_ptr<T>;

        template <typename>
        friend class COW;
        template <typename, typename>
        friend class COWHelper;

        explicit mutable_ptr(T* ptr) : Base(ptr) {}

    public:
        /// Copy: not possible.
        mutable_ptr(const mutable_ptr&) = delete;

        /// Move: ok.
        mutable_ptr(mutable_ptr&&) = default;
        mutable_ptr& operator=(mutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        mutable_ptr() = default;

        mutable_ptr(std::nullptr_t) {}
    };

public:
    using MutablePtr = mutable_ptr<Derived>;

    unsigned int use_count() const { return ref_counter.load(); }

protected:
    template <typename T>
    class immutable_ptr : public intrusive_ptr<const T> {
    private:
        using Base = intrusive_ptr<const T>;

        template <typename>
        friend class COW;
        template <typename, typename>
        friend class COWHelper;

        explicit immutable_ptr(const T* ptr) : Base(ptr) {}

    public:
        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr&) = default;
        immutable_ptr& operator=(const immutable_ptr&) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U>& other) : Base(other) {}

        /// Move: ok.
        immutable_ptr(immutable_ptr&&) = default;
        immutable_ptr& operator=(immutable_ptr&&) = default;

        /// Initializing from temporary of compatible type.
        template <typename U>
        immutable_ptr(immutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Move from mutable ptr: ok.
        template <typename U>
        immutable_ptr(mutable_ptr<U>&& other) : Base(std::move(other)) {}

        /// Copy from mutable ptr: not possible.
        template <typename U>
        immutable_ptr(const mutable_ptr<U>&) = delete;

        immutable_ptr() = default;

        immutable_ptr(std::nullptr_t) {}
    };

public:
    using Ptr = immutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return create(std::forward<std::initializer_list<T>>(arg));
    }

public:
    Ptr get_ptr() const { return Ptr(derived()); }
    MutablePtr get_ptr() { return MutablePtr(derived()); }

protected:
    MutablePtr shallow_mutate() const {
        if (this->use_count() > 1) {
            return derived()->clone();
        } else {
            return assume_mutable();
        }
    }

public:
    MutablePtr mutate() const&& { return shallow_mutate(); }

    MutablePtr assume_mutable() const { return const_cast<COW*>(this)->get_ptr(); }

    Derived& assume_mutable_ref() const { return const_cast<Derived&>(*derived()); }

protected:
    /// It works as immutable_ptr if it is const and as mutable_ptr if it is non const.
    template <typename T>
    class chameleon_ptr {
    private:
        immutable_ptr<T> value;

    public:
        template <typename... Args>
        chameleon_ptr(Args&&... args) : value(std::forward<Args>(args)...) {}

        template <typename U>
        chameleon_ptr(std::initializer_list<U>&& arg)
                : value(std::forward<std::initializer_list<U>>(arg)) {}

        const T* get() const { return value.get(); }
        T* get() { return &value->assume_mutable_ref(); }

        const T* operator->() const { return get(); }
        T* operator->() { return get(); }

        const T& operator*() const { return *value; }
        T& operator*() { return value->assume_mutable_ref(); }

        operator const immutable_ptr<T>&() const { return value; }
        operator immutable_ptr<T>&() { return value; }

        operator bool() const { return value != nullptr; }
        bool operator!() const { return value == nullptr; }

        bool operator==(const chameleon_ptr& rhs) const { return value == rhs.value; }
        bool operator!=(const chameleon_ptr& rhs) const { return value != rhs.value; }
    };

public:
    /** Use this type in class members for compositions.
      *
      * NOTE:
      * For classes with WrappedPtr members,
      * you must reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
      * It will guarantee, that mutable object have all subobjects unshared.
      *
      * NOTE:
      * If you override 'mutate' method in inherited classes, don't forget to make it virtual in base class or to make it call a virtual method.
      * (COW itself doesn't force any methods to be virtual).
      *
      * See example in "cow_compositions.cpp".
      */
    using WrappedPtr = chameleon_ptr<Derived>;
};

/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COW<IColumn>
  * {
  *     friend class COW<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  *
  * See example in "cow_columns.cpp".
  */
template <typename Base, typename Derived>
class COWHelper : public Base {
public:
    using Ptr = typename Base::template immutable_ptr<Derived>;
    using MutablePtr = typename Base::template mutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    typename Base::MutablePtr clone() const override {
        return typename Base::MutablePtr(new Derived(static_cast<const Derived&>(*this)));
    }

protected:
    MutablePtr shallow_mutate() const {
        return MutablePtr(static_cast<Derived*>(Base::shallow_mutate().get()));
    }
};