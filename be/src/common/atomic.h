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

#ifndef DORIS_BE_SRC_COMMON_ATOMIC_H
#define DORIS_BE_SRC_COMMON_ATOMIC_H

#include <algorithm>

#include "common/compiler_util.h"
#include "gutil/atomicops.h"
#include "gutil/macros.h"

namespace doris {

class AtomicUtil {
public:
    // Issues instruction to have the CPU wait, this is less busy (bus traffic
    // etc) than just spinning.
    // For example:
    //  while (1);
    // should be:
    //  while (1) CpuWait();
    static ALWAYS_INLINE void cpu_wait() {
#if (defined(__i386) || defined(__x86_64__))
        asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
        asm volatile("yield\n" ::: "memory");
#endif
    }

    /// Provides "barrier" semantics (see below) without a memory access.
    static ALWAYS_INLINE void memory_barrier() { __sync_synchronize(); }

    /// Provides a compiler barrier. The compiler is not allowed to reorder memory
    /// accesses across this (but the CPU can).  This generates no instructions.
    static ALWAYS_INLINE void compiler_barrier() { __asm__ __volatile__("" : : : "memory"); }
};

// Wrapper for atomic integers.  This should be switched to c++ 11 when
// we can switch.
// This class overloads operators to behave like a regular integer type
// but all operators and functions are thread safe.
template <typename T>
class AtomicInt {
public:
    AtomicInt(T initial) : _value(initial) {}
    AtomicInt() : _value(0) {}

    operator T() const { return _value; }

    AtomicInt& operator=(T val) {
        _value = val;
        return *this;
    }
    AtomicInt& operator=(const AtomicInt<T>& val) {
        _value = val._value;
        return *this;
    }

    AtomicInt& operator+=(T delta) {
        __sync_add_and_fetch(&_value, delta);
        return *this;
    }
    AtomicInt& operator-=(T delta) {
        __sync_add_and_fetch(&_value, -delta);
        return *this;
    }

    AtomicInt& operator|=(T v) {
        __sync_or_and_fetch(&_value, v);
        return *this;
    }
    AtomicInt& operator&=(T v) {
        __sync_and_and_fetch(&_value, v);
        return *this;
    }

    // These define the preIncrement (i.e. --value) operators.
    AtomicInt& operator++() {
        __sync_add_and_fetch(&_value, 1);
        return *this;
    }
    AtomicInt& operator--() {
        __sync_add_and_fetch(&_value, -1);
        return *this;
    }

    // This is post increment, which needs to return a new object.
    AtomicInt<T> operator++(int) {
        T prev = __sync_fetch_and_add(&_value, 1);
        return AtomicInt<T>(prev);
    }
    AtomicInt<T> operator--(int) {
        T prev = __sync_fetch_and_add(&_value, -1);
        return AtomicInt<T>(prev);
    }

    // Safe read of the value
    T read() { return __sync_fetch_and_add(&_value, 0); }

    /// Atomic load with "acquire" memory-ordering semantic.
    ALWAYS_INLINE T load() const { return base::subtle::Acquire_Load(&_value); }

    /// Atomic store with "release" memory-ordering semantic.
    ALWAYS_INLINE void store(T x) { base::subtle::Release_Store(&_value, x); }

    /// Atomic add with "barrier" memory-ordering semantic. Returns the new value.
    ALWAYS_INLINE T add(T x) { return base::subtle::Barrier_AtomicIncrement(&_value, x); }

    // Increments by delta (i.e. += delta) and returns the new val
    T update_and_fetch(T delta) { return __sync_add_and_fetch(&_value, delta); }

    // Increment by delta and returns the old val
    T fetch_and_update(T delta) { return __sync_fetch_and_add(&_value, delta); }

    // Updates the int to 'value' if value is larger
    void update_max(T value) {
        while (true) {
            T old_value = _value;
            T new_value = std::max(old_value, value);
            if (LIKELY(compare_and_swap(old_value, new_value))) {
                break;
            }
        }
    }
    void update_min(T value) {
        while (true) {
            T old_value = _value;
            T new_value = std::min(old_value, value);
            if (LIKELY(compare_and_swap(old_value, new_value))) {
                break;
            }
        }
    }

    // Returns true if the atomic compare-and-swap was successful.
    // If _value == oldVal, make _value = new_val and return true; otherwise return false;
    bool compare_and_swap(T old_val, T new_val) {
        return __sync_bool_compare_and_swap(&_value, old_val, new_val);
    }

    // Returns the content of _value before the operation.
    // If returnValue == old_val, then the atomic compare-and-swap was successful.
    T compare_and_swap_val(T old_val, T new_val) {
        return __sync_val_compare_and_swap(&_value, old_val, new_val);
    }

    // Atomically updates _value with new_val. Returns the old _value.
    T swap(const T& new_val) { return __sync_lock_test_and_set(&_value, new_val); }

private:
    T _value;
};

/// Supported atomic types. Use these types rather than referring to AtomicInt<>
/// directly.
typedef AtomicInt<int32_t> AtomicInt32;
typedef AtomicInt<int64_t> AtomicInt64;

/// Atomic pointer. Operations have the same semantics as AtomicInt.
template <typename T>
class AtomicPtr {
public:
    AtomicPtr(T* initial = nullptr) : _ptr(reinterpret_cast<intptr_t>(initial)) {}

    /// Atomic load with "acquire" memory-ordering semantic.
    inline T* load() const { return reinterpret_cast<T*>(_ptr.load()); }

    /// Atomic store with "release" memory-ordering semantic.
    inline void store(T* val) { _ptr.store(reinterpret_cast<intptr_t>(val)); }

    /// Store 'new_val' and return the previous value. Implies a Release memory barrier
    /// (i.e. the same as Store()).
    inline T* swap(T* val) {
        return reinterpret_cast<T*>(_ptr.swap(reinterpret_cast<intptr_t>(val)));
    }

private:
    AtomicInt<intptr_t> _ptr;
};

} // end namespace doris

#endif // DORIS_BE_SRC_COMMON_ATOMIC_H
