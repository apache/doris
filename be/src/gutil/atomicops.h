// Copyright 2003 Google Inc.
// All Rights Reserved.
//

// For atomic operations on statistics counters, see atomic_stats_counter.h.
// For atomic operations on sequence numbers, see atomic_sequence_num.h.
// For atomic operations on reference counts, see atomic_refcount.h.

// Some fast atomic operations -- typically with machine-dependent
// implementations.  This file may need editing as Google code is
// ported to different architectures.

// The routines exported by this module are subtle.  If you use them, even if
// you get the code right, it will depend on careful reasoning about atomicity
// and memory ordering; it will be less readable, and harder to maintain.  If
// you plan to use these routines, you should have a good reason, such as solid
// evidence that performance would otherwise suffer, or there being no
// alternative.  You should assume only properties explicitly guaranteed by the
// specifications in this file.  You are almost certainly _not_ writing code
// just for the x86; if you assume x86 semantics, x86 hardware bugs and
// implementations on other archtectures will cause your code to break.  If you
// do not know what you are doing, avoid these routines, and use a Mutex.
//
// These following lower-level operations are typically useful only to people
// implementing higher-level synchronization operations like spinlocks,
// mutexes, and condition-variables.  They combine CompareAndSwap(),
// addition, exchange, a load, or a store with appropriate memory-ordering
// instructions.  "Acquire" operations ensure that no later memory access by
// the same thread can be reordered ahead of the operation.  "Release"
// operations ensure that no previous memory access by the same thread can be
// reordered after the operation.  "Barrier" operations have both "Acquire" and
// "Release" semantics.  A MemoryBarrier() has "Barrier" semantics, but does no
// memory access.  "NoBarrier" operations have no barrier:  the CPU is
// permitted to reorder them freely (as seen by other threads), even in ways
// the appear to violate functional dependence, just as it can for any normal
// variable access.
//
// It is incorrect to make direct assignments to/from an atomic variable.
// You should use one of the Load or Store routines.  The NoBarrier
// versions are provided when no barriers are needed:
//   NoBarrier_Store()
//   NoBarrier_Load()
// Although there are currently no compiler enforcement, you are encouraged
// to use these.  Moreover, if you choose to use base::subtle::Atomic64 type,
// you MUST use one of the Load or Store routines to get correct behavior
// on 32-bit platforms.
//
// The intent is eventually to put all of these routines in namespace
// base::subtle

#pragma once

#include <stdint.h>

// ------------------------------------------------------------------------
// Include the platform specific implementations of the types
// and operations listed below.  Implementations are to provide Atomic32
// and Atomic64 operations. If there is a mismatch between intptr_t and
// the Atomic32 or Atomic64 types for a platform, the platform-specific header
// should define the macro, AtomicWordCastType in a clause similar to the
// following:
// #if ...pointers are 64 bits...
// # define AtomicWordCastType base::subtle::Atomic64
// #else
// # define AtomicWordCastType Atomic32
// #endif
// ------------------------------------------------------------------------

#define GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)

#define CLANG_VERSION (__clang_major__ * 10000 + __clang_minor__ * 100 + __clang_patchlevel__)

// ThreadSanitizer provides own implementation of atomicops.
#if defined(THREAD_SANITIZER)
#include "gutil/atomicops-internals-tsan.h"
#elif defined(__GNUC__) && (defined(__i386) || defined(__x86_64__))
#include "gutil/atomicops-internals-x86.h"
#elif defined(__GNUC__) && GCC_VERSION >= 40700
#include "gutil/atomicops-internals-gcc.h"
#elif defined(__clang__) && CLANG_VERSION >= 30400
#include "gutil/atomicops-internals-gcc.h"
#else
#error You need to implement atomic operations for this architecture
#endif

// Signed type that can hold a pointer and supports the atomic ops below, as
// well as atomic loads and stores.  Instances must be naturally-aligned.
typedef intptr_t AtomicWord;

#ifdef AtomicWordCastType
// ------------------------------------------------------------------------
// This section is needed only when explicit type casting is required to
// cast AtomicWord to one of the basic atomic types (Atomic64 or Atomic32).
// It also serves to document the AtomicWord interface.
// ------------------------------------------------------------------------

namespace base {
namespace subtle {

// Atomically execute:
//      result = *ptr;
//      if (*ptr == old_value)
//        *ptr = new_value;
//      return result;
//
// I.e., replace "*ptr" with "new_value" if "*ptr" used to be "old_value".
// Always return the old value of "*ptr"
//
// This routine implies no memory barriers.
inline AtomicWord NoBarrier_CompareAndSwap(volatile AtomicWord* ptr, AtomicWord old_value,
                                           AtomicWord new_value) {
    return NoBarrier_CompareAndSwap(reinterpret_cast<volatile AtomicWordCastType*>(ptr), old_value,
                                    new_value);
}

// Atomically store new_value into *ptr, returning the previous value held in
// *ptr.  This routine implies no memory barriers.
inline AtomicWord NoBarrier_AtomicExchange(volatile AtomicWord* ptr, AtomicWord new_value) {
    return NoBarrier_AtomicExchange(reinterpret_cast<volatile AtomicWordCastType*>(ptr), new_value);
}

inline AtomicWord Acquire_AtomicExchange(volatile AtomicWord* ptr, AtomicWord new_value) {
    return Acquire_AtomicExchange(reinterpret_cast<volatile AtomicWordCastType*>(ptr), new_value);
}

inline AtomicWord Release_AtomicExchange(volatile AtomicWord* ptr, AtomicWord new_value) {
    return Release_AtomicExchange(reinterpret_cast<volatile AtomicWordCastType*>(ptr), new_value);
}

// Atomically increment *ptr by "increment".  Returns the new value of
// *ptr with the increment applied.  This routine implies no memory
// barriers.
inline AtomicWord NoBarrier_AtomicIncrement(volatile AtomicWord* ptr, AtomicWord increment) {
    return NoBarrier_AtomicIncrement(reinterpret_cast<volatile AtomicWordCastType*>(ptr),
                                     increment);
}

inline AtomicWord Barrier_AtomicIncrement(volatile AtomicWord* ptr, AtomicWord increment) {
    return Barrier_AtomicIncrement(reinterpret_cast<volatile AtomicWordCastType*>(ptr), increment);
}

inline AtomicWord Acquire_CompareAndSwap(volatile AtomicWord* ptr, AtomicWord old_value,
                                         AtomicWord new_value) {
    return base::subtle::Acquire_CompareAndSwap(reinterpret_cast<volatile AtomicWordCastType*>(ptr),
                                                old_value, new_value);
}

inline AtomicWord Release_CompareAndSwap(volatile AtomicWord* ptr, AtomicWord old_value,
                                         AtomicWord new_value) {
    return base::subtle::Release_CompareAndSwap(reinterpret_cast<volatile AtomicWordCastType*>(ptr),
                                                old_value, new_value);
}

inline void NoBarrier_Store(volatile AtomicWord* ptr, AtomicWord value) {
    NoBarrier_Store(reinterpret_cast<volatile AtomicWordCastType*>(ptr), value);
}

inline void Acquire_Store(volatile AtomicWord* ptr, AtomicWord value) {
    return base::subtle::Acquire_Store(reinterpret_cast<volatile AtomicWordCastType*>(ptr), value);
}

inline void Release_Store(volatile AtomicWord* ptr, AtomicWord value) {
    return base::subtle::Release_Store(reinterpret_cast<volatile AtomicWordCastType*>(ptr), value);
}

inline AtomicWord NoBarrier_Load(volatile const AtomicWord* ptr) {
    return NoBarrier_Load(reinterpret_cast<volatile const AtomicWordCastType*>(ptr));
}

inline AtomicWord Acquire_Load(volatile const AtomicWord* ptr) {
    return base::subtle::Acquire_Load(reinterpret_cast<volatile const AtomicWordCastType*>(ptr));
}

inline AtomicWord Release_Load(volatile const AtomicWord* ptr) {
    return base::subtle::Release_Load(reinterpret_cast<volatile const AtomicWordCastType*>(ptr));
}

} // namespace subtle
} // namespace base
#endif // AtomicWordCastType

// ------------------------------------------------------------------------
// Commented out type definitions and method declarations for documentation
// of the interface provided by this module.
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// The following are to be deprecated when all uses have been changed to
// use the base::subtle namespace.
// ------------------------------------------------------------------------

#ifdef AtomicWordCastType
// AtomicWord versions to be deprecated
inline AtomicWord Acquire_CompareAndSwap(volatile AtomicWord* ptr, AtomicWord old_value,
                                         AtomicWord new_value) {
    return base::subtle::Acquire_CompareAndSwap(ptr, old_value, new_value);
}

inline AtomicWord Release_CompareAndSwap(volatile AtomicWord* ptr, AtomicWord old_value,
                                         AtomicWord new_value) {
    return base::subtle::Release_CompareAndSwap(ptr, old_value, new_value);
}

inline void Acquire_Store(volatile AtomicWord* ptr, AtomicWord value) {
    return base::subtle::Acquire_Store(ptr, value);
}

inline void Release_Store(volatile AtomicWord* ptr, AtomicWord value) {
    return base::subtle::Release_Store(ptr, value);
}

inline AtomicWord Acquire_Load(volatile const AtomicWord* ptr) {
    return base::subtle::Acquire_Load(ptr);
}

inline AtomicWord Release_Load(volatile const AtomicWord* ptr) {
    return base::subtle::Release_Load(ptr);
}
#endif // AtomicWordCastType

// 32-bit Acquire/Release operations to be deprecated.

inline Atomic32 Acquire_CompareAndSwap(volatile Atomic32* ptr, Atomic32 old_value,
                                       Atomic32 new_value) {
    return base::subtle::Acquire_CompareAndSwap(ptr, old_value, new_value);
}
inline Atomic32 Release_CompareAndSwap(volatile Atomic32* ptr, Atomic32 old_value,
                                       Atomic32 new_value) {
    return base::subtle::Release_CompareAndSwap(ptr, old_value, new_value);
}
inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
    base::subtle::Acquire_Store(ptr, value);
}
inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
    return base::subtle::Release_Store(ptr, value);
}
inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
    return base::subtle::Acquire_Load(ptr);
}
inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
    return base::subtle::Release_Load(ptr);
}

// 64-bit Acquire/Release operations to be deprecated.

inline base::subtle::Atomic64 Acquire_CompareAndSwap(volatile base::subtle::Atomic64* ptr,
                                                     base::subtle::Atomic64 old_value,
                                                     base::subtle::Atomic64 new_value) {
    return base::subtle::Acquire_CompareAndSwap(ptr, old_value, new_value);
}
inline base::subtle::Atomic64 Release_CompareAndSwap(volatile base::subtle::Atomic64* ptr,
                                                     base::subtle::Atomic64 old_value,
                                                     base::subtle::Atomic64 new_value) {
    return base::subtle::Release_CompareAndSwap(ptr, old_value, new_value);
}
inline void Acquire_Store(volatile base::subtle::Atomic64* ptr, base::subtle::Atomic64 value) {
    base::subtle::Acquire_Store(ptr, value);
}
inline void Release_Store(volatile base::subtle::Atomic64* ptr, base::subtle::Atomic64 value) {
    return base::subtle::Release_Store(ptr, value);
}
inline base::subtle::Atomic64 Acquire_Load(volatile const base::subtle::Atomic64* ptr) {
    return base::subtle::Acquire_Load(ptr);
}
inline base::subtle::Atomic64 Release_Load(volatile const base::subtle::Atomic64* ptr) {
    return base::subtle::Release_Load(ptr);
}
