// Copyright 2003 Google Inc.
// All Rights Reserved.
//
//
// This file is an internal atomic implementation, use base/atomicops.h instead.
//
// LinuxKernelCmpxchg and Barrier_AtomicIncrement are from Google Gears.

#ifndef BASE_AUXILIARY_ATOMICOPS_INTERNALS_ARM_GENERIC_H_
#define BASE_AUXILIARY_ATOMICOPS_INTERNALS_ARM_GENERIC_H_

#include <stdio.h>
#include <stdlib.h>
#include "gutil/macros.h"  // For COMPILE_ASSERT
#include "gutil/port.h"  // ATTRIBUTE_WEAK

typedef int32_t Atomic32;

namespace base {
namespace subtle {

typedef int64_t Atomic64;

// 0xffff0fc0 is the hard coded address of a function provided by
// the kernel which implements an atomic compare-exchange. On older
// ARM architecture revisions (pre-v6) this may be implemented using
// a syscall. This address is stable, and in active use (hard coded)
// by at least glibc-2.7 and the Android C library.
// pLinuxKernelCmpxchg has both acquire and release barrier sematincs.
typedef Atomic32 (*LinuxKernelCmpxchgFunc)(Atomic32 old_value,
                                           Atomic32 new_value,
                                           volatile Atomic32* ptr);
LinuxKernelCmpxchgFunc pLinuxKernelCmpxchg ATTRIBUTE_WEAK =
    (LinuxKernelCmpxchgFunc) 0xffff0fc0;

typedef void (*LinuxKernelMemoryBarrierFunc)(void);
LinuxKernelMemoryBarrierFunc pLinuxKernelMemoryBarrier ATTRIBUTE_WEAK =
    (LinuxKernelMemoryBarrierFunc) 0xffff0fa0;


inline Atomic32 NoBarrier_CompareAndSwap(volatile Atomic32* ptr,
                                         Atomic32 old_value,
                                         Atomic32 new_value) {
  Atomic32 prev_value = *ptr;
  do {
    if (!pLinuxKernelCmpxchg(old_value, new_value,
                             const_cast<Atomic32*>(ptr))) {
      return old_value;
    }
    prev_value = *ptr;
  } while (prev_value == old_value);
  return prev_value;
}

inline Atomic32 NoBarrier_AtomicExchange(volatile Atomic32* ptr,
                                         Atomic32 new_value) {
  Atomic32 old_value;
  do {
    old_value = *ptr;
  } while (pLinuxKernelCmpxchg(old_value, new_value,
                               const_cast<Atomic32*>(ptr)));
  return old_value;
}

inline Atomic32 Acquire_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic32 Release_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic32 Barrier_AtomicIncrement(volatile Atomic32* ptr,
                                        Atomic32 increment) {
  for (;;) {
    // Atomic exchange the old value with an incremented one.
    Atomic32 old_value = *ptr;
    Atomic32 new_value = old_value + increment;
    if (pLinuxKernelCmpxchg(old_value, new_value,
                            const_cast<Atomic32*>(ptr)) == 0) {
      // The exchange took place as expected.
      return new_value;
    }
    // Otherwise, *ptr changed mid-loop and we need to retry.
  }
}

inline Atomic32 NoBarrier_AtomicIncrement(volatile Atomic32* ptr,
                                          Atomic32 increment) {
  return Barrier_AtomicIncrement(ptr, increment);
}

inline Atomic32 Acquire_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}

inline Atomic32 Release_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}

inline void NoBarrier_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value;
}

inline void MemoryBarrier() {
  pLinuxKernelMemoryBarrier();
}

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value;
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
  MemoryBarrier();
  *ptr = value;
}

inline Atomic32 NoBarrier_Load(volatile const Atomic32* ptr) {
  return *ptr;
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
  Atomic32 value = *ptr;
  MemoryBarrier();
  return value;
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
  MemoryBarrier();
  return *ptr;
}


// 64-bit versions are not implemented yet.

inline void NotImplementedFatalError(const char *function_name) {
  fprintf(stderr, "64-bit %s() not implemented on this platform\n",
          function_name);
  abort();
}

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  NotImplementedFatalError("NoBarrier_CompareAndSwap");
  return 0;
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value) {
  NotImplementedFatalError("NoBarrier_AtomicExchange");
  return 0;
}

inline Atomic64 Acquire_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  NotImplementedFatalError("Acquire_AtomicExchange");
  return 0;
}

inline Atomic64 Release_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  NotImplementedFatalError("Release_AtomicExchange");
  return 0;
}

inline Atomic64 NoBarrier_AtomicIncrement(volatile Atomic64* ptr,
                                          Atomic64 increment) {
  NotImplementedFatalError("NoBarrier_AtomicIncrement");
  return 0;
}

inline Atomic64 Barrier_AtomicIncrement(volatile Atomic64* ptr,
                                        Atomic64 increment) {
  NotImplementedFatalError("Barrier_AtomicIncrement");
  return 0;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  NotImplementedFatalError("NoBarrier_Store");
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  NotImplementedFatalError("Acquire_Store64");
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  NotImplementedFatalError("Release_Store");
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  NotImplementedFatalError("NoBarrier_Load");
  return 0;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  NotImplementedFatalError("Atomic64 Acquire_Load");
  return 0;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  NotImplementedFatalError("Atomic64 Release_Load");
  return 0;
}

inline Atomic64 Acquire_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  NotImplementedFatalError("Atomic64 Acquire_CompareAndSwap");
  return 0;
}

inline Atomic64 Release_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  NotImplementedFatalError("Atomic64 Release_CompareAndSwap");
  return 0;
}

}  // namespace base::subtle
}  // namespace base

#endif  // BASE_AUXILIARY_ATOMICOPS_INTERNALS_ARM_GENERIC_H_
