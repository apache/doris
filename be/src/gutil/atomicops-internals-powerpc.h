// Copyright 2012 Google Inc.
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
// All Rights Reserved.
//
//
// Implementation of atomic operations for PowerPC.  This file should not
// be included directly.  Clients should instead include
// "base/atomicops.h".

// *** WARNING EXPERIMENTAL CODE ***
// This is not tested and may contain bugs.  Until we have bootstrapped
// this.

#ifndef GUTIL_ATOMICOPS_INTERNALS_POWERPC_H_
#define GUTIL_ATOMICOPS_INTERNALS_POWERPC_H_

typedef int32_t Atomic32;
#define BASE_HAS_ATOMIC64 1  // Use only in tests and base/atomic*


#define ATOMICOPS_COMPILER_BARRIER() __asm__ __volatile__("" : : : "memory")

// 32-bit PowerPC is not supported yet.
#ifndef ARCH_POWERPC64
#error "Only PowerPC64 is supported"
#endif

namespace base {
namespace subtle {

typedef int64_t Atomic64;

// sync vs. lwsync:
// 1. lwsync only works in cache enabled memory (system memory).  lwsync is
//    unsuitable for memory caching disabled & guarded (device memory).
//    sync can handle both system and device memory.
// 2. lwsync does not prevent reordering of a store followed by a load if they
//    access different memory.  sync orders all 4 kinds of memory access pairs.

inline void MemoryBarrier() {
  __asm__ __volatile__("sync" : : : "memory");
}

// 32-bit low-level operations.

inline Atomic32 NoBarrier_CompareAndSwap(volatile Atomic32* ptr,
                                         Atomic32 old_value,
                                         Atomic32 new_value) {
  Atomic32 value;
  __asm__ __volatile__("1:\n"
                       " lwarx  %0, 0, %2\n"
                       " cmpw   0, %0, %3\n"
                       " bne-   2f\n"
                       " stwcx. %4, 0, %2\n"
                       " bne-   1b\n"
                       "2:\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(old_value), "r"(new_value)
                       : "cc");
  return value;
}

inline Atomic32 NoBarrier_AtomicExchange(volatile Atomic32* ptr,
                                         Atomic32 new_value) {
  Atomic32 value;
  __asm__ __volatile__("1:\n"
                       " lwarx  %0, 0, %2\n"
                       " stwcx. %3, 0, %2\n"
                       " bne- 1b\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(new_value)
                       : "cc");
  return value;
}

inline Atomic32 Acquire_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  Atomic32 value = NoBarrier_AtomicExchange(ptr, new_value);
  MemoryBarrier();
  return value;
}

inline Atomic32 Release_AtomicExchange(volatile Atomic32* ptr,
                                       Atomic32 new_value) {
  MemoryBarrier();
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic32 NoBarrier_AtomicIncrement(volatile Atomic32* ptr,
                                          Atomic32 increment) {
  Atomic32 value;
  __asm__ __volatile__("1:\n"
                       " lwarx  %0, 0, %2\n"
                       " add    %0, %0, %3\n"
                       " stwcx. %0, 0, %2\n"
                       " bne- 1b\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(increment)
                       : "cc");
  return value;
}

inline Atomic32 Barrier_AtomicIncrement(volatile Atomic32* ptr,
                                        Atomic32 increment) {
  Atomic32 value;
  __asm__ __volatile__(" lwsync\n"
                       "1:\n"
                       " lwarx  %0, 0, %2\n"
                       " add    %0, %0, %3\n"
                       " stwcx. %0, 0, %2\n"
                       " bne- 1b\n"
                       " lwsync\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(increment)
                       : "cc", "memory");
  return value;
}

inline Atomic32 Acquire_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  Atomic32 value = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
  MemoryBarrier();
  return value;
}

inline Atomic32 Release_CompareAndSwap(volatile Atomic32* ptr,
                                       Atomic32 old_value,
                                       Atomic32 new_value) {
  MemoryBarrier();
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}

inline void NoBarrier_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value;
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

// 64-bit low-level operations.

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  Atomic64 value;
  __asm__ __volatile__("1:\n"
                       " ldarx  %0, 0, %2\n"
                       " cmpd   0, %0, %3\n"
                       " bne-   2f\n"
                       " stdcx. %4, 0, %2\n"
                       " bne-   1b\n"
                       "2:\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(old_value), "r"(new_value)
                       : "cc");
  return value;
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value) {
  Atomic64 value;
  __asm__ __volatile__("1:\n"
                       " ldarx  %0, 0, %2\n"
                       " stdcx. %3, 0, %2\n"
                       " bne- 1b\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(new_value)
                       : "cc");
  return value;
}

inline Atomic64 Acquire_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  Atomic64 value = NoBarrier_AtomicExchange(ptr, new_value);
  MemoryBarrier();
  return value;
}

inline Atomic64 Release_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  MemoryBarrier();
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic64 NoBarrier_AtomicIncrement(volatile Atomic64* ptr,
                                          Atomic64 increment) {
  Atomic64 value;
  __asm__ __volatile__("1:\n"
                       " ldarx  %0, 0, %2\n"
                       " add    %0, %0, %3\n"
                       " stdcx. %0, 0, %2\n"
                       " bne- 1b\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(increment)
                       : "cc");
  return value;
}

inline Atomic64 Barrier_AtomicIncrement(volatile Atomic64* ptr,
                                        Atomic64 increment) {
  Atomic64 value;
  __asm__ __volatile__(" lwsync\n"
                       "1:\n"
                       " ldarx  %0, 0, %2\n"
                       " add    %0, %0, %3\n"
                       " stdcx. %0, 0, %2\n"
                       " bne- 1b\n"
                       " lwsync\n"
                       : "=&r" (value), "+m"(*ptr)
                       : "b"(ptr), "r"(increment)
                       : "cc", "memory");
  return value;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  *ptr = value;
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  *ptr = value;
  MemoryBarrier();
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  MemoryBarrier();
  *ptr = value;
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  return *ptr;
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  Atomic64 value = *ptr;
  MemoryBarrier();
  return value;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  MemoryBarrier();
  return *ptr;
}

inline Atomic64 Acquire_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  Atomic64 value = NoBarrier_CompareAndSwap(ptr, old_value, new_value);
  MemoryBarrier();
  return value;
}

inline Atomic64 Release_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  MemoryBarrier();
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}

}  // namespace subtle
}  // namespace base

#undef ATOMICOPS_COMPILER_BARRIER

#endif  // GUTIL_ATOMICOPS_INTERNALS_POWERPC_H_
