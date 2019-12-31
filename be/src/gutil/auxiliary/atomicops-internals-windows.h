// Copyright 2003 Google Inc.
// All Rights Reserved.
//
//
// Implementation of atomic operations using Windows API
// functions.  This file should not be included directly.  Clients
// should instead include "base/atomicops.h".

#ifndef BASE_AUXILIARY_ATOMICOPS_INTERNALS_WINDOWS_H_
#define BASE_AUXILIARY_ATOMICOPS_INTERNALS_WINDOWS_H_

#include <stdio.h>
#include <stdlib.h>
#include "gutil/basictypes.h"  // For COMPILE_ASSERT

typedef int32 Atomic32;

#if defined(_WIN64)
#define BASE_HAS_ATOMIC64 1  // Use only in tests and base/atomic*
#endif

namespace base {
namespace subtle {

typedef int64 Atomic64;

// On windows, we assume intrinsics and asms are compiler barriers
// This is redefined below if using gcc asms.
#define ATOMICOPS_COMPILER_BARRIER()


// 32-bit low-level operations on any platform

extern "C" {
// We use windows intrinsics when we can (they seem to be supported
// well on MSVC 8.0 and above).  Unfortunately, in some
// environments, <windows.h> and <intrin.h> have conflicting
// declarations of some other intrinsics, breaking compilation:
//   http://connect.microsoft.com/VisualStudio/feedback/details/262047
// Therefore, we simply declare the relevant intrinsics ourself.

// MinGW has a bug in the header files where it doesn't indicate the
// first argument is volatile -- they're not up to date.  See
//   http://readlist.com/lists/lists.sourceforge.net/mingw-users/0/3861.html
// We have to const_cast away the volatile to avoid compiler warnings.
// TODO(user): remove this once MinGW has updated MinGW/include/winbase.h
#if defined(__MINGW32__)
inline LONG FastInterlockedCompareExchange(volatile LONG* ptr,
                                           LONG newval, LONG oldval) {
  return ::InterlockedCompareExchange(const_cast<LONG*>(ptr), newval, oldval);
}
inline LONG FastInterlockedExchange(volatile LONG* ptr, LONG newval) {
  return ::InterlockedExchange(const_cast<LONG*>(ptr), newval);
}
inline LONG FastInterlockedExchangeAdd(volatile LONG* ptr, LONG increment) {
  return ::InterlockedExchangeAdd(const_cast<LONG*>(ptr), increment);
}

#elif _MSC_VER >= 1400   // intrinsics didn't work so well before MSVC 8.0
// Unfortunately, in some environments, <windows.h> and <intrin.h>
// have conflicting declarations of some intrinsics, breaking
// compilation.  So we declare the intrinsics we need ourselves.  See
//   http://connect.microsoft.com/VisualStudio/feedback/details/262047
LONG _InterlockedCompareExchange(volatile LONG* ptr, LONG newval, LONG oldval);
#pragma intrinsic(_InterlockedCompareExchange)
inline LONG FastInterlockedCompareExchange(volatile LONG* ptr,
                                           LONG newval, LONG oldval) {
  return _InterlockedCompareExchange(ptr, newval, oldval);
}

LONG _InterlockedExchange(volatile LONG* ptr, LONG newval);
#pragma intrinsic(_InterlockedExchange)
inline LONG FastInterlockedExchange(volatile LONG* ptr, LONG newval) {
  return _InterlockedExchange(ptr, newval);
}

LONG _InterlockedExchangeAdd(volatile LONG* ptr, LONG increment);
#pragma intrinsic(_InterlockedExchangeAdd)
inline LONG FastInterlockedExchangeAdd(volatile LONG* ptr, LONG increment) {
  return _InterlockedExchangeAdd(ptr, increment);
}

#else
inline LONG FastInterlockedCompareExchange(volatile LONG* ptr,
                                           LONG newval, LONG oldval) {
  return ::InterlockedCompareExchange(ptr, newval, oldval);
}
inline LONG FastInterlockedExchange(volatile LONG* ptr, LONG newval) {
  return ::InterlockedExchange(ptr, newval);
}
inline LONG FastInterlockedExchangeAdd(volatile LONG* ptr, LONG increment) {
  return ::InterlockedExchangeAdd(ptr, increment);
}

#endif  // ifdef __MINGW32__
}  // extern "C"

inline Atomic32 NoBarrier_CompareAndSwap(volatile Atomic32* ptr,
                                         Atomic32 old_value,
                                         Atomic32 new_value) {
  LONG result = FastInterlockedCompareExchange(
      reinterpret_cast<volatile LONG*>(ptr),
      static_cast<LONG>(new_value),
      static_cast<LONG>(old_value));
  return static_cast<Atomic32>(result);
}

inline Atomic32 NoBarrier_AtomicExchange(volatile Atomic32* ptr,
                                         Atomic32 new_value) {
  LONG result = FastInterlockedExchange(
      reinterpret_cast<volatile LONG*>(ptr),
      static_cast<LONG>(new_value));
  return static_cast<Atomic32>(result);
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
  return FastInterlockedExchangeAdd(
      reinterpret_cast<volatile LONG*>(ptr),
      static_cast<LONG>(increment)) + increment;
}

inline Atomic32 NoBarrier_AtomicIncrement(volatile Atomic32* ptr,
                                          Atomic32 increment) {
  return Barrier_AtomicIncrement(ptr, increment);
}

}  // namespace base::subtle
}  // namespace base


// In msvc8/vs2005, winnt.h already contains a definition for
// MemoryBarrier in the global namespace.  Add it there for earlier
// versions and forward to it from within the namespace.
#if !(_MSC_VER && _MSC_VER >= 1400)
inline void MemoryBarrier() {
  Atomic32 value = 0;
  base::subtle::NoBarrier_AtomicExchange(&value, 0);
                        // actually acts as a barrier in thisd implementation
}
#endif

namespace base {
namespace subtle {

inline void MemoryBarrier() {
  ::MemoryBarrier();
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

inline void Acquire_Store(volatile Atomic32* ptr, Atomic32 value) {
  Acquire_AtomicExchange(ptr, value);
}

inline void Release_Store(volatile Atomic32* ptr, Atomic32 value) {
  *ptr = value; // works w/o barrier for current Intel chips as of June 2005
  // See comments in Atomic64 version of Release_Store() below.
}

inline Atomic32 NoBarrier_Load(volatile const Atomic32* ptr) {
  return *ptr;
}

inline Atomic32 Acquire_Load(volatile const Atomic32* ptr) {
  Atomic32 value = *ptr;
  return value;
}

inline Atomic32 Release_Load(volatile const Atomic32* ptr) {
  MemoryBarrier();
  return *ptr;
}

// 64-bit operations

#if defined(_WIN64) || defined(__MINGW64__)

// 64-bit low-level operations on 64-bit platform.

COMPILE_ASSERT(sizeof(Atomic64) == sizeof(PVOID), atomic_word_is_atomic);

// These are the intrinsics needed for 64-bit operations.  Similar to the
// 32-bit case above.

extern "C" {
#if defined(__MINGW64__)
inline PVOID FastInterlockedCompareExchangePointer(volatile PVOID* ptr,
                                                   PVOID newval, PVOID oldval) {
  return ::InterlockedCompareExchangePointer(const_cast<PVOID*>(ptr),
                                             newval, oldval);
}
inline PVOID FastInterlockedExchangePointer(volatile PVOID* ptr, PVOID newval) {
  return ::InterlockedExchangePointer(const_cast<PVOID*>(ptr), newval);
}
inline LONGLONG FastInterlockedExchangeAdd64(volatile LONGLONG* ptr,
                                             LONGLONG increment) {
  return ::InterlockedExchangeAdd64(const_cast<LONGLONG*>(ptr), increment);
}

#elif _MSC_VER >= 1400   // intrinsics didn't work so well before MSVC 8.0
// Like above, we need to declare the intrinsics ourselves.
PVOID _InterlockedCompareExchangePointer(volatile PVOID* ptr,
                                         PVOID newval, PVOID oldval);
#pragma intrinsic(_InterlockedCompareExchangePointer)
inline PVOID FastInterlockedCompareExchangePointer(volatile PVOID* ptr,
                                                   PVOID newval, PVOID oldval) {
  return _InterlockedCompareExchangePointer(const_cast<PVOID*>(ptr),
                                            newval, oldval);
}

PVOID _InterlockedExchangePointer(volatile PVOID* ptr, PVOID newval);
#pragma intrinsic(_InterlockedExchangePointer)
inline PVOID FastInterlockedExchangePointer(volatile PVOID* ptr, PVOID newval) {
  return _InterlockedExchangePointer(const_cast<PVOID*>(ptr), newval);
}

LONGLONG _InterlockedExchangeAdd64(volatile LONGLONG* ptr, LONGLONG increment);
#pragma intrinsic(_InterlockedExchangeAdd64)
inline LONGLONG FastInterlockedExchangeAdd64(volatile LONGLONG* ptr,
                                             LONGLONG increment) {
  return _InterlockedExchangeAdd64(const_cast<LONGLONG*>(ptr), increment);
}

#else
inline PVOID FastInterlockedCompareExchangePointer(volatile PVOID* ptr,
                                                   PVOID newval, PVOID oldval) {
  return ::InterlockedCompareExchangePointer(ptr, newval, oldval);
}
inline PVOID FastInterlockedExchangePointer(volatile PVOID* ptr, PVOID newval) {
  return ::InterlockedExchangePointer(ptr, newval);
}
inline LONGLONG FastInterlockedExchangeAdd64(volatile LONGLONG* ptr,
                                         LONGLONG increment) {
  return ::InterlockedExchangeAdd64(ptr, increment);
}

#endif  // ifdef __MINGW64__
}  // extern "C"

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  PVOID result = FastInterlockedCompareExchangePointer(
    reinterpret_cast<volatile PVOID*>(ptr),
    reinterpret_cast<PVOID>(new_value), reinterpret_cast<PVOID>(old_value));
  return reinterpret_cast<Atomic64>(result);
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value) {
  PVOID result = FastInterlockedExchangePointer(
    reinterpret_cast<volatile PVOID*>(ptr),
    reinterpret_cast<PVOID>(new_value));
  return reinterpret_cast<Atomic64>(result);
}

inline Atomic64 NoBarrier_AtomicIncrement(volatile Atomic64* ptr,
                                          Atomic64 increment) {
  return FastInterlockedExchangeAdd64(
      reinterpret_cast<volatile LONGLONG*>(ptr),
      static_cast<LONGLONG>(increment)) + increment;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  *ptr = value;
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  NoBarrier_AtomicExchange(ptr, value);
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  return *ptr;
}

inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  MemoryBarrier();
  return *ptr;
}

#else  // defined(_WIN64) || defined(__MINGW64__)

// 64-bit low-level operations on 32-bit platform

#if defined(_MSC_VER)
// Windows, 32-bit ABI, with MSVC compiler.

inline Atomic64 NoBarrier_Load(volatile const Atomic64* p) {
  Atomic64 value;
  __asm {
    mov eax, p
    movq mm0, [eax]  // Use mmx reg for 64-bit atomic moves
    movq value, mm0
    emms             // Empty mmx state to enable FP registers
  }
  return value;
}

inline void NoBarrier_Store(volatile Atomic64* p, Atomic64 value) {
  __asm {
    mov eax, p
    movq mm0, value  // Use mmx reg for 64-bit atomic moves
    movq [eax], mm0
    emms             // Empty mmx state to enable FP registers
  }
}

#pragma warning(push)
#pragma warning(disable : 4035) // disable the warning about no return statement
                                // in NoBarrier_CompareAndSwap()

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* p,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  __asm  {
     lea edi, old_value
     lea esi, new_value
     mov eax, [edi]
     mov edx, [edi+4]
     mov ebx, [esi]
     mov ecx, [esi+4]
     mov edi, p
     lock cmpxchg8b [edi]
  }
  // There's no explcit return statement, so the warning is disabled above.
  // The result is returned in edx,eax
}
#pragma warning(pop)

#elif defined(__MINGW32__)
// Windows, 32-bit ABI, with GNU compiler.

#undef ATOMICOPS_COMPILER_BARRIER
#define ATOMICOPS_COMPILER_BARRIER() __asm__ __volatile__("" : : : "memory")

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  Atomic64 value;
  __asm__ __volatile__("movq %1, %%mm0\n\t"  // Use mmx reg for 64-bit atomic
                       "movq %%mm0, %0\n\t"  // moves (ptr could be read-only)
                       "emms\n\t"            // Empty mmx state/Reset FP regs
                       : "=m" (value)
                       : "m" (*ptr)
                       : // mark the FP stack and mmx registers as clobbered
                         "st", "st(1)", "st(2)", "st(3)", "st(4)",
                         "st(5)", "st(6)", "st(7)", "mm0", "mm1",
                         "mm2", "mm3", "mm4", "mm5", "mm6", "mm7");
  return value;
}

inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  __asm__ __volatile__("movq %1, %%mm0\n\t"  // Use mmx reg for 64-bit atomic
                       "movq %%mm0, %0\n\t"  // moves (ptr could be read-only)
                       "emms\n\t"            // Empty mmx state/Reset FP regs
                       : "=m" (*ptr)
                       : "m" (value)
                       : // mark the FP stack and mmx registers as clobbered
                         "st", "st(1)", "st(2)", "st(3)", "st(4)",
                         "st(5)", "st(6)", "st(7)", "mm0", "mm1",
                         "mm2", "mm3", "mm4", "mm5", "mm6", "mm7");
}

inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  Atomic64 prev;
  __asm__ __volatile__("push %%ebx\n\t"
                       "movl (%3), %%ebx\n\t"    // Move 64-bit new_value into
                       "movl 4(%3), %%ecx\n\t"   // ecx:ebx
                       "lock; cmpxchg8b (%1)\n\t"// If edx:eax (old_value) same
                       "pop %%ebx\n\t"
                       : "=A" (prev)             // as contents of ptr:
                       : "D" (ptr),              //   ecx:ebx => ptr
                         "0" (old_value),        // else:
                         "S" (&new_value)        //   old *ptr => edx:eax
                       : "memory", "%ecx");
  return prev;
}

#else
// Windows, 32-bit ABI, but not Microsoft compiler, or GNU compiler.

inline void NotImplementedFatalError(const char *function_name) {
  fprintf(stderr, "64-bit %s() not implemented on this platform\n",
          function_name);
  abort();
}

inline Atomic64 NoBarrier_Load(volatile const Atomic64* ptr) {
  NotImplementedFatalError("NoBarrier_Load(Atomic64 *)");
}
inline void NoBarrier_Store(volatile Atomic64* ptr, Atomic64 value) {
  NotImplementedFatalError("NoBarrier_Store(Atomic64 *, ...)");
}
inline Atomic64 NoBarrier_CompareAndSwap(volatile Atomic64* ptr,
                                         Atomic64 old_value,
                                         Atomic64 new_value) {
  NotImplementedFatalError("NoBarrier_CompareAndSwap(Atomic64 *, ...)");
}
#endif


inline Atomic64 Release_Load(volatile const Atomic64* ptr) {
  MemoryBarrier();
  return NoBarrier_Load(ptr);
}

inline Atomic64 NoBarrier_AtomicExchange(volatile Atomic64* ptr,
                                         Atomic64 new_value) {
  Atomic64 old_value;
  do {
    old_value = NoBarrier_Load(ptr);
  } while (NoBarrier_CompareAndSwap(ptr, old_value, new_value) != old_value);
  return old_value;
}

inline Atomic64 NoBarrier_AtomicIncrement(volatile Atomic64* ptr,
                                          Atomic64 increment) {
  Atomic64 old_value;
  Atomic64 new_value;
  do {
    old_value = NoBarrier_Load(ptr);
    new_value = old_value + increment;
  } while (NoBarrier_CompareAndSwap(ptr, old_value, new_value) != old_value);
  return new_value;
}

inline void Acquire_Store(volatile Atomic64* ptr, Atomic64 value) {
  // acquire and release are implicit in x86 AtomicExchange
  NoBarrier_AtomicExchange(ptr, value);
}

#endif  // defined(_WIN64) || defined(__MINGW64__)

inline Atomic64 Acquire_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  // acquire and release are implicit in x86 AtomicExchange
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic64 Release_AtomicExchange(volatile Atomic64* ptr,
                                       Atomic64 new_value) {
  // acquire and release are implicit in x86 AtomicExchange
  return NoBarrier_AtomicExchange(ptr, new_value);
}

inline Atomic64 Acquire_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  // acquire and release are implicit in x86 CompareAndSwap
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}

inline Atomic64 Release_CompareAndSwap(volatile Atomic64* ptr,
                                       Atomic64 old_value,
                                       Atomic64 new_value) {
  // acquire and release are implicit in x86 CompareAndSwap
  return NoBarrier_CompareAndSwap(ptr, old_value, new_value);
}


inline Atomic64 Barrier_AtomicIncrement(volatile Atomic64* ptr,
                                        Atomic64 increment) {
  // barriers are implicit in atomic increment on on the x86
  return NoBarrier_AtomicIncrement(ptr, increment);
}

inline Atomic64 Acquire_Load(volatile const Atomic64* ptr) {
  Atomic64 result = NoBarrier_Load(ptr);  // acquire is implicit in x86 loads
  ATOMICOPS_COMPILER_BARRIER();
  return result;
}

inline void Release_Store(volatile Atomic64* ptr, Atomic64 value) {
  ATOMICOPS_COMPILER_BARRIER();
  NoBarrier_Store(ptr, value);   // release is implicit on x86 stores
}

#undef ATOMICOPS_COMPILER_BARRIER

}  // namespace base::subtle
}  // namespace base

#endif  // BASE_AUXILIARY_ATOMICOPS_INTERNALS_WINDOWS_H_
