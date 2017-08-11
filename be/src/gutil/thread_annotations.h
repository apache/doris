// Copyright (c) 2008, Google Inc.
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
// This header file contains the macro definitions for thread safety
// annotations that allow the developers to document the locking policies
// of their multi-threaded code. The annotations can also help program
// analysis tools to identify potential thread safety issues.
//
//
// The annotations are implemented using GCC's "attributes" extension.
// Using the macros defined here instead of the raw GCC attributes allows
// for portability and future compatibility.
//

#ifndef BASE_THREAD_ANNOTATIONS_H_
#define BASE_THREAD_ANNOTATIONS_H_


#if defined(__GNUC__) && defined(__SUPPORT_TS_ANNOTATION__) && !defined(SWIG)
#define THREAD_ANNOTATION_ATTRIBUTE__(x)   __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x)   // no-op
#endif

#if defined(__GNUC__) && !defined(__clang__)

// Document if a shared variable/field needs to be protected by a lock.
// GUARDED_BY allows the user to specify a particular lock that should be
// held when accessing the annotated variable, while GUARDED_VAR only
// indicates a shared variable should be guarded (by any lock). GUARDED_VAR
// is primarily used when the client cannot express the name of the lock.
#define GUARDED_BY(x)          THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#define GUARDED_VAR            THREAD_ANNOTATION_ATTRIBUTE__(guarded)

// Document if the memory location pointed to by a pointer should be guarded
// by a lock when dereferencing the pointer. Similar to GUARDED_VAR,
// PT_GUARDED_VAR is primarily used when the client cannot express the name
// of the lock. Note that a pointer variable to a shared memory location
// could itself be a shared variable. For example, if a shared global pointer
// q, which is guarded by mu1, points to a shared memory location that is
// guarded by mu2, q should be annotated as follows:
//     int *q GUARDED_BY(mu1) PT_GUARDED_BY(mu2);
#define PT_GUARDED_BY(x) \
  THREAD_ANNOTATION_ATTRIBUTE__(point_to_guarded_by(x))
#define PT_GUARDED_VAR \
  THREAD_ANNOTATION_ATTRIBUTE__(point_to_guarded)

// Document the acquisition order between locks that can be held
// simultaneously by a thread. For any two locks that need to be annotated
// to establish an acquisition order, only one of them needs the annotation.
// (i.e. You don't have to annotate both locks with both ACQUIRED_AFTER
// and ACQUIRED_BEFORE.)
#define ACQUIRED_AFTER(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))
#define ACQUIRED_BEFORE(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

// The following three annotations document the lock requirements for
// functions/methods.

// Document if a function expects certain locks to be held before it is called
#define EXCLUSIVE_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))

#define SHARED_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))

// Document the locks acquired in the body of the function. These locks
// non-reentrant).
#define LOCKS_EXCLUDED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

// Document the lock the annotated function returns without acquiring it.
#define LOCK_RETURNED(x)       THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

// Document if a class/type is a lockable type (such as the Mutex class).
#define LOCKABLE               THREAD_ANNOTATION_ATTRIBUTE__(lockable)

// Document if a class is a scoped lockable type (such as the MutexLock class).
#define SCOPED_LOCKABLE        THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

// The following annotations specify lock and unlock primitives.
#define EXCLUSIVE_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock(__VA_ARGS__))

#define SHARED_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_lock(__VA_ARGS__))

#define EXCLUSIVE_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock(__VA_ARGS__))

#define SHARED_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock(__VA_ARGS__))

#define UNLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(unlock(__VA_ARGS__))

// An escape hatch for thread safety analysis to ignore the annotated function.
#define NO_THREAD_SAFETY_ANALYSIS \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// Used to mark functions that need to be fixed, because they are producing 
// thread safety warnings.  This macro is intended primarily for use by the 
// compiler team; it allows new thread safety warnings to be rolled out 
// without breaking existing code.  Code which triggers the new warnings are 
// marked with a FIXME, and referred back to the code owners to fix.
#define NO_THREAD_SAFETY_ANALYSIS_FIXME \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// NO_THREAD_SAFETY_ANALYSIS_OPT turns off thread-safety checking in the
// annotated function in opt (NDEBUG) mode.  It is for use specifically when
// the thread-safety checker is failing in opt mode on an otherwise correct
// piece of code.
#ifdef NDEBUG
#define NO_THREAD_SAFETY_ANALYSIS_OPT NO_THREAD_SAFETY_ANALYSIS
#else
#define NO_THREAD_SAFETY_ANALYSIS_OPT
#endif

// TS_UNCHECKED should be placed around lock expressions that are not valid 
// C++ syntax, but which are present for documentation purposes.  The 
// expressions are passed unchanged to gcc, which will usually treat them 
// as the universal lock.  
#define TS_UNCHECKED(x) x

// TS_FIXME is used to mark lock expressions that are not valid C++ syntax.  
// This annotation should eventually be either fixed, or changed to 
// TS_UNCHECKED.  
#define TS_FIXME(x) x

// This is used to pass different annotations to gcc and clang, in cases where 
// gcc would reject a lock expression (e.g. &MyClass::mu_) that is accepted
// by clang.  This is seldom needed, since GCC usually ignores invalid lock
// expressions except in certain cases, such as LOCK_RETURNED. 
#define TS_CLANG_ONLY(CLANG_EXPR, GCC_EXPR) GCC_EXPR

// Clang Attributes
// The names of attributes in the clang analysis are slightly different
#else

#define GUARDED_BY(x) \
  THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define GUARDED_VAR \
  THREAD_ANNOTATION_ATTRIBUTE__(guarded)

#define PT_GUARDED_BY(x) \
  THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))
  
#define PT_GUARDED_VAR \
  THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded)

#define ACQUIRED_AFTER(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))
  
#define ACQUIRED_BEFORE(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define EXCLUSIVE_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))
  
#define SHARED_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))

#define LOCKS_EXCLUDED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define LOCK_RETURNED(x) \
  THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define LOCKABLE \
  THREAD_ANNOTATION_ATTRIBUTE__(lockable)

#define SCOPED_LOCKABLE \
  THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define EXCLUSIVE_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock_function(__VA_ARGS__))
  
#define SHARED_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_lock_function(__VA_ARGS__))
  
#define EXCLUSIVE_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock_function(__VA_ARGS__))
  
#define SHARED_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock_function(__VA_ARGS__))
  
#define UNLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))

#define NO_THREAD_SAFETY_ANALYSIS \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

#define NO_THREAD_SAFETY_ANALYSIS_OPT

#define NO_THREAD_SAFETY_ANALYSIS_FIXME \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

#define TS_UNCHECKED(x) ""

#define TS_FIXME(x) ""

#define TS_CLANG_ONLY(CLANG_EXPR, GCC_EXPR) CLANG_EXPR

#endif  // defined(__clang__)

#endif  // BASE_THREAD_ANNOTATIONS_H_
