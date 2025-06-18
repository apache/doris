// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#ifndef BASE_EXPORT
#define BASE_EXPORT
#endif

// A helper class alongside macros to be used to verify assumptions about thread
// safety of a class.
//
// Example: Queue implementation non thread-safe but still usable if clients
//          are synchronized somehow.
//
//          In this case the macro DFAKE_SCOPED_LOCK has to be
//          used, it checks that if a thread is inside the push/pop then
//          noone else is still inside the pop/push
//
// class NonThreadSafeQueue {
//  public:
//   ...
//   void push(int) { DFAKE_SCOPED_LOCK(push_pop_); ... }
//   int pop() { DFAKE_SCOPED_LOCK(push_pop_); ... }
//   ...
//  private:
//   DFAKE_MUTEX(push_pop_);
// };
//
//
// Example: Queue implementation non thread-safe but still usable if clients
//          are synchronized somehow, it calls a method to "protect" from
//          a "protected" method
//
//          In this case the macro DFAKE_SCOPED_RECURSIVE_LOCK
//          has to be used, it checks that if a thread is inside the push/pop
//          then noone else is still inside the pop/push
//
// class NonThreadSafeQueue {
//  public:
//   void push(int) {
//     DFAKE_SCOPED_LOCK(push_pop_);
//     ...
//   }
//   int pop() {
//     DFAKE_SCOPED_RECURSIVE_LOCK(push_pop_);
//     bar();
//     ...
//   }
//   void bar() { DFAKE_SCOPED_RECURSIVE_LOCK(push_pop_); ... }
//   ...
//  private:
//   DFAKE_MUTEX(push_pop_);
// };
//
//
// Example: Queue implementation not usable even if clients are synchronized,
//          so only one thread in the class life cycle can use the two members
//          push/pop.
//
//          In this case the macro DFAKE_SCOPED_LOCK_THREAD_LOCKED pins the
//          specified
//          critical section the first time a thread enters push or pop, from
//          that time on only that thread is allowed to execute push or pop.
//
// class NonThreadSafeQueue {
//  public:
//   ...
//   void push(int) { DFAKE_SCOPED_LOCK_THREAD_LOCKED(push_pop_); ... }
//   int pop() { DFAKE_SCOPED_LOCK_THREAD_LOCKED(push_pop_); ... }
//   ...
//  private:
//   DFAKE_MUTEX(push_pop_);
// };
//
//
// Example: Class that has to be contructed/destroyed on same thread, it has
//          a "shareable" method (with external synchronization) and a not
//          shareable method (even with external synchronization).
//
//          In this case 3 Critical sections have to be defined
//
// class ExoticClass {
//  public:
//   ExoticClass() { DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_); ... }
//   ~ExoticClass() { DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_); ... }
//
//   void Shareable() { DFAKE_SCOPED_LOCK(shareable_section_); ... }
//   void NotShareable() { DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_); ... }
//   ...
//  private:
//   DFAKE_MUTEX(ctor_dtor_);
//   DFAKE_MUTEX(shareable_section_);
// };

#if !defined(NDEBUG)

// Defines a class member that acts like a mutex. It is used only as a
// verification tool.
#define DFAKE_MUTEX(obj) mutable base::ThreadCollisionWarner obj
// Asserts the call is never called simultaneously in two threads. Used at
// member function scope.
#define DFAKE_SCOPED_LOCK(obj) base::ThreadCollisionWarner::ScopedCheck s_check_##obj(&obj)
// Asserts the call is never called simultaneously in two threads. Used at
// member function scope. Same as DFAKE_SCOPED_LOCK but allows recursive locks.
#define DFAKE_SCOPED_RECURSIVE_LOCK(obj) \
    base::ThreadCollisionWarner::ScopedRecursiveCheck sr_check_##obj(&obj)
// Asserts the code is always executed in the same thread.
#define DFAKE_SCOPED_LOCK_THREAD_LOCKED(obj) base::ThreadCollisionWarner::Check check_##obj(&obj)

#else

#define DFAKE_MUTEX(obj) typedef void InternalFakeMutexType##obj
#define DFAKE_SCOPED_LOCK(obj) ((void)0)
#define DFAKE_SCOPED_RECURSIVE_LOCK(obj) ((void)0)
#define DFAKE_SCOPED_LOCK_THREAD_LOCKED(obj) ((void)0)

#endif
