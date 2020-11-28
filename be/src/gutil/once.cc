// Copyright 2008 Google Inc. All Rights Reserved.

#include "gutil/once.h"

#include <common/logging.h>

#include "gutil/dynamic_annotations.h"
#include "gutil/spinlock_internal.h"

// All modifications to a GoogleOnceType occur inside GoogleOnceInternalInit.
// The fast path reads the variable with an acquire-load..
// This is safe provided we always perform a memory barrier
// immediately before setting the value to GOOGLE_ONCE_INTERNAL_DONE.

void GoogleOnceInternalInit(Atomic32* control, void (*func)(), void (*func_with_arg)(void*),
                            void* arg) {
    // if (DEBUG_MODE) {
    //   int32 old_control = base::subtle::Acquire_Load(control);
    //   if (old_control != GOOGLE_ONCE_INTERNAL_INIT &&
    //       old_control != GOOGLE_ONCE_INTERNAL_RUNNING &&
    //       old_control != GOOGLE_ONCE_INTERNAL_WAITER &&
    //       old_control != GOOGLE_ONCE_INTERNAL_DONE) {
    //     LOG(FATAL) << "Either GoogleOnceType is used in non-static storage "
    //                   "(where GoogleOnceDynamic might be appropriate), "
    //                   "or there's a memory corruption.";
    //   }
    // }
    static const base::internal::SpinLockWaitTransition trans[] = {
            {GOOGLE_ONCE_INTERNAL_INIT, GOOGLE_ONCE_INTERNAL_RUNNING, true},
            {GOOGLE_ONCE_INTERNAL_RUNNING, GOOGLE_ONCE_INTERNAL_WAITER, false},
            {GOOGLE_ONCE_INTERNAL_DONE, GOOGLE_ONCE_INTERNAL_DONE, true}};
    // Short circuit the simplest case to avoid procedure call overhead.
    if (base::subtle::Acquire_CompareAndSwap(control, GOOGLE_ONCE_INTERNAL_INIT,
                                             GOOGLE_ONCE_INTERNAL_RUNNING) ==
                GOOGLE_ONCE_INTERNAL_INIT ||
        base::internal::SpinLockWait(control, ARRAYSIZE(trans), trans) ==
                GOOGLE_ONCE_INTERNAL_INIT) {
        if (func != nullptr) {
            (*func)();
        } else {
            (*func_with_arg)(arg);
        }
        ANNOTATE_HAPPENS_BEFORE(control);
        int32 old_control = base::subtle::NoBarrier_Load(control);
        base::subtle::Release_Store(control, GOOGLE_ONCE_INTERNAL_DONE);
        if (old_control == GOOGLE_ONCE_INTERNAL_WAITER) {
            base::internal::SpinLockWake(control, true);
        }
    } // else *control is already GOOGLE_ONCE_INTERNAL_DONE
}
