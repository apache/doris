// Copyright 2008 Google Inc. All Rights Reserved.
//
// The first call to GoogleOnceInit() with a particular GoogleOnceType
// argument will run the specified function. Other calls with the same
// argument will not run the function, but will wait for the provided
// function to finish running (if it is still running). This provides
// a safe, simple, and fast mechanism for one-time initialization in a
// multi-threaded process.
//
// This module is a replacement for pthread_once().  It was added
// since some versions of pthread_once() call the supplied function
//
// Example usage:
//   static GoogleOnceType once = GOOGLE_ONCE_INIT;
//   static void Initializer() {
//     ... do initialization ...
//   }
//   ...
//   void SomeFunction() {
//     GoogleOnceInit(&once, &Initializer);
//     ...
//   }

#ifndef BASE_ONCE_H_
#define BASE_ONCE_H_

#include "gutil/atomicops.h"
#include "gutil/dynamic_annotations.h"
#include "gutil/integral_types.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/type_traits.h"

// The following enum values are not for use by clients
enum {
    GOOGLE_ONCE_INTERNAL_INIT = 0,
    GOOGLE_ONCE_INTERNAL_RUNNING = 0x65C2937B, // an improbable 32-bit value
    GOOGLE_ONCE_INTERNAL_WAITER = 0x05A308D2,  // a different improbable value
    GOOGLE_ONCE_INTERNAL_DONE = 0x3F2D8AB0,    // yet another improbable value
};

struct GoogleOnceType {
    Atomic32 state;
};

#define GOOGLE_ONCE_INIT \
    { GOOGLE_ONCE_INTERNAL_INIT }

// For internal use only.
extern void GoogleOnceInternalInit(Atomic32* state, void (*func)(), void (*func_with_arg)(void*),
                                   void* arg);

inline void GoogleOnceInit(GoogleOnceType* state, void (*func)()) {
    Atomic32 s = Acquire_Load(&state->state);
    if (PREDICT_FALSE(s != GOOGLE_ONCE_INTERNAL_DONE)) {
        GoogleOnceInternalInit(&state->state, func, 0, 0);
    }
    ANNOTATE_HAPPENS_AFTER(&state->state);
}

// A version of GoogleOnceInit where the function argument takes a pointer
// of arbitrary type.
template <typename T>
inline void GoogleOnceInitArg(GoogleOnceType* state, void (*func_with_arg)(T*), T* arg) {
    Atomic32 s = Acquire_Load(&state->state);
    if (PREDICT_FALSE(s != GOOGLE_ONCE_INTERNAL_DONE)) {
        // Deal with const T as well as non-const T.
        typedef typename base::remove_const<T>::type mutable_T;
        GoogleOnceInternalInit(&state->state, 0, reinterpret_cast<void (*)(void*)>(func_with_arg),
                               const_cast<mutable_T*>(arg));
    }
    ANNOTATE_HAPPENS_AFTER(&state->state);
}

// GoogleOnceDynamic is like GoogleOnceType, but is dynamically
// initialized instead of statically initialized.  This should be used only
// when the variable is not of static storage class.
// It might be used to delay expensive initialization of part of a
// dynamically-allocated data structure until it is known to be needed.  For
// example:
//   class MyType {
//     GoogleOnceDynamic once_;
//     ComplexStuff* complex_stuff_;
//     static void InitComplexStuff(MyType* me) {
//       me->complex_stuff_ = ...;
//     }
//    public:
//     ComplexStuff* complex_stuff() {
//       this->once_.Init(&InitComplexStuff, this);
//       return this->complex_stuff_;
//     }
//   }
class GoogleOnceDynamic {
public:
    GoogleOnceDynamic() : state_(GOOGLE_ONCE_INTERNAL_INIT) {}

    // If this->Init() has not been called before by any thread,
    // execute (*func_with_arg)(arg) then return.
    // Otherwise, wait until that prior invocation has finished
    // executing its function, then return.
    template <typename T>
    void Init(void (*func_with_arg)(T*), T* arg) {
        Atomic32 s = Acquire_Load(&this->state_);
        if (PREDICT_FALSE(s != GOOGLE_ONCE_INTERNAL_DONE)) {
            // Deal with const T as well as non-const T.
            typedef typename base::remove_const<T>::type mutable_T;
            GoogleOnceInternalInit(&this->state_, 0,
                                   reinterpret_cast<void (*)(void*)>(func_with_arg),
                                   const_cast<mutable_T*>(arg));
        }
        ANNOTATE_HAPPENS_AFTER(&this->state_);
    }

private:
    Atomic32 state_;
    DISALLOW_COPY_AND_ASSIGN(GoogleOnceDynamic);
};

#endif // BASE_ONCE_H_
