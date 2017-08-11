// Copyright 2003 Google Inc.
//
// The Singleton<Type> class manages a single instance of Type which will be
// created on first use and (usually) never destroyed.
//
//   MyClass* ptr = Singleton<MyClass>::get()
//   ptr->DoSomething();
//
// Singleton<> has no non-static members and is never actually instantiated.
//
// WARNING: Read go/singletons before using.
//
// This class is thread safe; the constructor will be run at most once, and
// no user will gain access to the object until the constructor is completed.
// The underlying Type must of course be thread-safe if you want to use it
// concurrently.
//
// If you want to ensure that your class can only exist as a singleton, make
// its constructors private, and make Singleton<> a friend:
//
//   class MySingletonOnlyClass {
//    public:
//     void DoSomething() { ... }
//    private:
//     DISALLOW_COPY_AND_ASSIGN(MySingletonOnlyClass);
//     MySingletonOnlyClass() { ... }
//     friend class Singleton<MySingletonOnlyClass>;
//   }
//
// If your singleton requires complex initialization, or does not have a
// suitable default constructor, you can provide a specialization of
// Singleton<Type>::CreateInstance() to perform the appropriate setup, e.g.:
//
//   template <>
//   Type* Singleton<VirtualType>::CreateInstance() { return new ConcreteImpl; }
//
// If you want to initialize something eagerly at startup, rather than lazily
// upon use, consider using REGISTER_MODULE_INITIALIZER (in base/googleinit.h).
//
// This class also allows users to pick a particular instance as the
// singleton with InjectInstance(). This enables unittesting and
// dependency injection. It must only be used at program startup.
//
// Caveats:
// (a) The instance is normally never destroyed.  Destroying a Singleton is
//     complex and error-prone; C++ books go on about this at great length,
//     and I have seen no perfect general solution to the problem.
//     We *do* offer UnsafeReset() which is not thread-safe at all.
//
// (b) Your class must have a default (no-argument) constructor, or you must
//     provide a specialization for Singleton<Type>::CreateInstance().
//
// (c) Your class's constructor must never throw an exception.
//
// Singleton::get() is very fast - about 1ns on a 2.4GHz Core 2.

#ifndef UTIL_GTL_SINGLETON_H__
#define UTIL_GTL_SINGLETON_H__

#include <stddef.h>

#include <common/logging.h>

#include "gutil/logging-inl.h"
#include "gutil/once.h"

namespace util {
namespace gtl {
template <typename SingletonType> class ScopedSingletonOverride;
template <typename SingletonType> class ScopedSingletonOverrideNoDelete;
}  // namespace gtl
}  // namespace util

template <typename Type>
class Singleton {
 public:
  // Return a pointer to the one true instance of the class.
  static Type* get() {
    GoogleOnceInit(&once_, &Singleton<Type>::Init);
    return instance_;
  }

  // WARNING!!!  This function is not thread-safe and may leak memory.
  static void UnsafeReset() {
    delete instance_;
    instance_ = NULL;
    once_.state = GOOGLE_ONCE_INTERNAL_INIT;  // This is the bad part!
  }

  // This function is used to replace the instance used by
  // Singleton<Type>::get(). It can be used for breaking dependencies.  For
  // unittesting, you probably want to use ScopedSingletonOverride instead.
  //
  // This function must be called before Singleton<Type>::get() is
  // called and before any threads are created. If these assumptions
  // are violated, anything could happen, but we try to crash in debug
  // mode and do nothing in production.
  static void InjectInstance(Type* instance) {
    injected_instance_ = instance;
    GoogleOnceInit(&once_, &Singleton<Type>::Inject);
    injected_instance_ = NULL;  // Helps detect leaks in the unittest.
    if (instance_ != instance) {
      LOG(DFATAL) << "(jyasskin) InjectInstance() must be called at most once"
                  << " at the start of the program, before the Singleton has"
                  << " been accessed and before any threads have been created."
                  << " Ignoring the call in production.";
      delete instance;
    }
  }

 private:
  friend class util::gtl::ScopedSingletonOverride<Type>;
  friend class util::gtl::ScopedSingletonOverrideNoDelete<Type>;

  // Create the instance.
  static void Init() {
    instance_ = CreateInstance();
  }

  // Create and return the instance. You can use Singleton for objects which
  // require more complex setup by defining a specialization for your type.
  static Type* CreateInstance() {
    // use ::new to work around a gcc bug when operator new is overloaded
    return ::new Type;
  }

  // Inject the instance.
  static void Inject() {
    instance_ = injected_instance_;
  }

  // Used by ScopedSingletonOverride.  Definitely not threadsafe.  No one
  // should be calling this other than ScopedSingletonOverride (which has
  // friend access to do this and makes sure it calls get() first).
  static void OverrideSingleton(Type* override_instance) {
    instance_ = override_instance;
  }

  static GoogleOnceType once_;
  static Type* instance_;
  static Type* injected_instance_;
};

template <typename Type>
GoogleOnceType Singleton<Type>::once_ = GOOGLE_ONCE_INIT;

template <typename Type>
Type* Singleton<Type>::instance_ = NULL;

template <typename Type>
Type* Singleton<Type>::injected_instance_ = NULL;

#endif  // UTIL_GTL_SINGLETON_H__
