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

#pragma once
#include <memory>

// All object should inherit from this class, like
// class A  {
// DISALLOW_EXPILICT_NEW(A);
// };
//
// Then the caller could not call new A() any more, has to call A::create_shared(...)
// or A::create_unique(...), then we could make sure all object in our project is shared
// pointer.
// But could call A a(...);

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
//
// Not use template like cowhelper to implements this feature because it has problem
// during inherits
// TODO try to allow make_unique
//

// On macOS, LLVM libc++ (both 18 and 20) has an internal compressed_pair
// static_cast issue when make_shared is used with types that inherit from
// enable_shared_from_this.  Using the two-allocation form std::shared_ptr(new T)
// avoids the libc++ bug while still correctly initialising the enable_shared_from_this
// weak pointer (the shared_ptr constructor detects and handles this automatically).
// create_shared is defined inside the class body (via macro expansion), so it can
// access the private operator new of each class.
#ifdef __APPLE__
#define _DORIS_CREATE_SHARED_IMPL(TypeName) \
    std::shared_ptr<TypeName>(new TypeName(std::forward<Args>(args)...))
#else
#define _DORIS_CREATE_SHARED_IMPL(TypeName) std::make_shared<TypeName>(std::forward<Args>(args)...)
#endif

#define ENABLE_FACTORY_CREATOR(TypeName)                                             \
private:                                                                             \
    void* operator new(std::size_t size) {                                           \
        return ::operator new(size);                                                 \
    }                                                                                \
    void* operator new[](std::size_t size) {                                         \
        return ::operator new[](size);                                               \
    }                                                                                \
                                                                                     \
public:                                                                              \
    void* operator new(std::size_t count, void* ptr) {                               \
        return ::operator new(count, ptr);                                           \
    }                                                                                \
    void operator delete(void* ptr) noexcept {                                       \
        ::operator delete(ptr);                                                      \
    }                                                                                \
    void operator delete[](void* ptr) noexcept {                                     \
        ::operator delete[](ptr);                                                    \
    }                                                                                \
    void operator delete(void* ptr, void* place) noexcept {                          \
        ::operator delete(ptr, place);                                               \
    }                                                                                \
    template <typename... Args>                                                      \
    static std::shared_ptr<TypeName> create_shared(Args&&... args) {                 \
        return _DORIS_CREATE_SHARED_IMPL(TypeName);                                  \
    }                                                                                \
    template <typename... Args>                                                      \
    static std::unique_ptr<TypeName> create_unique(Args&&... args) {                 \
        return std::unique_ptr<TypeName>(new TypeName(std::forward<Args>(args)...)); \
    }
