// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_RPC_COMPAT_H
#define BDG_PALO_BE_SRC_RPC_COMPAT_H

// The same stuff for C code
#include "compat_c.h"
#include <cstddef> // for std::size_t and std::ptrdiff_t
#include <memory>

// C++ specific stuff
#ifndef BOOST_SPIRIT_THREADSAFE
#  define BOOST_SPIRIT_THREADSAFE
#endif
#define BOOST_IOSTREAMS_USE_DEPRECATED
#define BOOST_FILESYSTEM_VERSION 3
#define BOOST_FILESYSTEM_DEPRECATED     1
#define HT_UNUSED(x) static_cast<void>(x)

template<typename T, typename... Ts>
std::unique_ptr<T> make_unique(Ts&&... params) {
    return std::unique_ptr<T>(new T(std::forward<Ts>(params)...));
}

#if defined(__APPLE__) || !defined(_GLIBCXX_HAVE_QUICK_EXIT)
namespace std {
    inline void quick_exit(int status) { _exit(status); }
}
#endif

#endif //BDG_PALO_BE_SRC_RPC_COMPAT_H
