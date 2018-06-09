// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

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
