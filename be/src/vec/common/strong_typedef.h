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

#include <boost/operators.hpp>
#include <type_traits>

/** https://svn.boost.org/trac/boost/ticket/5182
  */

template <class T, class Tag>
struct StrongTypedef : boost::totally_ordered1<StrongTypedef<T, Tag>,
                                               boost::totally_ordered2<StrongTypedef<T, Tag>, T>> {
private:
    using Self = StrongTypedef;
    T t;

public:
    template <class Enable = typename std::is_copy_constructible<T>::type>
    explicit StrongTypedef(const T& t_) : t(t_) {}
    template <class Enable = typename std::is_move_constructible<T>::type>
    explicit StrongTypedef(T&& t_) : t(std::move(t_)) {}

    template <class Enable = typename std::is_default_constructible<T>::type>
    StrongTypedef() : t() {}

    StrongTypedef(const Self&) = default;
    StrongTypedef(Self&&) = default;

    Self& operator=(const Self&) = default;
    Self& operator=(Self&&) = default;

    template <class Enable = typename std::is_copy_assignable<T>::type>
    Self& operator=(const T& rhs) {
        t = rhs;
        return *this;
    }

    template <class Enable = typename std::is_move_assignable<T>::type>
    Self& operator=(T&& rhs) {
        t = std::move(rhs);
        return *this;
    }

    operator const T&() const { return t; }
    operator T&() { return t; }

    bool operator==(const Self& rhs) const { return t == rhs.t; }
    bool operator<(const Self& rhs) const { return t < rhs.t; }

    T& to_under_type() { return t; }
    const T& to_under_type() const { return t; }
};

namespace std {
template <class T, class Tag>
struct hash<StrongTypedef<T, Tag>> {
    size_t operator()(const StrongTypedef<T, Tag>& x) const {
        return std::hash<T>()(x.to_under_type());
    }
};
} // namespace std

#define STRONG_TYPEDEF(T, D) \
    struct D##Tag {};        \
    using D = StrongTypedef<T, D##Tag>;
