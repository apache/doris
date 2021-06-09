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

#ifndef DORIS_BE_UTIL_DEFER_OP_H
#define DORIS_BE_UTIL_DEFER_OP_H

#include <functional>

namespace doris {

// This class is used to defer a function when this object is deconstruct
// A Better Defer operator #5576
// for C++17
// Defer defer {[]{ call something }};
//
// for C++11
// auto op = [] {};
// Defer<decltype<op>> (op);
template <class T>
class Defer {
public:
    Defer(T& closure) : _closure(closure) {}
    Defer(T&& closure) : _closure(std::move(closure)) {}
    ~Defer() { _closure(); }

private:
    T _closure;
};

} // namespace doris

#endif
