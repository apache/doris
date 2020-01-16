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

#ifndef IMPALA_UTIL_TEMPLATE_UTIL_H
#define IMPALA_UTIL_TEMPLATE_UTIL_H

#include <boost/type_traits/is_arithmetic.hpp>
#include <boost/type_traits/is_float.hpp>
#include <boost/type_traits/is_integral.hpp>
#include <boost/utility/enable_if.hpp>

/// The ENABLE_IF_* macros are used to 'enable' - i.e. to make available to the compiler -
/// a method only if a type parameter belongs to the set described by each macro. Each
/// macro takes the return type of the method as an argument (a requirement of the
/// underlying type trait template that implements this logic).
/// Usage:
/// template <typename T> ENABLE_IF_ARITHMETIC(T, T) foo(T arg) { return arg + 2; }

/// Enables a method only if 'type_param' is arithmetic, that is an integral type or a
/// floating-point type
#define ENABLE_IF_ARITHMETIC(type_param, return_type) \
    typename boost::enable_if_c<boost::is_arithmetic<type_param>::value, return_type>::type

/// Enables a method only if 'type_param' is not arithmetic, that is neither an integral
/// type or a floating-point type
#define ENABLE_IF_NOT_ARITHMETIC(type_param, return_type) \
    typename boost::enable_if_c<!boost::is_arithmetic<type_param>::value, return_type>::type

/// Enables a method only if 'type_param' is integral, i.e. some variant of int or long
#define ENABLE_IF_INTEGRAL(type_param, return_type) \
    typename boost::enable_if_c<boost::is_integral<type_param>::value, return_type>::type

/// Enables a method only if 'type_param' is a floating point type, i.e. some variant of
/// float or double.
#define ENABLE_IF_FLOAT(type_param, return_type) \
    typename boost::enable_if_c<!boost::is_integral<type_param>::value, return_type>::type

#endif
