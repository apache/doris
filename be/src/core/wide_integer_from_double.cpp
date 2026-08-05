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

// On platforms whose long double lacks an 80-bit mantissa (LDBL_MANT_DIG != 64),
// wide_integer_impl.h emulates the from-double intermediate type with
// boost::multiprecision::cpp_bin_float_double_extended. Making that include part
// of every TU's closure costs ~4.3MB of preprocessed text; instead, ordinary TUs
// only see declarations of set_multiplier / wide_integer_from_builtin(double)
// and this TU compiles the bodies once. The macro below switches
// wide_integer_impl.h into "definitions inline here" mode.
#define DORIS_WIDE_INTEGER_FROM_DOUBLE_IMPL_TU

#include "core/wide_integer.h"

#if !(LDBL_MANT_DIG == 64)

// Every wide::integer specialization used by Doris (see core/extended_types.h)
// gets its from-double conversion emitted here. A new specialization that is
// constructed from double elsewhere will fail at link time — add it below.
template void wide::integer<128, signed>::_impl::wide_integer_from_builtin(
        wide::integer<128, signed>&, double) noexcept;
template void wide::integer<128, unsigned>::_impl::wide_integer_from_builtin(
        wide::integer<128, unsigned>&, double) noexcept;
template void wide::integer<256, signed>::_impl::wide_integer_from_builtin(
        wide::integer<256, signed>&, double) noexcept;
template void wide::integer<256, unsigned>::_impl::wide_integer_from_builtin(
        wide::integer<256, unsigned>&, double) noexcept;

#endif // !(LDBL_MANT_DIG == 64)
