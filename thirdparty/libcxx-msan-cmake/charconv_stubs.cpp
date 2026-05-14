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

// Stub implementations for std::to_chars(float/double/long double) which are
// missing from our MSAN libc++ build because charconv.cpp requires LLVM libc
// internal headers (shared/fp_bits.h) that are not available in our tree.
// These stubs use snprintf as a fallback.

#include <charconv>
#include <cstdio>
#include <cstring>
#include <cstdlib>

_LIBCPP_BEGIN_NAMESPACE_STD

// Helper: format a floating-point value using snprintf and write to [first, last).
template <typename _Fp>
static to_chars_result __to_chars_snprintf(char* __first, char* __last, _Fp __value, const char* __fmt) {
    // Use a local buffer to avoid overflow
    char __buf[128];
    int __n = std::snprintf(__buf, sizeof(__buf), __fmt, static_cast<double>(__value));
    if (__n < 0 || static_cast<size_t>(__n) >= sizeof(__buf))
        return {__last, std::errc::value_too_large};
    if (__first + __n > __last)
        return {__last, std::errc::value_too_large};
    std::memcpy(__first, __buf, __n);
    return {__first + __n, std::errc{}};
}

static const char* __fmt_for_chars_format(chars_format __fmt) {
    switch (__fmt) {
        case chars_format::scientific: return "%e";
        case chars_format::fixed: return "%f";
        case chars_format::hex: return "%a";
        default: return "%g";
    }
}

to_chars_result to_chars(char* __first, char* __last, float __value) {
    return __to_chars_snprintf(__first, __last, __value, "%g");
}

to_chars_result to_chars(char* __first, char* __last, double __value) {
    return __to_chars_snprintf(__first, __last, __value, "%g");
}

to_chars_result to_chars(char* __first, char* __last, long double __value) {
    return __to_chars_snprintf(__first, __last, __value, "%Lg");
}

to_chars_result to_chars(char* __first, char* __last, float __value, chars_format __fmt) {
    return __to_chars_snprintf(__first, __last, __value, __fmt_for_chars_format(__fmt));
}

to_chars_result to_chars(char* __first, char* __last, double __value, chars_format __fmt) {
    return __to_chars_snprintf(__first, __last, __value, __fmt_for_chars_format(__fmt));
}

to_chars_result to_chars(char* __first, char* __last, long double __value, chars_format __fmt) {
    const char* base = __fmt_for_chars_format(__fmt);
    // For long double, use 'L' modifier
    char lfmt[8];
    if (base[1] == 'g' || base[1] == 'e' || base[1] == 'f' || base[1] == 'a') {
        lfmt[0] = '%'; lfmt[1] = 'L'; lfmt[2] = base[1]; lfmt[3] = '\0';
    } else {
        lfmt[0] = '%'; lfmt[1] = 'L'; lfmt[2] = 'g'; lfmt[3] = '\0';
    }
    char __buf[128];
    int __n = std::snprintf(__buf, sizeof(__buf), lfmt, __value);
    if (__n < 0 || static_cast<size_t>(__n) >= sizeof(__buf))
        return {__last, std::errc::value_too_large};
    if (__first + __n > __last)
        return {__last, std::errc::value_too_large};
    std::memcpy(__first, __buf, __n);
    return {__first + __n, std::errc{}};
}

static const char* __fmt_for_precision(chars_format __fmt) {
    switch (__fmt) {
        case chars_format::scientific: return "%.*e";
        case chars_format::fixed: return "%.*f";
        case chars_format::hex: return "%.*a";
        default: return "%.*g";
    }
}

to_chars_result to_chars(char* __first, char* __last, float __value, chars_format __fmt, int __precision) {
    char __buf[128];
    int __n = std::snprintf(__buf, sizeof(__buf), __fmt_for_precision(__fmt), __precision, static_cast<double>(__value));
    if (__n < 0 || static_cast<size_t>(__n) >= sizeof(__buf))
        return {__last, std::errc::value_too_large};
    if (__first + __n > __last)
        return {__last, std::errc::value_too_large};
    std::memcpy(__first, __buf, __n);
    return {__first + __n, std::errc{}};
}

to_chars_result to_chars(char* __first, char* __last, double __value, chars_format __fmt, int __precision) {
    char __buf[128];
    int __n = std::snprintf(__buf, sizeof(__buf), __fmt_for_precision(__fmt), __precision, __value);
    if (__n < 0 || static_cast<size_t>(__n) >= sizeof(__buf))
        return {__last, std::errc::value_too_large};
    if (__first + __n > __last)
        return {__last, std::errc::value_too_large};
    std::memcpy(__first, __buf, __n);
    return {__first + __n, std::errc{}};
}

to_chars_result to_chars(char* __first, char* __last, long double __value, chars_format __fmt, int __precision) {
    const char* base = __fmt_for_precision(__fmt);
    char lfmt[16];
    // Insert 'L' before the format char
    if (base[2] == 'g' || base[2] == 'e' || base[2] == 'f' || base[2] == 'a') {
        std::snprintf(lfmt, sizeof(lfmt), "%%.*L%c", base[2]);
    } else {
        std::strcpy(lfmt, "%.*Lg");
    }
    char __buf[128];
    int __n = std::snprintf(__buf, sizeof(__buf), lfmt, __precision, __value);
    if (__n < 0 || static_cast<size_t>(__n) >= sizeof(__buf))
        return {__last, std::errc::value_too_large};
    if (__first + __n > __last)
        return {__last, std::errc::value_too_large};
    std::memcpy(__first, __buf, __n);
    return {__first + __n, std::errc{}};
}

_LIBCPP_END_NAMESPACE_STD
