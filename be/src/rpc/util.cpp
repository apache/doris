// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "util.h"

#include <cstdarg>
#include <cstdio>
#include <algorithm>

namespace palo {

std::string format(const char *fmt, ...) {
    char buf[1024];       // should be enough for most cases
    int n = 0;
    int size = sizeof(buf);
    char *p = buf;
    va_list ap;
    do {
        va_start(ap, fmt);
        n = vsnprintf(p, size, fmt, ap);
        va_end(ap);
        if (n > -1 && n < size) {
            break;    // worked!
        }
        if (n > -1) {        // glibc 2.1+/iso c99
            size = n + 1;     //   exactly what's needed
        } else {                // glibc 2.0
            size *= 2;        //   double the size and try again
        }
        p = (char *)(p == buf ? malloc(size) : realloc(p, size));
        if (!p) {
            throw std::bad_alloc();
        }
    } while (true);
    if (buf == p) {
        return std::string(p, n);
    }
    std::string ret(p, n);
    free(p);
    return ret;
}

char const *const digits = "0123456789";

std::string format_number(int64_t n, int sep) {
    char buf[30];
    char *p = buf;
    char *p0 = buf;
    int ndigits = 0;
    uint64_t num; // for edge cases when -n is still negative when n < 0
    if (n < 0) {
        *p++ = '-';
        p0 = p;
        num = -n;
    } else {
        num = n;
    }
    if (num == 0) {
        *p++ = '0';
    } else {
        for (; num != 0; num /= 10) {
            *p++ = digits[num % 10];
            ++ndigits;
            if (num >= 10 && ndigits % 3 == 0) {
                *p++ = sep;
            }
        }
    }
    int len = ndigits + (ndigits - 1) / 3;
    std::reverse(p0, p0 + len);
    return std::string(buf, len + p0 - buf);
}

std::string
format_bytes(size_t n, const void *buf, size_t len, const char *trailer) {
    if (buf) {
        if (len <= n) {
            return std::string((char *)buf, len);
        }
        std::string out((char *)buf, n);
        out += trailer;
        return out;
    }
    return "<null>";
}

const char NumericFormatterDigits::DIGITS[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

} //namespace palo
