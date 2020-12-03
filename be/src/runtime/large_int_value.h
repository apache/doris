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

#ifndef DORIS_BE_RUNTIME_LARGE_INT_VALUE_H
#define DORIS_BE_RUNTIME_LARGE_INT_VALUE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <sstream>
#include <string>

#include "runtime/decimal_value.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"

namespace doris {

const __int128 MAX_INT128 = ~((__int128)0x01 << 127);
const __int128 MIN_INT128 = ((__int128)0x01 << 127);

class LargeIntValue {
public:
    static char* to_string(__int128 value, char* buffer, int* len) {
        DCHECK(*len >= 40);
        unsigned __int128 tmp = value < 0 ? -value : value;
        char* d = buffer + *len;
        do {
            --d;
            *d = "0123456789"[tmp % 10];
            tmp /= 10;
        } while (tmp != 0);
        if (value < 0) {
            --d;
            *d = '-';
        }
        *len = (buffer + *len) - d;
        return d;
    }

    static std::string to_string(__int128 value) {
        char buf[64] = {0};
        int len = 64;
        char* str = to_string(value, buf, &len);
        return std::string(str, len);
    }
};

std::ostream& operator<<(std::ostream& os, __int128 const& value);

std::istream& operator>>(std::istream& is, __int128& value);

std::size_t hash_value(LargeIntValue const& value);

} // namespace doris

#endif
