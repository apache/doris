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

#include "vec/common/demangle.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define MEMORY_SANITIZER 1
#endif
#elif defined(__MEMORY_SANITIZER__)
#define MEMORY_SANITIZER 1
#endif

#if _MSC_VER || MEMORY_SANITIZER

std::string demangle(const char* name, int& status) {
    status = 0;
    return name;
}

#else

#include <cxxabi.h>
#include <stdlib.h>

std::string demangle(const char* name, int& status) {
    std::string res;

    char* demangled_str = abi::__cxa_demangle(name, 0, 0, &status);
    if (demangled_str) {
        try {
            res = demangled_str;
        } catch (...) {
            free(demangled_str);
            throw;
        }
        free(demangled_str);
    } else
        res = name;

    return res;
}

#endif
