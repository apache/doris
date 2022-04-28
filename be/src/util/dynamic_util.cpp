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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/dynamic-util.cc
// and modified by Doris

#include "util/dynamic_util.h"

#include <dlfcn.h>

#include <sstream>

namespace doris {

Status dynamic_lookup(void* handle, const char* symbol, void** fn_ptr) {
    *(void**)(fn_ptr) = dlsym(handle, symbol);
    char* error = dlerror();

    if (error != nullptr) {
        std::stringstream ss;
        ss << "Unable to find " << symbol << "\ndlerror: " << error;
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status dynamic_open(const char* library, void** handle) {
    int flags = RTLD_NOW;

    *handle = dlopen(library, flags);

    if (*handle == nullptr) {
        std::stringstream ss;
        ss << "Unable to load " << library << "\ndlerror: " << dlerror();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

void dynamic_close(void* handle) {
// There is an issue of LSAN can't deal well with dlclose(), so we disable LSAN here, more details:
// https://github.com/google/sanitizers/issues/89
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER)
    dlclose(handle);
#endif
}

} // namespace doris
