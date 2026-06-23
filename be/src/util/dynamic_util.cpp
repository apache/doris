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

#include "common/phdr_cache.h"

namespace doris {

Status dynamic_lookup(void* handle, const char* symbol, void** fn_ptr) {
    *(void**)(fn_ptr) = dlsym(handle, symbol);
    char* error = dlerror();

    if (error != nullptr) {
        return Status::InternalError("Unable to find {}\ndlerror: {}", symbol, error);
    }

    return Status::OK();
}

Status dynamic_open(const char* library, void** handle) {
    int flags = RTLD_NOW;

    *handle = dlopen(library, flags);

    if (*handle == nullptr) {
        return Status::InternalError("Unable to load {}\ndlerror: {}", library, dlerror());
    }

    // The process-wide dl_iterate_phdr override serves unwinders from a PHDR snapshot. Refresh it
    // after Doris-controlled dlopen so stack traces and exceptions can see newly loaded UDF libs.
    updatePHDRCache();
    return Status::OK();
}

void dynamic_close(void* handle) {
// There is an issue of LSAN can't deal well with dlclose(), so we disable LSAN here, more details:
// https://github.com/google/sanitizers/issues/89
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER)
    dlclose(handle);
    // dlclose changes the loader object list. Refresh after the close so later unwinding does not
    // consult stale PHDR entries for Doris-controlled dynamic libraries.
    updatePHDRCache();
#endif
}

} // namespace doris
