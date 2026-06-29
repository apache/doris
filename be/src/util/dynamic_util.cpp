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
#if defined(__ELF__) && !defined(__FreeBSD__)
#include "common/symbol_index.h"
#endif

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

    // Doris-controlled dynamic loads should be visible to diagnostic stack snapshots without
    // forcing the next /api/stack_trace request to rebuild loader-derived state on demand.
    updatePHDRCache();
#if defined(__ELF__) && !defined(__FreeBSD__)
    SymbolIndex::reload();
#endif
    return Status::OK();
}

void dynamic_close(void* handle) {
// There is an issue of LSAN can't deal well with dlclose(), so we disable LSAN here, more details:
// https://github.com/google/sanitizers/issues/89
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER)
    dlclose(handle);
    // Refresh after dlclose so later symbolization does not keep stale Doris-controlled library
    // entries. SymbolIndex::reload() serializes concurrent rebuilds internally.
    updatePHDRCache();
#if defined(__ELF__) && !defined(__FreeBSD__)
    SymbolIndex::reload();
#endif
#endif
}

} // namespace doris
