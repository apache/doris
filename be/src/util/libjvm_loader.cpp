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

#include "util/libjvm_loader.h"

#include <dlfcn.h>

#include <cstdlib>
#include <filesystem>
#include <mutex>

#include "common/status.h"

namespace {

#ifndef __APPLE__
#define LIBJVM_SO "libjvm.so"
#else
#define LIBJVM_SO "libjvm.dylib"
#endif

template <typename T>
doris::Status resolve_symbol(T& pointer, void* handle, const char* symbol) {
    pointer = reinterpret_cast<T>(dlsym(handle, symbol));
    return (pointer != nullptr)
                   ? doris::Status::OK()
                   : doris::Status::RuntimeError("Failed to resolve the symbol {}", symbol);
}

} // namespace

namespace doris {

LibJVMLoader::JNI_GetCreatedJavaVMsPointer LibJVMLoader::JNI_GetCreatedJavaVMs = nullptr;
LibJVMLoader::JNI_CreateJavaVMPointer LibJVMLoader::JNI_CreateJavaVM = nullptr;

LibJVMLoader& LibJVMLoader::instance() {
    static std::once_flag find_library;
    static std::string library;
    std::call_once(find_library, []() {
        const auto* java_home = getenv("JAVA_HOME");
        if (!java_home) {
            return;
        }
        std::string path(java_home);
        for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
            if (entry.path().filename() == LIBJVM_SO) {
                library = entry.path().string();
                break;
            }
        }
    });

    static LibJVMLoader loader(library);
    return loader;
}

Status LibJVMLoader::load() {
    if (_library.empty()) {
        return Status::RuntimeError("Failed to find the library {}.", LIBJVM_SO);
    }

    static std::once_flag resolve_symbols;
    static Status status;
    std::call_once(resolve_symbols, [this]() {
        _handle = std::unique_ptr<void, void (*)(void*)>(dlopen(_library.c_str(), RTLD_LAZY),
                                                         [](void* handle) { dlclose(handle); });
        if (!_handle) {
            status = Status::RuntimeError(dlerror());
            return;
        }

        if (status = resolve_symbol(JNI_GetCreatedJavaVMs, _handle.get(), "JNI_GetCreatedJavaVMs");
            !status.ok()) {
            return;
        }
        if (status = resolve_symbol(JNI_CreateJavaVM, _handle.get(), "JNI_CreateJavaVM");
            !status.ok()) {
            return;
        }
    });
    return status;
}

} // namespace doris
