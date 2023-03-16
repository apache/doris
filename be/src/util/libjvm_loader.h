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

#pragma once

#include <dlfcn.h>
#include <jni.h>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

namespace doris {

class Status;

class LibJVMLoader {
public:
    LibJVMLoader(const LibJVMLoader&) = delete;
    LibJVMLoader& operator=(const LibJVMLoader&) = delete;

    static LibJVMLoader& instance();
    Status load();

    using JNI_GetCreatedJavaVMsPointer = std::add_pointer_t<decltype(::JNI_GetCreatedJavaVMs)>;
    static JNI_GetCreatedJavaVMsPointer JNI_GetCreatedJavaVMs;

    using JNI_CreateJavaVMPointer = std::add_pointer_t<decltype(::JNI_CreateJavaVM)>;
    static JNI_CreateJavaVMPointer JNI_CreateJavaVM;

private:
    explicit LibJVMLoader(std::string_view library)
            : _library(library), _handle(nullptr, nullptr) {}

    const std::string _library;
    std::unique_ptr<void, void (*)(void*)> _handle;
};

} // namespace doris
