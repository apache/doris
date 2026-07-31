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

#include "util/adbc_driver_registry.h"

#include <arrow-adbc/adbc_driver_manager.h>
#include <glog/logging.h>
#include <stdlib.h>

#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common/check.h"

namespace doris {

namespace {

std::string resolve_path(const std::string& driver_path) {
    // A driver that is not on disk yet still needs a stable cache key, so keep the original string.
    std::unique_ptr<char, decltype(&free)> resolved(realpath(driver_path.c_str(), nullptr), &free);
    if (resolved == nullptr) {
        return driver_path;
    }
    return {resolved.get()};
}

// Drivers own the strings they put in AdbcError, so every populated error must be released.
std::string take_error_message(AdbcError* error) {
    DORIS_CHECK(error != nullptr);
    std::string message = error->message != nullptr ? error->message : "";
    if (error->release != nullptr) {
        error->release(error);
    }
    return message;
}

} // namespace

AdbcDriverRegistry& AdbcDriverRegistry::instance() {
    // Allocated and never freed, on purpose. A plain function-local static is destroyed during
    // static destruction, and this registry must outlive that: it hands out AdbcDriver pointers
    // documented to stay valid for the life of the process, and it holds each driver's
    // manager-side state, which only the driver's release callback frees -- a call this registry
    // never makes, because dlclosing a driver that owns background threads is a use-after-free.
    // Destroying the map would therefore drop the last reference to memory that stays live anyway,
    // which is also exactly what LeakSanitizer reports at exit (its check runs after every static
    // destructor).
    static auto* registry = new AdbcDriverRegistry();
    return *registry;
}

Status AdbcDriverRegistry::get_or_load(const std::string& driver_path,
                                       const std::string& entrypoint, const AdbcDriver** out) {
    DORIS_CHECK(out != nullptr);
    if (driver_path.empty()) {
        return Status::InvalidArgument("ADBC: driver path is empty");
    }

    const std::string key = resolve_path(driver_path);

    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _drivers.find(key);
    if (it != _drivers.end()) {
        if (!it->second.loaded) {
            return it->second.load_status;
        }
        *out = &it->second.driver;
        return Status::OK();
    }

    Entry entry;
    AdbcError error = ADBC_ERROR_INIT;
    const AdbcStatusCode code =
            AdbcLoadDriver(key.c_str(), entrypoint.empty() ? nullptr : entrypoint.c_str(),
                           ADBC_VERSION_1_1_0, &entry.driver, &error);
    if (code != ADBC_STATUS_OK) {
        const std::string message = take_error_message(&error);
        // The path is what the user controls, so it has to be in the message for them to fix it.
        // AdbcStatusCode is a uint8_t, so spell out the name as well as the number.
        entry.load_status = Status::InternalError(
                "ADBC: failed to load driver '{}' ({}, code {}): {}", driver_path,
                AdbcStatusCodeMessage(code), static_cast<int>(code),
                message.empty() ? "no error message from the driver manager" : message);
        LOG(WARNING) << entry.load_status;
        return _drivers.emplace(key, std::move(entry)).first->second.load_status;
    }
    // A successful load can still leave a warning behind, and it is the driver's memory to free.
    take_error_message(&error);
    entry.loaded = true;

    *out = &_drivers.emplace(key, std::move(entry)).first->second.driver;
    return Status::OK();
}

size_t AdbcDriverRegistry::loaded_count() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _drivers.size();
}

} // namespace doris
