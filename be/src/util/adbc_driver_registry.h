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

#include <arrow-adbc/adbc.h>

#include <cstddef>
#include <map>
#include <mutex>
#include <string>

#include "common/status.h"

namespace doris {

/// Process-wide ADBC driver load cache.
///
/// Each resolved path is passed to AdbcLoadDriver (which dlopens it) at most once, and is **never
/// dlclosed**: drivers carry global state and background threads -- Go runtimes especially -- so
/// unloading one is a use-after-free hazard. Handles stay live until the process exits.
///
/// Failures are cached too, so a bad path does not retry the dlopen once per scan range.
///
/// **The registry itself is never destroyed either** -- see instance(). A function-local static
/// would be torn down during static destruction, which would both dangle every AdbcDriver* already
/// handed out and orphan the driver manager's own per-driver state (it is freed only by the
/// driver's release callback, which this registry deliberately never calls).
class AdbcDriverRegistry {
public:
    AdbcDriverRegistry(const AdbcDriverRegistry&) = delete;
    AdbcDriverRegistry& operator=(const AdbcDriverRegistry&) = delete;

    static AdbcDriverRegistry& instance();

    /// Loads the driver at `driver_path`, or returns the already-loaded one. An empty `entrypoint`
    /// lets the driver manager search for one based on the driver name. The returned pointer stays
    /// valid for the lifetime of the process.
    Status get_or_load(const std::string& driver_path, const std::string& entrypoint,
                       const AdbcDriver** out);

    /// Test only: how many paths have been attempted, successes and failures alike.
    size_t loaded_count() const;

private:
    AdbcDriverRegistry() = default;
    ~AdbcDriverRegistry() = default;

    struct Entry {
        AdbcDriver driver {};
        Status load_status;
        bool loaded = false;
    };

    mutable std::mutex _mutex;
    // Keyed by realpath(driver_path), falling back to the original string when it cannot be
    // resolved. std::map keeps the entries pointer-stable, which the returned AdbcDriver* needs.
    std::map<std::string, Entry> _drivers;
};

} // namespace doris
