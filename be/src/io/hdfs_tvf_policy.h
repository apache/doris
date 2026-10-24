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

#include <functional>
#include <memory>
#include <string>

#include "common/status.h"

namespace doris {
class THdfsParams;
namespace io {
class HdfsHandler;

// Owns the private Java configuration used to initialize one native Hadoop connection.
class HdfsTvfPolicy {
public:
    static constexpr const char* POLICY_KEY = "fs.doris.tvf.remote.paths";
    static constexpr const char* ROOT_KEY = "fs.doris.tvf.root";
    static constexpr const char* PATH_KEY = "fs.doris.tvf.path";
    static Status on_pthread(const std::function<Status()>& operation);
    static bool requested(const THdfsParams& params);
    static Status prepare_runtime();
    static Status clear_runtime();
    static Status capture(const std::string& fs_name, std::shared_ptr<HdfsTvfPolicy>* result);
    static std::string storage_cache_identity(const HdfsHandler& handler,
                                              const std::string& fs_name);

    ~HdfsTvfPolicy();
    bool requires_jni() const;
    Status check(const std::string& fs_name, const std::string& native_path);

private:
    struct Impl;
    explicit HdfsTvfPolicy(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};
} // namespace io
} // namespace doris
