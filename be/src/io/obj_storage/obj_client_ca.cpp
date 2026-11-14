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

#include "io/obj_storage/obj_client_ca.h"

#include <mutex>
#include <utility>

#include "common/config.h"
#include "cpp/aws_common.h"
#include "util/string_util.h"

namespace doris::io {
namespace {

struct CaCertCache {
    std::mutex lock;
    std::string path;
};

// A function-local static, so that callers during static initialization find it constructed.
CaCertCache& ca_cert_cache() {
    static CaCertCache cache;
    return cache;
}

} // namespace

std::string obj_client_ca_cert_file_path() {
    auto& cache = ca_cert_cache();
    std::lock_guard lock(cache.lock);
    if (cache.path.empty()) {
        cache.path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    }
    return cache.path;
}

#ifdef BE_TEST
std::string exchange_obj_client_ca_cert_file_path_for_test(std::string path) {
    auto& cache = ca_cert_cache();
    std::lock_guard lock(cache.lock);
    return std::exchange(cache.path, std::move(path));
}
#endif

} // namespace doris::io
