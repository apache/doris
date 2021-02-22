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

#include "test_util/test_util.h"

#include <libgen.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <unistd.h>

#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace doris {

static const char* const kSlowTestsEnvVar = "DORIS_ALLOW_SLOW_TESTS";

bool AllowSlowTests() {
    return GetBooleanEnvironmentVariable(kSlowTestsEnvVar);
}

bool GetBooleanEnvironmentVariable(const char* env_var_name) {
    const char* const e = getenv(env_var_name);
    if ((e == nullptr) || (strlen(e) == 0) || (strcasecmp(e, "false") == 0) ||
        (strcasecmp(e, "0") == 0) || (strcasecmp(e, "no") == 0)) {
        return false;
    }
    if ((strcasecmp(e, "true") == 0) || (strcasecmp(e, "1") == 0) || (strcasecmp(e, "yes") == 0)) {
        return true;
    }
    LOG(FATAL) << Substitute("$0: invalid value for environment variable $0", e, env_var_name);
    return false; // unreachable
}

std::string GetCurrentRunningDir() {
    char exe[PATH_MAX];
    ssize_t r;

    if ((r = readlink("/proc/self/exe", exe, PATH_MAX)) < 0) {
        return std::string();
    }

    if (r == PATH_MAX) {
        r -= 1;
    }
    exe[r] = 0;
    char* dir = dirname(exe);
    return std::string(dir);
}

} // namespace doris
