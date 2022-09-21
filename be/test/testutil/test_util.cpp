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

#include "testutil/test_util.h"

#ifndef __APPLE__
#include <linux/limits.h>
#endif

#include <common/configbase.h>
#include <libgen.h>
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

void InitConfig() {
    std::string conf_file = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!config::init(conf_file.c_str(), false)) {
        fprintf(stderr, "Init config file failed, path %s\n", conf_file.c_str());
        exit(-1);
    }
}

bool equal_ignore_case(std::string lhs, std::string rhs) {
    std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
    std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
    return lhs == rhs;
}

std::mt19937_64 rng_64(std::chrono::steady_clock::now().time_since_epoch().count());

int rand_rng_int(int l, int r) {
    std::uniform_int_distribution<int> u(l, r);
    return u(rng_64);
}
char rand_rng_char() {
    return (rand_rng_int(0, 1) ? 'a' : 'A') + rand_rng_int(0, 25);
}
std::string rand_rng_string(size_t length) {
    string s;
    while (length--) {
        s += rand_rng_char();
    }
    return s;
}
std::string rand_rng_by_type(FieldType fieldType) {
    if (fieldType == OLAP_FIELD_TYPE_CHAR) {
        return rand_rng_string(rand_rng_int(1, 8));
    } else if (fieldType == OLAP_FIELD_TYPE_VARCHAR) {
        return rand_rng_string(rand_rng_int(1, 128));
    } else if (fieldType == OLAP_FIELD_TYPE_STRING) {
        return rand_rng_string(rand_rng_int(1, 100000));
    } else {
        return std::to_string(rand_rng_int(1, 1000000));
    }
}

} // namespace doris
