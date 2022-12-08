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

#include "util/errno.h"

#ifndef __APPLE__
#include <features.h>
#endif

#include <cstring>

#include "gutil/dynamic_annotations.h"

namespace doris {

void errno_to_cstring(int err, char* buf, size_t buf_len) {
#if !defined(__GLIBC__) || \
        ((_POSIX_C_SOURCE >= 200112 || _XOPEN_SOURCE >= 600) && !defined(_GNU_SOURCE))
    // Using POSIX version 'int strerror_r(...)'.
    int ret = strerror_r(err, buf, buf_len);
    if (ret && ret != ERANGE && ret != EINVAL) {
        strncpy(buf, "unknown error", buf_len);
        buf[buf_len - 1] = '\0';
    }
#else
    // Using GLIBC version

    // KUDU-1515: TSAN in Clang 3.9 has an incorrect interceptor for strerror_r:
    // https://github.com/google/sanitizers/issues/696
    ANNOTATE_IGNORE_WRITES_BEGIN();
    char* ret = strerror_r(err, buf, buf_len);
    ANNOTATE_IGNORE_WRITES_END();
    if (ret != buf) {
        strncpy(buf, ret, buf_len);
        buf[buf_len - 1] = '\0';
    }
#endif
}

} // namespace doris
