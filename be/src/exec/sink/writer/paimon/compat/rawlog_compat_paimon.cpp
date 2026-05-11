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

#include <cstdarg>
#include <cstdio>

namespace {
void raw_log_impl(int severity, const char* file, int line, const char* format, va_list ap) {
    char buffer[2048];
    std::vsnprintf(buffer, sizeof(buffer), format, ap);
    const char* sev = "INFO";
    if (severity == 1)
        sev = "WARN";
    else if (severity == 2)
        sev = "ERROR";
    else if (severity == 3)
        sev = "FATAL";
    std::fprintf(stderr, "[%s] %s:%d %s\n", sev, file ? file : "", line, buffer);
}
} // namespace

extern "C" void rawlog_compat_paimon(int severity, const char* file, int line, const char* format,
                                     ...) __asm__("_ZN6google8RawLog__ENS_11LogSeverityEPKciS2_z");

extern "C" void rawlog_compat_paimon(int severity, const char* file, int line, const char* format,
                                     ...) {
    va_list ap;
    va_start(ap, format);
    raw_log_impl(severity, file, line, format, ap);
    va_end(ap);
}
