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

// Compatibility stubs for glibc symbols that aws-lc-rs (Rust TLS crypto)
// references but are only available in newer glibc versions.
//
// All functions use __attribute__((weak)) so that if the real glibc
// provides them, the real versions take precedence. This prevents
// duplicate symbol errors on newer systems.

#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define WEAK __attribute__((weak))

// glibc 2.38+: ISO C23 scanning/parsing functions
WEAK int __isoc23_sscanf(const char* s, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int r = vsscanf(s, fmt, ap);
    va_end(ap);
    return r;
}

WEAK long __isoc23_strtol(const char* s, char** endp, int base) {
    return strtol(s, endp, base);
}

WEAK unsigned long __isoc23_strtoul(const char* s, char** endp, int base) {
    return strtoul(s, endp, base);
}

WEAK unsigned long long __isoc23_strtoull(const char* s, char** endp, int base) {
    return strtoull(s, endp, base);
}

// glibc 2.32+: thread safety indicator
WEAK char __libc_single_threaded = 0;

// glibc 2.30+: clock-aware pthread functions
WEAK int pthread_cond_clockwait(pthread_cond_t* cond, pthread_mutex_t* mutex, clockid_t clock_id,
                                const struct timespec* abstime) {
    (void)clock_id;
    return pthread_cond_timedwait(cond, mutex, abstime);
}

WEAK int pthread_mutex_clocklock(pthread_mutex_t* mutex, clockid_t clock_id,
                                 const struct timespec* abstime) {
    (void)clock_id;
    return pthread_mutex_timedlock(mutex, abstime);
}
