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

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <sys/syscall.h>
#include <unistd.h>

#if !defined(SYS_getrandom) && defined(__NR_getrandom)
#define SYS_getrandom __NR_getrandom
#endif

// Why this compatibility symbol is needed:
//
// CloudUT ASAN executables are built against glibc 2.27 and run against glibc
// 2.17. The ASAN runtime exports a getentropy interceptor, so libfdb_c's weak
// symbol check concludes that getentropy is available. At runtime, however,
// the interceptor cannot resolve a next libc implementation (RTLD_NEXT is
// null on glibc 2.17) and calling it jumps to address zero.
//
// This file is linked only into Linux ASAN test executables that initialize
// the FDB network. Its strong, exported symbol gives libfdb_c a valid target
// in those processes. Calling the kernel directly is important: forwarding to
// a libc entropy function could enter the same interceptor path again.

// Build headers may define SYS_getrandom even when an older worker kernel does
// not implement it. Fall back to /dev/urandom only when the syscall reports
// ENOSYS.
static int fill_from_urandom(unsigned char* output, size_t length) {
    int fd;
    do {
        fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        return -1;
    }

    while (length > 0) {
        const ssize_t bytes_read = read(fd, output, length);
        if (bytes_read > 0) {
            output += bytes_read;
            length -= (size_t)bytes_read;
            continue;
        }
        if (bytes_read < 0 && errno == EINTR) {
            continue;
        }

        const int saved_errno = bytes_read == 0 ? EIO : errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }

    return close(fd);
}

int getentropy(void* buffer, size_t length) {
    // Match the getentropy(3) contract. FDB requests small buffers, but keeping
    // the standard 256-byte limit makes this a safe process-wide replacement.
    if (length > 256) {
        errno = EIO;
        return -1;
    }

    unsigned char* output = (unsigned char*)buffer;

#if defined(SYS_getrandom)
    // Use syscall rather than libc getrandom/getentropy so this implementation
    // cannot recurse through an ASAN interceptor.
    while (length > 0) {
        const long bytes_read = syscall(SYS_getrandom, output, length, 0);
        if (bytes_read > 0) {
            output += bytes_read;
            length -= (size_t)bytes_read;
            continue;
        }
        if (bytes_read < 0 && errno == EINTR) {
            continue;
        }
        if (bytes_read < 0 && errno == ENOSYS) {
            break;
        }
        if (bytes_read == 0) {
            errno = EIO;
        }
        return -1;
    }

    if (length == 0) {
        return 0;
    }
#endif

    return fill_from_urandom(output, length);
}
