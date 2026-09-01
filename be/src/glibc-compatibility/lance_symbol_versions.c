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

#include <fcntl.h>
#include <spawn.h>
#include <sys/uio.h>

#if defined(__x86_64__)
#define DORIS_GLIBC_BASE_VERSION "GLIBC_2.2.5"
#define DORIS_GLIBC_PREADV_VERSION "GLIBC_2.10"
#define DORIS_GLIBC_SPLICE_VERSION "GLIBC_2.5"
#elif defined(__aarch64__)
#define DORIS_GLIBC_BASE_VERSION "GLIBC_2.17"
#define DORIS_GLIBC_PREADV_VERSION "GLIBC_2.17"
#define DORIS_GLIBC_SPLICE_VERSION "GLIBC_2.17"
#else
#error Unsupported architecture for Lance libc symbol version adapters.
#endif

#define DORIS_GLIBC_SYMVER(alias, symbol, version) \
    __asm__(".symver " #alias "," #symbol "@" version)

// Resolve late libc references from the static lance_c archive without exporting
// process-wide interposers. Each wrapper forwards to an explicitly versioned
// glibc symbol, so it cannot recurse back into the hidden wrapper.
#define DORIS_HIDDEN __attribute__((visibility("hidden")))

extern __typeof__(posix_spawnp) __doris_old_posix_spawnp;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawnp, posix_spawnp, DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_init) __doris_old_posix_spawn_file_actions_init;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_init, posix_spawn_file_actions_init,
                   DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_destroy) __doris_old_posix_spawn_file_actions_destroy;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_destroy, posix_spawn_file_actions_destroy,
                   DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_adddup2) __doris_old_posix_spawn_file_actions_adddup2;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_adddup2,
                   posix_spawn_file_actions_adddup2, DORIS_GLIBC_BASE_VERSION);

extern __typeof__(preadv) __doris_old_preadv;
DORIS_GLIBC_SYMVER(__doris_old_preadv, preadv, DORIS_GLIBC_PREADV_VERSION);

extern __typeof__(splice) __doris_old_splice;
DORIS_GLIBC_SYMVER(__doris_old_splice, splice, DORIS_GLIBC_SPLICE_VERSION);

DORIS_HIDDEN int posix_spawnp(pid_t* pid, const char* file,
                              const posix_spawn_file_actions_t* file_actions,
                              const posix_spawnattr_t* attr, char* const argv[],
                              char* const envp[]) {
    return __doris_old_posix_spawnp(pid, file, file_actions, attr, argv, envp);
}

DORIS_HIDDEN int posix_spawn_file_actions_init(posix_spawn_file_actions_t* file_actions) {
    return __doris_old_posix_spawn_file_actions_init(file_actions);
}

DORIS_HIDDEN int posix_spawn_file_actions_destroy(posix_spawn_file_actions_t* file_actions) {
    return __doris_old_posix_spawn_file_actions_destroy(file_actions);
}

DORIS_HIDDEN int posix_spawn_file_actions_adddup2(posix_spawn_file_actions_t* file_actions, int fd,
                                                  int new_fd) {
    return __doris_old_posix_spawn_file_actions_adddup2(file_actions, fd, new_fd);
}

DORIS_HIDDEN ssize_t preadv(int fd, const struct iovec* iov, int iov_count, off_t offset) {
    return __doris_old_preadv(fd, iov, iov_count, offset);
}

DORIS_HIDDEN ssize_t splice(int fd_in, off64_t* offset_in, int fd_out, off64_t* offset_out,
                            size_t length, unsigned int flags) {
    return __doris_old_splice(fd_in, offset_in, fd_out, offset_out, length, flags);
}
