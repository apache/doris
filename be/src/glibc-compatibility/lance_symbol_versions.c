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
#include <pthread.h>
#include <spawn.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

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

typedef void (*doris_tls_destructor)(void*);

struct doris_tls_destructor_entry {
    doris_tls_destructor destructor;
    void* object;
    struct doris_tls_destructor_entry* next;
};

static pthread_key_t doris_tls_destructor_key;
static pthread_once_t doris_tls_destructor_once = PTHREAD_ONCE_INIT;

static void doris_run_tls_destructors(void* value) {
    struct doris_tls_destructor_entry* entry = value;
    while (entry != NULL) {
        struct doris_tls_destructor_entry* next = entry->next;
        doris_tls_destructor destructor = entry->destructor;
        void* object = entry->object;

        // Publish the remainder before invoking the destructor. A destructor
        // may register another TLS destructor, which must run before the older
        // entries that are still pending.
        if (pthread_setspecific(doris_tls_destructor_key, next) != 0) {
            abort();
        }
        free(entry);
        destructor(object);
        entry = pthread_getspecific(doris_tls_destructor_key);
    }
}

static void doris_run_main_thread_tls_destructors(void) {
    doris_run_tls_destructors(pthread_getspecific(doris_tls_destructor_key));
}

static void doris_init_tls_destructor_key(void) {
    if (pthread_key_create(&doris_tls_destructor_key, doris_run_tls_destructors) != 0) {
        abort();
    }
    // pthread_key_create() arranges for doris_run_tls_destructors() to be
    // called automatically when an ordinary worker thread exits. However,
    // returning from main() (which is equivalent to exit()) does not run the
    // initial thread's pthread key destructors. Register an atexit handler so
    // TLS destructors belonging to the thread that performs normal process
    // termination are still invoked.
    //
    // This relies on Doris normally terminating the process from its initial
    // thread. If another thread calls exit(), atexit handlers execute in that
    // thread and pthread_getspecific() observes that thread's TLS state.
    // atexit handlers are not invoked by abort(), _exit(), fatal signals, or
    // SIGKILL; those paths are already abnormal process termination.
    //
    // Unlike glibc, this fallback puts the initial thread's TLS cleanup in the
    // same LIFO list as atexit callbacks and static-object destructors. A static
    // object initialized after this handler is registered is therefore destroyed
    // before the TLS objects. We accept this limitation because Doris normally
    // terminates with _exit(), and normal destructor processing is currently used
    // only when enable_graceful_exit_check is enabled for sanitizer leak checks.
    // TLS destructors on that diagnostic path must not access static-lifetime
    // objects that may already have been destroyed. If graceful exit becomes a
    // production path, the initial thread's TLS destructors must instead be run
    // explicitly before the atexit/static-destructor list.
    //
    // A registration failure means that normal main-thread TLS cleanup cannot
    // be guaranteed, so fail immediately instead of continuing with a partially
    // installed compatibility implementation.
    if (atexit(doris_run_main_thread_tls_destructors) != 0) {
        abort();
    }
}

extern __typeof__(posix_spawnp) __doris_old_posix_spawnp;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawnp, posix_spawnp, DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_init) __doris_old_posix_spawn_file_actions_init;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_init, posix_spawn_file_actions_init,
                   DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_destroy) __doris_old_posix_spawn_file_actions_destroy;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_destroy, posix_spawn_file_actions_destroy,
                   DORIS_GLIBC_BASE_VERSION);

extern __typeof__(posix_spawn_file_actions_adddup2) __doris_old_posix_spawn_file_actions_adddup2;
DORIS_GLIBC_SYMVER(__doris_old_posix_spawn_file_actions_adddup2, posix_spawn_file_actions_adddup2,
                   DORIS_GLIBC_BASE_VERSION);

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

// Rust std weak-links copy_file_range and otherwise issues the syscall itself.
// Provide that syscall path locally so linking on glibc 2.27 does not attach a
// GLIBC_2.27 version requirement. Old kernels return ENOSYS and Rust falls back
// to its generic copy loop.
DORIS_HIDDEN ssize_t copy_file_range(int fd_in, off64_t* offset_in, int fd_out, off64_t* offset_out,
                                     size_t length, unsigned int flags) {
    return (ssize_t)syscall(SYS_copy_file_range, fd_in, offset_in, fd_out, offset_out, length,
                            flags);
}

// Rust std weak-links this glibc 2.18 entry point and has an internal fallback
// when it is absent. Since the LDB sysroot exposes it, the final linker would
// otherwise record GLIBC_2.18. Supply equivalent pthread-key based registration
// locally. All callers are linked into doris_be, so dso_symbol tracking for
// dlclose is intentionally unnecessary.
// See https://github.com/rust-lang/rust/issues/57497 for more details.
DORIS_HIDDEN int __cxa_thread_atexit_impl(doris_tls_destructor destructor, void* object,
                                          void* dso_symbol) {
    (void)dso_symbol;
    if (pthread_once(&doris_tls_destructor_once, doris_init_tls_destructor_key) != 0) {
        abort();
    }

    struct doris_tls_destructor_entry* entry = malloc(sizeof(*entry));
    if (entry == NULL) {
        abort();
    }
    entry->destructor = destructor;
    entry->object = object;
    entry->next = pthread_getspecific(doris_tls_destructor_key);
    if (pthread_setspecific(doris_tls_destructor_key, entry) != 0) {
        free(entry);
        abort();
    }
    return 0;
}
