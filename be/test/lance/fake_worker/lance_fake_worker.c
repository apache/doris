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

/*
 * lance_fake_worker: a tiny protocol-speaking fake Lance index worker, exec'd
 * by the IndexJobSupervisor unit tests (be/test/lance/index_job_supervisor_test.cpp)
 * through the supervisor's force_worker_exec_for_test() seam. Pure libc +
 * pthreads (no Doris linkage) so it starts instantly and fits inside a tiny
 * cgroup memory budget (the real-cgroup OOM case pins memory.max at 32 MiB).
 *
 * Wire protocol (mirrors be/src/lance/index_worker.h): the supervisor writes
 * one length-prefixed (u32 big-endian) thrift-compact TLanceIndexJobDispatch
 * frame to the worker's stdin and closes it; the worker writes one
 * TLanceIndexWorkerHandshake frame followed by at most one TLanceIndexJobReport
 * frame to stdout. This program hand-encodes the two outbound structs in
 * thrift-compact (both have fixed sequential field layouts) and never decodes
 * the dispatch: the invocation identity it must echo back arrives via argv.
 *
 * Usage: lance_fake_worker <persona> [key=value ...]
 *   keys: job=<i64> rev=<i64> inv=<string> epoch=<i64> code=<i32> msg=<string>
 *         mark=<path> sleep=<seconds> mib=<chunk cap>
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>

#define HANDSHAKE_PROTOCOL_MAGIC INT64_C(0x4C414E43450001)
#define HANDSHAKE_PROTOCOL_VERSION 1

/* Thrift-compact type tags (only what the two outbound structs need). */
#define CT_STOP 0x00
#define CT_I32 0x05
#define CT_I64 0x06
#define CT_BINARY 0x08
#define CT_STRUCT 0x0C

#define COMPACT_BUF_BYTES 16384

/* --------------------------- compact writer --------------------------- */

struct cw {
    uint8_t data[COMPACT_BUF_BYTES];
    size_t len;
};

static void cw_put(struct cw* out, uint8_t byte) {
    if (out->len < COMPACT_BUF_BYTES) {
        out->data[out->len++] = byte;
    }
}

static void cw_varint(struct cw* out, uint64_t value) {
    for (;;) {
        if (value <= 0x7F) {
            cw_put(out, (uint8_t)value);
            return;
        }
        cw_put(out, (uint8_t)((value & 0x7F) | 0x80));
        value >>= 7;
    }
}

static void cw_zigzag64(struct cw* out, int64_t value) {
    cw_varint(out, ((uint64_t)value << 1) ^ (uint64_t)(value >> 63));
}

/* Every field written here follows its predecessor by one field id, so the
 * short form (delta << 4 | type) always applies. */
static void cw_next_field(struct cw* out, uint8_t type) {
    cw_put(out, (uint8_t)((1u << 4) | type));
}

static void cw_i64(struct cw* out, uint8_t type, int64_t value) {
    cw_next_field(out, type);
    cw_zigzag64(out, value);
}

static void cw_string(struct cw* out, const char* value) {
    const size_t len = strlen(value);
    cw_next_field(out, CT_BINARY);
    cw_varint(out, (uint64_t)len);
    for (size_t i = 0; i < len; ++i) {
        cw_put(out, (uint8_t)value[i]);
    }
}

/* ----------------------------- io helpers ----------------------------- */

/* Returns 0 on success; EPIPE (supervisor closed its read end) and any other
 * write error end the persona quietly — the supervisor's kill/reap discipline
 * is what the test observes, not this exit code. */
static int write_all(int fd, const uint8_t* data, size_t len) {
    size_t done = 0;
    while (done < len) {
        const ssize_t n = write(fd, data + done, len - done);
        if (n > 0) {
            done += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return -1;
    }
    return 0;
}

static int write_frame(int fd, const uint8_t* payload, size_t len) {
    uint8_t header[4];
    header[0] = (uint8_t)((len >> 24) & 0xFF);
    header[1] = (uint8_t)((len >> 16) & 0xFF);
    header[2] = (uint8_t)((len >> 8) & 0xFF);
    header[3] = (uint8_t)(len & 0xFF);
    if (write_all(fd, header, sizeof(header)) != 0) {
        return -1;
    }
    return write_all(fd, payload, len);
}

/* Drains the dispatch frame until the supervisor closes its write end. */
static void drain_stdin(void) {
    uint8_t chunk[8192];
    for (;;) {
        const ssize_t n = read(STDIN_FILENO, chunk, sizeof(chunk));
        if (n == 0) {
            return;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
    }
}

/* ---------------------- self-observed confinement ---------------------- */

static void read_self_cgroup_rel(char* out, size_t out_size) {
    char text[4096];
    out[0] = '\0';
    const int fd = open("/proc/self/cgroup", 0 /* O_RDONLY */);
    if (fd < 0) {
        return;
    }
    ssize_t total = 0;
    for (;;) {
        const ssize_t n = read(fd, text + total, sizeof(text) - 1 - (size_t)total);
        if (n <= 0) {
            break;
        }
        total += n;
        if ((size_t)total >= sizeof(text) - 1) {
            break;
        }
    }
    close(fd);
    text[total < 0 ? 0 : total] = '\0';
    const char* line = text;
    while (*line != '\0') {
        const char* eol = strchr(line, '\n');
        const size_t line_len = eol != NULL ? (size_t)(eol - line) : strlen(line);
        if (line_len > 3 && line[0] == '0' && line[1] == ':' && line[2] == ':') {
            size_t rel_len = line_len - 3;
            if (rel_len >= out_size) {
                rel_len = out_size - 1;
            }
            memcpy(out, line + 3, rel_len);
            out[rel_len] = '\0';
            return;
        }
        if (eol == NULL) {
            break;
        }
        line = eol + 1;
    }
}

static int64_t read_cgroup_i64(const char* rel, const char* file) {
    char path[1200];
    char text[128];
    (void)snprintf(path, sizeof(path), "/sys/fs/cgroup%s/%s", rel, file);
    const int fd = open(path, 0 /* O_RDONLY */);
    if (fd < 0) {
        return -1;
    }
    ssize_t total = 0;
    for (;;) {
        const ssize_t n = read(fd, text + total, sizeof(text) - 1 - (size_t)total);
        if (n <= 0) {
            break;
        }
        total += n;
        if ((size_t)total >= sizeof(text) - 1) {
            break;
        }
    }
    close(fd);
    text[total < 0 ? 0 : total] = '\0';
    if (strncmp(text, "max", 3) == 0) {
        return INT64_MAX;
    }
    return strtoll(text, NULL, 10);
}

static int64_t rlimit_cur(int resource) {
    struct rlimit limit;
    if (getrlimit(resource, &limit) != 0) {
        return -1;
    }
    if (limit.rlim_cur == RLIM_INFINITY) {
        return INT64_MAX;
    }
    return (int64_t)limit.rlim_cur;
}

/* ------------------------------ frames ------------------------------- */

/* A handshake reporting the worker's real confinement facts; the supervisor
 * cross-checks every value against its own cgroup write/read-back, so the
 * honest values are exactly what passes. bad_cgroup / bad_limits corrupt one
 * value each to exercise the kill-on-mismatch path. */
static void write_handshake(int bad_cgroup, int bad_limits) {
    char rel[1024];
    read_self_cgroup_rel(rel, sizeof(rel));
    int64_t memory_max = read_cgroup_i64(rel, "memory.max");
    const int64_t pids_max = read_cgroup_i64(rel, "pids.max");
    if (bad_limits) {
        memory_max += 1024;
    }
    struct cw out = {{0}, 0};
    cw_i64(&out, CT_I64, HANDSHAKE_PROTOCOL_MAGIC);
    cw_i64(&out, CT_I32, HANDSHAKE_PROTOCOL_VERSION);
    cw_string(&out, bad_cgroup ? "/bogus/not-the-invocation-cgroup" : rel);
    cw_i64(&out, CT_I64, memory_max);
    cw_i64(&out, CT_I64, pids_max);
    cw_i64(&out, CT_I64, rlimit_cur(RLIMIT_AS));
    cw_i64(&out, CT_I64, rlimit_cur(RLIMIT_CPU));
    cw_i64(&out, CT_I64, rlimit_cur(RLIMIT_NOFILE));
    cw_i64(&out, CT_I64, rlimit_cur(RLIMIT_CORE));
    cw_put(&out, CT_STOP);
    (void)write_frame(STDOUT_FILENO, out.data, out.len);
}

/* TLanceIndexJobReport fields 1..5 (+ optional 7 sanitized_message), the
 * identity quad taken from argv. invocation_suffix corrupts the identity for
 * the mismatch persona. */
static void write_result(int64_t job_id, int64_t revision, const char* invocation_id,
                         int64_t epoch, int32_t code, const char* message,
                         const char* invocation_suffix) {
    char identity[1024];
    (void)snprintf(identity, sizeof(identity), "%s%s", invocation_id,
                   invocation_suffix != NULL ? invocation_suffix : "");
    struct cw out = {{0}, 0};
    cw_i64(&out, CT_I64, job_id);
    cw_i64(&out, CT_I64, revision);
    cw_string(&out, identity);
    cw_i64(&out, CT_I64, epoch);
    cw_i64(&out, CT_I32, code);
    if (message != NULL) {
        /* Field 7: delta 2 from field 5. */
        cw_put(&out, (uint8_t)((2u << 4) | CT_BINARY));
        const size_t len = strlen(message);
        cw_varint(&out, (uint64_t)len);
        for (size_t i = 0; i < len; ++i) {
            cw_put(&out, (uint8_t)message[i]);
        }
    }
    cw_put(&out, CT_STOP);
    (void)write_frame(STDOUT_FILENO, out.data, out.len);
}

/* ------------------------------ personas ------------------------------ */

static void* sleeper_thread(void* unused) {
    (void)unused;
    for (;;) {
        pause();
    }
    return NULL;
}

int main(int argc, char** argv) {
    int64_t job_id = 0;
    int64_t revision = 0;
    int64_t epoch = 0;
    int32_t code = 5; /* NATIVE_OK */
    const char* invocation_id = "fake-invocation";
    const char* message = NULL;
    const char* mark_path = NULL;
    long orphan_sleep_seconds = 8;
    long bomb_mib_cap = 4096;

    if (argc < 2) {
        return 64;
    }
    const char* persona = argv[1];
    for (int i = 2; i < argc; ++i) {
        const char* eq = strchr(argv[i], '=');
        if (eq == NULL) {
            continue;
        }
        const size_t key_len = (size_t)(eq - argv[i]);
        const char* value = eq + 1;
        if (key_len == 3 && strncmp(argv[i], "job", 3) == 0) {
            job_id = strtoll(value, NULL, 10);
        } else if (key_len == 3 && strncmp(argv[i], "rev", 3) == 0) {
            revision = strtoll(value, NULL, 10);
        } else if (key_len == 3 && strncmp(argv[i], "inv", 3) == 0) {
            invocation_id = value;
        } else if (key_len == 5 && strncmp(argv[i], "epoch", 5) == 0) {
            epoch = strtoll(value, NULL, 10);
        } else if (key_len == 4 && strncmp(argv[i], "code", 4) == 0) {
            code = (int32_t)strtol(value, NULL, 10);
        } else if (key_len == 3 && strncmp(argv[i], "msg", 3) == 0) {
            message = value;
        } else if (key_len == 4 && strncmp(argv[i], "mark", 4) == 0) {
            mark_path = value;
        } else if (key_len == 5 && strncmp(argv[i], "sleep", 5) == 0) {
            orphan_sleep_seconds = strtol(value, NULL, 10);
        } else if (key_len == 3 && strncmp(argv[i], "mib", 3) == 0) {
            bomb_mib_cap = strtol(value, NULL, 10);
        }
    }

    if (strcmp(persona, "happy") == 0) {
        drain_stdin();
        write_handshake(0, 0);
        write_result(job_id, revision, invocation_id, epoch, code, message, NULL);
        return 0;
    }
    if (strcmp(persona, "identity_mismatch") == 0) {
        drain_stdin();
        write_handshake(0, 0);
        write_result(job_id, revision, invocation_id, epoch, code, NULL, "-WRONG");
        return 0;
    }
    if (strcmp(persona, "bad_cgroup") == 0) {
        drain_stdin();
        write_handshake(1, 0);
        return 0;
    }
    if (strcmp(persona, "bad_limits") == 0) {
        drain_stdin();
        write_handshake(0, 1);
        return 0;
    }
    if (strcmp(persona, "garbage") == 0) {
        /* A well-sized frame whose payload is not decodable compact. */
        uint8_t payload[64];
        memset(payload, 0xFF, sizeof(payload));
        (void)write_frame(STDOUT_FILENO, payload, sizeof(payload));
        return 0;
    }
    if (strcmp(persona, "oversized") == 0) {
        /* Declared length far above the 4 KiB handshake cap. */
        const uint8_t header[4] = {0x01, 0x00, 0x00, 0x00};
        uint8_t payload[256];
        memset(payload, 'B', sizeof(payload));
        (void)write_all(STDOUT_FILENO, header, sizeof(header));
        (void)write_all(STDOUT_FILENO, payload, sizeof(payload));
        return 0;
    }
    if (strcmp(persona, "deep_nest") == 0) {
        /* Nested STRUCT field headers past the decoder recursion limit (16). */
        struct cw out = {{0}, 0};
        for (int i = 0; i < 24; ++i) {
            cw_put(&out, (uint8_t)((1u << 4) | CT_STRUCT));
        }
        for (int i = 0; i < 24; ++i) {
            cw_put(&out, CT_STOP);
        }
        (void)write_frame(STDOUT_FILENO, out.data, out.len);
        return 0;
    }
    if (strcmp(persona, "flood") == 0) {
        /* Floods both streams (1 MiB each) without ever reading stdin; the
         * supervisor must drain in parallel and kill on the garbage prefix. */
        uint8_t chunk[4096];
        memset(chunk, 'A', sizeof(chunk));
        for (int i = 0; i < 256; ++i) {
            if (write_all(STDOUT_FILENO, chunk, sizeof(chunk)) != 0) {
                return 0;
            }
            if (write_all(STDERR_FILENO, chunk, sizeof(chunk)) != 0) {
                return 0;
            }
        }
        return 0;
    }
    if (strcmp(persona, "hang") == 0) {
        for (;;) {
            pause();
        }
    }
    if (strcmp(persona, "term_ignore") == 0) {
        (void)signal(SIGTERM, SIG_IGN);
        for (;;) {
            pause();
        }
    }
    if (strcmp(persona, "exit_no_frame") == 0) {
        return 7;
    }
    if (strcmp(persona, "exit_partial") == 0) {
        /* A 100-byte promise with 10 bytes delivered, then EOF. */
        const uint8_t header[4] = {0x00, 0x00, 0x00, 0x64};
        uint8_t payload[10];
        memset(payload, 'P', sizeof(payload));
        (void)write_all(STDOUT_FILENO, header, sizeof(header));
        (void)write_all(STDOUT_FILENO, payload, sizeof(payload));
        return 0;
    }
    if (strcmp(persona, "malloc_bomb") == 0) {
        drain_stdin();
        write_handshake(0, 0);
        /* Touches 1 MiB chunks until the cgroup OOM killer stops it. */
        for (long i = 0; i < bomb_mib_cap; ++i) {
            char* block = (char*)malloc(1u << 20);
            if (block == NULL) {
                continue;
            }
            memset(block, 1, 1u << 20);
        }
        return 3; /* reached only if the memory bound was not enforced */
    }
    if (strcmp(persona, "thread_bomb") == 0) {
        drain_stdin();
        write_handshake(0, 0);
        int created = 0;
        for (int i = 0; i < 512; ++i) {
            pthread_t thread;
            if (pthread_create(&thread, NULL, sleeper_thread, NULL) != 0) {
                break; /* pids.max enforcement: EAGAIN */
            }
            (void)pthread_detach(thread);
            ++created;
        }
        (void)fprintf(stderr, "thread_bomb created=%d\n", created);
        return 0;
    }
    if (strcmp(persona, "abort_now") == 0) {
        drain_stdin();
        write_handshake(0, 0);
        abort();
    }
    if (strcmp(persona, "orphan") == 0) {
        drain_stdin();
        /* The worker exits (no frames: silent death) while a grandchild keeps
         * the invocation cgroup populated. The grandchild escapes the worker's
         * process group (setpgid) so the supervisor's kill(-pgid) containment
         * cannot reach it — the documented "descendants can escape the process
         * group" case; cgroup membership is unaffected. The parent blocks on a
         * sync pipe until the escape has completed, so the supervisor's group
         * kill can never catch the grandchild pre-setpgid. */
        int sync_pipe[2];
        if (pipe(sync_pipe) != 0) {
            return 66;
        }
        const pid_t grandchild = fork();
        if (grandchild == 0) {
            (void)setpgid(0, 0);
            (void)close(STDIN_FILENO);
            (void)close(STDOUT_FILENO);
            (void)close(STDERR_FILENO);
            (void)close(sync_pipe[0]);
            const uint8_t done = 1;
            (void)write_all(sync_pipe[1], &done, 1);
            (void)close(sync_pipe[1]);
            sleep((unsigned int)orphan_sleep_seconds);
            _exit(0);
        }
        (void)close(sync_pipe[1]);
        uint8_t byte = 0;
        while (read(sync_pipe[0], &byte, 1) < 0 && errno == EINTR) {
        }
        (void)close(sync_pipe[0]);
        return 0;
    }
    if (strcmp(persona, "mark_hang") == 0) {
        /* The exec side effect the fork-barrier test looks for. */
        if (mark_path != NULL) {
            FILE* mark = fopen(mark_path, "w");
            if (mark != NULL) {
                (void)fputs("execed\n", mark);
                (void)fclose(mark);
            }
        }
        drain_stdin();
        for (;;) {
            pause();
        }
    }
    return 65;
}
