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

// Instrumented half of the ASAN stale-stack-poison reproducer.
// See asan-glog-rca.md at the repository root for the analysis.
//
// Part 1 (deterministic evidence, never crashes):
//   descend instrumented frames -> throw from an UN-instrumented TU ->
//   catch -> prove with __asan_region_is_poisoned() that the unwound region
//   kept its shadow poison. A control run throwing from INSTRUMENTED code
//   shows the asymmetry (the compiler emits __asan_handle_no_return() before
//   instrumented throws only).
//
// Part 2 (the real Jira crash site):
//   drive the exact production path from QA-202 / DORIS-1073 / DORIS-15154
//   with a real ThriftClient<FrontendServiceClient> against an in-process
//   dead peer that RSTs every connection:
//       report() RPC fails -> libthrift (un-instrumented) throws
//       TTransportException through the instrumented gensrc frames (poison)
//       -> ThriftClientImpl::close() -> TBufferedTransport::close() ->
//       flush() -> TSocket::write_partial() -> getSocketInfo() locals land
//       on the stale poison -> ASAN false positive -> kCurrentStackFrameMagic
//       CHECK abort.
//   Alignment between the poison stripes and thrift's locals is the only
//   probabilistic part, so the close() descent is swept over 257 different
//   stack offsets with an alloca() pad (8-byte step over 2KB).
//
// Part 3 (fallback): if part 2 somehow survives while part 1 proved the
//   poison exists, trigger the synthetic crash site instead so CI still
//   fails deterministically.

#include "common/asan_repro.h"

#ifdef ADDRESS_SANITIZER

#include <unistd.h>

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#if defined(__linux__)
#include <alloca.h>
#include <arpa/inet.h>
#include <gen_cpp/FrontendService.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>

#include "common/status.h"
#include "util/thrift_client.h"
#endif // __linux__

// Provided by the statically linked ASAN runtime. Query-only, never reports.
extern "C" void* __asan_region_is_poisoned(void* beg, size_t size);

namespace doris {
namespace {

using asan_repro::ProbeResult;

// Depth of the instrumented descent that the exception unwinds, and depth of
// the un-instrumented probe descent. Instrumented frames are much fatter
// (redzones around every local), so the probe descends deeper to be sure it
// spans the whole poisoned region.
constexpr int kThrowDepth = 32;
constexpr int kProbeDepth = 160;

// Deterministic logging even when the deep stack region carries stale
// poison: vsnprintf/write only get a global buffer range checked by their
// interceptors, never a stack local placed in the dirty zone.
char g_log_buf[4096];

void repro_log(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(g_log_buf, sizeof(g_log_buf), fmt, ap);
    va_end(ap);
    if (len <= 0) {
        return;
    }
    size_t to_write = static_cast<size_t>(len) < sizeof(g_log_buf) ? static_cast<size_t>(len)
                                                                   : sizeof(g_log_buf) - 1;
    ssize_t written = write(STDERR_FILENO, g_log_buf, to_write);
    (void)written;
}

// ---------------------------------------------------------------------------
// Part 1: deterministic stale-poison measurement (synthetic frames)
// ---------------------------------------------------------------------------

__attribute__((noinline)) void instrumented_throw() {
    // Control case: for a throw compiled WITH ASAN instrumentation the
    // compiler emits __asan_handle_no_return() right before __cxa_throw,
    // which wipes the shadow of everything below the catch frame, so no
    // stale poison is left behind.
    throw 42;
}

__attribute__((noinline)) void deep_instrumented(int level, bool use_uninstrumented_throw) {
    // Three separate arrays so every frame contributes dense left/mid/right
    // redzone stripes to the shadow. No destructors and no try-blocks: the
    // frames must not have landing pads, so phase-2 unwinding skips them
    // without running any cleanup code.
    char a[96];
    char b[96];
    char c[96];
    a[0] = b[0] = c[0] = static_cast<char>(level);
    asm volatile("" : : "r"(a), "r"(b), "r"(c) : "memory");
    if (level + 1 < kThrowDepth) {
        deep_instrumented(level + 1, use_uninstrumented_throw);
    } else if (use_uninstrumented_throw) {
        asan_repro::uninstrumented_throw();
    } else {
        instrumented_throw();
    }
}

__attribute__((noinline)) void run_scenario(bool use_uninstrumented_throw, ProbeResult* out) {
    try {
        deep_instrumented(0, use_uninstrumented_throw);
    } catch (int) {
        // The frames between the throw site and here have been unwound. If
        // __asan_handle_no_return() did not run at throw time, their shadow
        // poison is still on this thread's stack.
    }
    // Immediately re-enter the unwound region from un-instrumented code,
    // exactly like the production retry path does.
    asan_repro::probe_and_scrub(0, kProbeDepth, out);
}

// ---------------------------------------------------------------------------
// Part 2: the real production path (Jira stack site)
// ---------------------------------------------------------------------------

#if defined(__linux__)

// A minimal in-process "dead FE": accepts every connection and immediately
// closes it with SO_LINGER=0 so the peer gets a hard RST. Any later thrift
// write on the client side fails inside TSocket::write_partial(), which is
// where getSocketInfo()/GlobalOutput.perror() build their error strings --
// the exact crash frames of QA-202 / DORIS-1073 / DORIS-15154.
class DeadPeerServer {
public:
    bool start() {
        _listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (_listen_fd < 0) {
            return false;
        }
        int reuse = 1;
        ::setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0; // ephemeral
        if (::bind(_listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(_listen_fd, 128) != 0) {
            ::close(_listen_fd);
            _listen_fd = -1;
            return false;
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(_listen_fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            ::close(_listen_fd);
            _listen_fd = -1;
            return false;
        }
        _port = ntohs(addr.sin_port);
        _accept_thread = std::thread([this]() { accept_loop(); });
        return true;
    }

    void stop() {
        _stopping.store(true);
        if (_listen_fd >= 0) {
            ::shutdown(_listen_fd, SHUT_RDWR);
            ::close(_listen_fd);
        }
        if (_accept_thread.joinable()) {
            _accept_thread.join();
        }
    }

    int port() const { return _port; }

private:
    void accept_loop() {
        while (!_stopping.load()) {
            int conn = ::accept(_listen_fd, nullptr, nullptr);
            if (conn < 0) {
                if (_stopping.load()) {
                    break;
                }
                continue;
            }
            struct linger lg;
            lg.l_onoff = 1;
            lg.l_linger = 0;
            ::setsockopt(conn, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg)); // close => RST
            ::close(conn);
        }
    }

    int _listen_fd = -1;
    int _port = 0;
    std::atomic<bool> _stopping {false};
    std::thread _accept_thread;
};

// The real failed RPC of the production stacks: FrontendServiceClient::report
// serializes TReportRequest through the instrumented gensrc frames and fails
// inside un-instrumented libthrift (peer RST) -> TTransportException unwinds
// through the gensrc frames -> stale poison. Also doubles as a sync point:
// once this throws, the RST has definitely arrived on this connection.
__attribute__((noinline)) bool real_rpc_attempt(ThriftClient<FrontendServiceClient>* client) {
    try {
        TMasterResult res;
        TReportRequest req;
        client->iface()->report(res, req);
    } catch (const std::exception&) {
        return true; // thrown from libthrift, as in production
    }
    return false;
}

// Make sure TBufferedTransport still holds unflushed bytes, so that the
// following close() takes the close -> flush -> write_partial path of the
// Jira stacks. The write is buffer-only unless the buffer overflows, in
// which case it throws just like production; both outcomes are fine.
void stuff_pending_bytes(ThriftClient<FrontendServiceClient>* client) {
    try {
        uint8_t junk[64];
        memset(junk, 'x', sizeof(junk));
        client->iface()->getOutputProtocol()->getTransport()->write(
                junk, static_cast<uint32_t>(sizeof(junk)));
    } catch (const std::exception&) {
        // buffer overflow flush on a dead socket: same poison, keep going
    }
}

// The crash site of QA-202 / DORIS-15154: ThriftClientImpl::close() ->
// TBufferedTransport::close() -> flush() -> TSocket::write_partial() ->
// getSocketInfo(). The alloca pad shifts the whole descent by `pad` bytes so
// that across the sweep thrift's locals are guaranteed to overlap the stale
// poison stripes at some offset.
__attribute__((noinline)) void padded_close(ThriftClientImpl* client, int pad) {
    char* p = static_cast<char*>(alloca(static_cast<size_t>(pad) + 16));
    p[0] = 1;
    asm volatile("" : : "r"(p) : "memory");
    client->close();
}

// Returns only if the bug did not trigger (e.g. a fixed build).
void real_path_sweep(int port) {
    for (int pad = 0; pad <= 2048; pad += 8) {
        if (pad % 256 == 0) {
            repro_log("[ASAN-REPRO] part2: sweep pad=%d/2048 (still alive)\n", pad);
        }
        ThriftClient<FrontendServiceClient> client("127.0.0.1", port);
        client.set_conn_timeout(2000);
        client.set_send_timeout(2000);
        client.set_recv_timeout(2000);
        Status st = client.open();
        if (!st.ok()) {
            repro_log("[ASAN-REPRO] part2: open failed at pad=%d: %s\n", pad,
                      st.to_string().c_str());
            continue;
        }
        real_rpc_attempt(&client); // poison: real TTransportException from libthrift
        stuff_pending_bytes(&client);
        padded_close(&client, pad); // Jira crash site: close -> flush -> getSocketInfo
        // ~ThriftClient runs close() again: one more shot at a slightly
        // different depth, matching ClientCacheHelper::reopen_client's
        // close() + delete sequence.
    }
}

#endif // __linux__

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

void repro_thread_body() {
    char sym[512];
    asan_repro::describe_repro_symbols(sym, sizeof(sym));
    repro_log("%s", sym);

    {
        // Probe calibration: __asan_region_is_poisoned() must see the
        // redzone right after this 16-byte local, otherwise the probe
        // methodology itself is broken.
        char cal[16];
        asm volatile("" : : "r"(cal) : "memory");
        const void* redzone = __asan_region_is_poisoned(cal, 64);
        repro_log("[ASAN-REPRO] probe calibration (expect non-null): %p\n", redzone);
    }

    ProbeResult control;
    run_scenario(false, &control);
    repro_log(
            "[ASAN-REPRO] part1 control (throw from INSTRUMENTED code):   %d/%d probe frames "
            "on stale poison, first level=%d addr=%p\n",
            control.poisoned_levels, kProbeDepth, control.first_poisoned_level,
            control.first_poisoned_addr);

    ProbeResult test;
    run_scenario(true, &test);
    repro_log(
            "[ASAN-REPRO] part1 test    (throw from UN-INSTRUMENTED code): %d/%d probe frames "
            "on stale poison, first level=%d addr=%p\n",
            test.poisoned_levels, kProbeDepth, test.first_poisoned_level, test.first_poisoned_addr);
    if (test.poisoned_levels > 0) {
        repro_log(
                "[ASAN-REPRO] part1 verdict: stale stack shadow poison CONFIRMED after an "
                "un-instrumented throw. Expecting part2 to crash at the Jira stack site.\n");
    } else {
        repro_log(
                "[ASAN-REPRO] part1 verdict: no stale poison detected. Running part2 as "
                "confirmation; it should survive.\n");
    }

#if defined(__linux__)
    DeadPeerServer server;
    if (server.start()) {
        repro_log(
                "[ASAN-REPRO] part2: dead-peer FE listening on 127.0.0.1:%d, driving the real "
                "path: report() fails -> ThriftClientImpl::close() -> TBufferedTransport::"
                "close/flush -> TSocket::write_partial -> getSocketInfo()\n"
                "[ASAN-REPRO] part2: a crash below with frames like the Jira stacks IS the "
                "expected POSITIVE result, e.g.\n"
                "[ASAN-REPRO]   AddressSanitizer: CHECK failed: asan_thread.cpp:... "
                "\"((ptr[0] == kCurrentStackFrameMagic)) != (0)\" (0x0, 0x0)\n",
                server.port());
        real_path_sweep(server.port());
        server.stop();
        repro_log("[ASAN-REPRO] part2: sweep finished WITHOUT crashing.\n");
    } else {
        repro_log("[ASAN-REPRO] part2: failed to start in-process dead peer, skipped\n");
    }
#else
    repro_log("[ASAN-REPRO] part2: real-path repro is linux-only, skipped\n");
#endif

    if (test.poisoned_levels == 0) {
        repro_log(
                "[ASAN-REPRO] RESULT: NEGATIVE - no stale stack shadow poison after an "
                "un-instrumented throw and the real path survived. The bug is NOT present in "
                "this build (fixed, or the link layout changed). BE continues to start.\n");
        return;
    }

    repro_log(
            "[ASAN-REPRO] part3: real path survived although poison exists (alignment luck); "
            "triggering the synthetic crash site via an intercepted memset() instead.\n"
            "[ASAN-REPRO] expected: AddressSanitizer: CHECK failed: asan_thread.cpp:... "
            "\"((ptr[0] == kCurrentStackFrameMagic)) != (0)\" (0x0, 0x0)\n");
    asan_repro::descend_and_boom(0, kProbeDepth);
    repro_log(
            "[ASAN-REPRO] UNEXPECTED: part3 returned without crashing although stale poison "
            "was found. Please report this output.\n");
}

} // namespace

void run_asan_stale_poison_repro() {
    if (getenv("DORIS_DISABLE_ASAN_REPRO") != nullptr) {
        repro_log("[ASAN-REPRO] disabled by DORIS_DISABLE_ASAN_REPRO\n");
        return;
    }
    repro_log(
            "[ASAN-REPRO] ==============================================================\n"
            "[ASAN-REPRO] Deliberate reproducer for the ASAN stale-stack-poison BE crash\n"
            "[ASAN-REPRO] (thrift reopen / glog LOG(WARNING) cores: QA-202, DORIS-1073,\n"
            "[ASAN-REPRO] DORIS-15154, google/glog#978, google/sanitizers#1010).\n"
            "[ASAN-REPRO] Root cause analysis: asan-glog-rca.md in the repository root.\n"
            "[ASAN-REPRO] A crash right below IS the expected POSITIVE result.\n"
            "[ASAN-REPRO] ==============================================================\n");
    // A dedicated thread gives a pristine stack, which makes the layout (and
    // therefore the reproduction) deterministic.
    std::thread t(repro_thread_body);
    t.join();
    repro_log("[ASAN-REPRO] done (process still alive => bug not reproduced)\n");
}

} // namespace doris

#else // !ADDRESS_SANITIZER

namespace doris {
void run_asan_stale_poison_repro() {}
} // namespace doris

#endif // ADDRESS_SANITIZER
