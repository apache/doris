#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# libunwind 1.6.2 hard-coded a 4 KiB page size in its AArch64 memory validator.
# On a 64 KiB-page kernel (Kunpeng/Kylin/openEuler/CentOS aarch64) mincore() and
# msync() reject its 4 KiB-aligned probes with EINVAL, every validation fails,
# unw_step() falls back to a link register that getcontext_trace never saved,
# and is_plt_entry() dereferences that garbage unvalidated. Because the static
# libunwind's weak `backtrace` alias replaces glibc's, bRPC's pre-main
# `backtrace()` warm-up crashed every aarch64 BE on such hosts before main().
#
# CI has no 64 KiB-page host, so this drives the installed libunwind.a through a
# program shaped like that warm-up while an LD_PRELOAD shim makes the kernel
# look like a 64 KiB-page one to libunwind: the same EINVAL for unaligned
# probes and a 64 KiB sysconf(_SC_PAGESIZE). Run it after
# `build-thirdparty.sh libunwind` on Linux; it only needs a C compiler.

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
TP_INSTALL_DIR="${TP_INSTALL_DIR:-${ROOT}/installed}"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

if [[ "$(uname -s)" != 'Linux' ]]; then
    echo "SKIP: GNU libunwind is only built on Linux"
    exit 0
fi

if [[ -n "${CC:-}" ]]; then
    cc="${CC}"
else
    for candidate in gcc clang cc; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            cc="${candidate}"
            break
        fi
    done
fi
[[ -n "${cc:-}" ]] || fail "no C compiler found; set CC"
[[ -f "${TP_INSTALL_DIR}/lib/libunwind.a" ]] || fail "${TP_INSTALL_DIR}/lib/libunwind.a not found; build libunwind first"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

cat >"${tmpdir}/shim64k.c" <<'EOF'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>

/* What a 64 KiB-page kernel does with libunwind's validation probes. */
#define EMU_PAGE 65536UL

int mincore(void *addr, size_t len, unsigned char *vec) {
    static int (*real)(void *, size_t, unsigned char *);
    if (!real) real = dlsym(RTLD_NEXT, "mincore");
    if ((uintptr_t)addr & (EMU_PAGE - 1)) { errno = EINVAL; return -1; }
    size_t n = (len + EMU_PAGE - 1) / EMU_PAGE;
    for (size_t i = 0; i < n; i++) {
        unsigned char tmp[EMU_PAGE / 4096];
        if (real((char *)addr + i * EMU_PAGE, EMU_PAGE, tmp) != 0) return -1;
        unsigned char any = 0;
        for (size_t j = 0; j < sizeof tmp; j++) any |= tmp[j] & 1;
        vec[i] = any;
    }
    return 0;
}
int msync(void *addr, size_t len, int flags) {
    static int (*real)(void *, size_t, int);
    if (!real) real = dlsym(RTLD_NEXT, "msync");
    if ((uintptr_t)addr & (EMU_PAGE - 1)) { errno = EINVAL; return -1; }
    return real(addr, len, flags);
}
long sysconf(int name) {
    static long (*real)(int);
    if (!real) real = dlsym(RTLD_NEXT, "sysconf");
    if (name == _SC_PAGESIZE) return EMU_PAGE;
    return real(name);
}
int getpagesize(void) { return (int)EMU_PAGE; }
EOF

cat >"${tmpdir}/premain.c" <<'EOF'
#define _GNU_SOURCE
#include <execinfo.h>
#include <link.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* be/src/common/phdr_cache.cpp before main(): the PHDR cache is still NULL, so the
 * Doris hook that the libunwind patch routes dwarf_find_proc_info() through finds
 * nothing. */
int doris_unwind_iterate_phdr(int (*callback)(struct dl_phdr_info *, size_t, void *), void *data,
                              unsigned long ip) {
    (void)callback; (void)data; (void)ip;
    return 0;
}

static void *dummy_buf[4];
static int dummy_bt = -1;

/* Zero the stack below us, like the never-touched region the warm-up ran on, so the
 * uc.regs[30] slot that _Uaarch64_getcontext_trace never writes reads as 0. */
static void __attribute__((noinline)) scrub_stack(void) {
    volatile char buf[256 * 1024];
    memset((char *)buf, 0, sizeof(buf));
    __asm__ volatile("" ::: "memory");
}

/* bRPC src/bthread/mutex.cpp: "Warm up backtrace before main()." */
__attribute__((constructor)) static void warm_up(void) {
    scrub_stack();
    dummy_bt = backtrace(dummy_buf, 4);
}

int main(void) {
    void *buf[32];
    int n = backtrace(buf, 32);
    printf("premain=%d runtime=%d pagesz=%ld\n", dummy_bt, n, sysconf(_SC_PAGESIZE));
    return (dummy_bt < 0 || n < 0) ? 1 : 0;
}
EOF

extra_libs=()
[[ -f "${TP_INSTALL_DIR}/lib/libz.a" ]] && extra_libs+=(-lz)

"${cc}" -O2 -g -shared -fPIC -o "${tmpdir}/shim64k.so" "${tmpdir}/shim64k.c" -ldl
# -u unw_backtrace pulls libunwind's backtrace.o in, whose weak `backtrace` alias then
# takes over the call, exactly as it does in doris_be.
"${cc}" -O2 -g -fno-omit-frame-pointer -o "${tmpdir}/premain" "${tmpdir}/premain.c" \
    -I"${TP_INSTALL_DIR}/include" -L"${TP_INSTALL_DIR}/lib" \
    -Wl,-u,unw_backtrace -lunwind -llzma "${extra_libs[@]}" -lpthread

if command -v nm >/dev/null 2>&1; then
    nm "${tmpdir}/premain" | grep -Eq ' [WT] backtrace$' ||
        fail "backtrace() is not bound to libunwind's unw_backtrace in the test program"
fi

echo "== native page size =="
out="$("${tmpdir}/premain")" || fail "pre-main backtrace() crashed on the native page size: ${out}"
echo "${out}"

echo "== emulated 64 KiB page size =="
out="$(LD_PRELOAD="${tmpdir}/shim64k.so" "${tmpdir}/premain")" ||
    fail "pre-main backtrace() crashed on an emulated 64 KiB-page kernel: ${out}"
echo "${out}"
[[ "${out}" == *"pagesz=65536"* ]] || fail "the 64 KiB page emulation did not take effect: ${out}"

echo "PASS"
