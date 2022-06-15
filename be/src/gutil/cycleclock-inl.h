// Copyright (C) 1999-2007 Google, Inc.
//
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
//
// All rights reserved.
// Extracted from base/timer.h by jrvb

// The implementation of CycleClock::Now()
// See cycleclock.h
//
// IWYU pragma: private, include "base/cycleclock.h"

// NOTE: only i386 and x86_64 have been well tested.
// PPC, sparc, alpha, and ia64 are based on
//    http://peter.kuscsik.com/wordpress/?p=14
// with modifications by m3b.  See also
//    https://setisvn.ssl.berkeley.edu/svn/lib/fftw-3.0.1/kernel/cycle.h

#pragma once

#include <sys/time.h>

#include "gutil/arm_instruction_set_select.h"
#include "gutil/port.h"

// Please do not nest #if directives.  Keep one section, and one #if per
// platform.

// For historical reasons, the frequency on some platforms is scaled to be
// close to the platform's core clock frequency.  This is not guaranteed by the
// interface, and may change in future implementations.

// ----------------------------------------------------------------
#if defined(__APPLE__)
#include <mach/mach_time.h>
inline int64 CycleClock::Now() {
    // this goes at the top because we need ALL Macs, regardless of
    // architecture, to return the number of "mach time units" that
    // have passed since startup.  See sysinfo.cc where
    // InitializeSystemInfo() sets the supposed cpu clock frequency of
    // macs to the number of mach time units per second, not actual
    // CPU clock frequency (which can change in the face of CPU
    // frequency scaling).  Also note that when the Mac sleeps, this
    // counter pauses; it does not continue counting, nor does it
    // reset to zero.
    return mach_absolute_time();
}

// ----------------------------------------------------------------
#elif defined(__i386__)
inline int64 CycleClock::Now() {
    int64 ret;
    __asm__ volatile("rdtsc" : "=A"(ret));
    return ret;
}

// ----------------------------------------------------------------
#elif defined(__x86_64__) || defined(__amd64__)
inline int64 CycleClock::Now() {
    uint64 low, high;
    __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
    return (high << 32) | low;
}

// ----------------------------------------------------------------
#elif defined(__powerpc__) || defined(__ppc__)
#define SPR_TB 268
#define SPR_TBU 269
inline int64 CycleClock::Now() {
    uint64 time_base_value;
    if (sizeof(void*) == 8) {
        // On PowerPC64, time base can be read with one SPR read.
        asm volatile("mfspr %0, %1" : "=r"(time_base_value) : "i"(SPR_TB));
    } else {
        uint32 tbl, tbu0, tbu1;
        asm volatile(
                " mfspr %0, %3\n"
                " mfspr %1, %4\n"
                " mfspr %2, %3\n"
                : "=r"(tbu0), "=r"(tbl), "=r"(tbu1)
                : "i"(SPR_TBU), "i"(SPR_TB));
        // If there is a carry into the upper half, it is okay to return
        // (tbu1, 0) since it must be between the 2 TBU reads.
        tbl &= -static_cast<uint32>(tbu0 == tbu1);
        // high 32 bits in tbu1; low 32 bits in tbl  (tbu0 is garbage)
        time_base_value = (static_cast<uint64>(tbu1) << 32) | static_cast<uint64>(tbl);
    }
    return static_cast<int64>(time_base_value);
}

// ----------------------------------------------------------------
#elif defined(__sparc__)
inline int64 CycleClock::Now() {
    int64 tick;
    asm(".byte 0x83, 0x41, 0x00, 0x00");
    asm("mov   %%g1, %0" : "=r"(tick));
    return tick;
}

// ----------------------------------------------------------------
#elif defined(__ia64__)
inline int64 CycleClock::Now() {
    int64 itc;
    asm("mov %0 = ar.itc" : "=r"(itc));
    return itc;
}

// ----------------------------------------------------------------
#elif defined(_MSC_VER) && defined(_M_IX86)
inline int64 CycleClock::Now() {
    // Older MSVC compilers (like 7.x) don't seem to support the
    // __rdtsc intrinsic properly, so I prefer to use _asm instead
    // when I know it will work.  Otherwise, I'll use __rdtsc and hope
    // the code is being compiled with a non-ancient compiler.
    _asm rdtsc
}

// ----------------------------------------------------------------
#elif defined(_MSC_VER)
// For MSVC, we want to use '_asm rdtsc' when possible (since it works
// with even ancient MSVC compilers), and when not possible the
// __rdtsc intrinsic, declared in <intrin.h>.  Unfortunately, in some
// environments, <windows.h> and <intrin.h> have conflicting
// declarations of some other intrinsics, breaking compilation.
// Therefore, we simply declare __rdtsc ourselves. See also
// http://connect.microsoft.com/VisualStudio/feedback/details/262047
extern "C" uint64 __rdtsc();
#pragma intrinsic(__rdtsc)
inline int64 CycleClock::Now() {
    return __rdtsc();
}

// ----------------------------------------------------------------
#elif defined(ARMV6) // V6 is the earliest arm that has a standard cyclecount
#include "gutil/sysinfo.h"
inline int64 CycleClock::Now() {
    uint32 pmccntr;
    uint32 pmuseren;
    uint32 pmcntenset;
    // Read the user mode perf monitor counter access permissions.
    asm volatile("mrc p15, 0, %0, c9, c14, 0" : "=r"(pmuseren));
    if (pmuseren & 1) { // Allows reading perfmon counters for user mode code.
        asm volatile("mrc p15, 0, %0, c9, c12, 1" : "=r"(pmcntenset));
        if (pmcntenset & 0x80000000ul) { // Is it counting?
            asm volatile("mrc p15, 0, %0, c9, c13, 0" : "=r"(pmccntr));
            // The counter is set up to count every 64th cycle
            return static_cast<int64>(pmccntr) * 64; // Should optimize to << 6
        }
    }
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64>((tv.tv_sec + tv.tv_usec * 0.000001) * CyclesPerSecond());
}

// ----------------------------------------------------------------
#elif defined(ARMV3)
#include "gutil/sysinfo.h" // for CyclesPerSecond()
inline int64 CycleClock::Now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64>((tv.tv_sec + tv.tv_usec * 0.000001) * CyclesPerSecond());
}

// ----------------------------------------------------------------
#elif defined(__mips__)
#include "gutil/sysinfo.h"
inline int64 CycleClock::Now() {
    // mips apparently only allows rdtsc for superusers, so we fall
    // back to gettimeofday.  It's possible clock_gettime would be better.
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64>((tv.tv_sec + tv.tv_usec * 0.000001) * CyclesPerSecond());
}

// ----------------------------------------------------------------
#elif defined(__aarch64__)
#include "gutil/sysinfo.h"
inline int64 CycleClock::Now() {
    // System timer of ARMv8 runs at a different frequency than the CPU's.
    // The frequency is fixed, typically in the range 1-50MHz.  It can be
    // read at CNTFRQ special register.  We assume the OS has set up
    // the virtual timer properly.
    int64_t virtual_timer_value;
    asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
    return virtual_timer_value;
}
// ----------------------------------------------------------------
#else
// The soft failover to a generic implementation is automatic only for some
// platforms.  For other platforms the developer is expected to make an attempt
// to create a fast implementation and use generic version if nothing better is
// available.
#error You need to define CycleTimer for your O/S and CPU
#endif
