// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2006, Google Inc.
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef _SYSINFO_H_
#define _SYSINFO_H_

#include <cstdint>

namespace base {

// Return the number of online CPUs. This is computed and cached the first time this or
// NumCPUs() is called, so does not reflect any CPUs enabled or disabled at a later
// point in time.
//
// Note that, if not all CPUs are online, this may return a value lower than the maximum
// value of sched_getcpu().
extern int NumCPUs();

// Return the maximum CPU index that may be returned by sched_getcpu(). For example, on
// an 8-core machine, this will return '7' even if some of the CPUs have been disabled.
extern int MaxCPUIndex();

void SleepForNanoseconds(int64_t nanoseconds);
void SleepForMilliseconds(int64_t milliseconds);

// processor cycles per second of each processor.  Thread-safe.
extern double CyclesPerSecond(void);

// Parse the maximum CPU index from 'str'. The list is in the format of the CPU lists
// under /sys/devices/system/cpu/, e.g. /sys/devices/system/cpu/present. Returns the
// index of the max CPU or -1 if the string could not be parsed.
// Examples of the format and the expected output include:
// * "0\n" -> 0
// * "0-8\n" -> 8
// * "0-15,32-47\n" -> 47
// * "2,4-127,128-143\n" -> 143
// Ref: https://www.kernel.org/doc/Documentation/cputopology.txt
// Exposed for testing.
extern int ParseMaxCpuIndex(const char* str);

} // namespace base
#endif   /* #ifndef _SYSINFO_H_ */
