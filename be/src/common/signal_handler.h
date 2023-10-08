// Copyright (c) 2008, Google Inc.
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
//
// Author: Satoru Takabayashi
//
// Implementation of InstallFailureSignalHandler().

#pragma once

#include <glog/logging.h>
#include <gutil/macros.h>

#include <boost/stacktrace.hpp>
#include <csignal>
#include <ctime>

#include "common/version_internal.h"
#ifdef HAVE_UCONTEXT_H
#include <ucontext.h>
#endif
#ifdef HAVE_SYS_UCONTEXT_H
#include <sys/ucontext.h>
#endif
#include <algorithm>

namespace doris::signal {

inline thread_local uint64 query_id_hi;
inline thread_local uint64 query_id_lo;
inline thread_local int64_t tablet_id = 0;

namespace {

// We'll install the failure signal handler for these signals.  We could
// use strsignal() to get signal names, but we don't use it to avoid
// introducing yet another #ifdef complication.
//
// The list should be synced with the comment in signalhandler.h.
const struct {
    int number;
    const char* name;
} kFailureSignals[] = {
        {SIGSEGV, "SIGSEGV"}, {SIGILL, "SIGILL"}, {SIGFPE, "SIGFPE"},
        {SIGABRT, "SIGABRT"}, {SIGBUS, "SIGBUS"}, {SIGTERM, "SIGTERM"},
};

static bool kFailureSignalHandlerInstalled = false;

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * These signal explainer is copied from Meta's Folly
 */
const char* sigill_reason(int si_code) {
    switch (si_code) {
    case ILL_ILLOPC:
        return "illegal opcode";
    case ILL_ILLOPN:
        return "illegal operand";
    case ILL_ILLADR:
        return "illegal addressing mode";
    case ILL_ILLTRP:
        return "illegal trap";
    case ILL_PRVOPC:
        return "privileged opcode";
    case ILL_PRVREG:
        return "privileged register";
    case ILL_COPROC:
        return "coprocessor error";
    case ILL_BADSTK:
        return "internal stack error";

    default:
        return nullptr;
    }
}

const char* sigfpe_reason(int si_code) {
    switch (si_code) {
    case FPE_INTDIV:
        return "integer divide by zero";
    case FPE_INTOVF:
        return "integer overflow";
    case FPE_FLTDIV:
        return "floating-point divide by zero";
    case FPE_FLTOVF:
        return "floating-point overflow";
    case FPE_FLTUND:
        return "floating-point underflow";
    case FPE_FLTRES:
        return "floating-point inexact result";
    case FPE_FLTINV:
        return "floating-point invalid operation";
    case FPE_FLTSUB:
        return "subscript out of range";

    default:
        return nullptr;
    }
}

const char* sigsegv_reason(int si_code) {
    switch (si_code) {
    case SEGV_MAPERR:
        return "address not mapped to object";
    case SEGV_ACCERR:
        return "invalid permissions for mapped object";

    default:
        return nullptr;
    }
}

const char* sigbus_reason(int si_code) {
    switch (si_code) {
    case BUS_ADRALN:
        return "invalid address alignment";
    case BUS_ADRERR:
        return "nonexistent physical address";
    case BUS_OBJERR:
        return "object-specific hardware error";

        // MCEERR_AR and MCEERR_AO: in sigaction(2) but not in headers.

    default:
        return nullptr;
    }
}

const char* signal_reason(int signum, int si_code) {
    switch (signum) {
    case SIGILL:
        return sigill_reason(si_code);
    case SIGFPE:
        return sigfpe_reason(si_code);
    case SIGSEGV:
        return sigsegv_reason(si_code);
    case SIGBUS:
        return sigbus_reason(si_code);
    default:
        return nullptr;
    }
}

// The class is used for formatting error messages.  We don't use printf()
// as it's not async signal safe.
class MinimalFormatter {
public:
    MinimalFormatter(char* buffer, size_t size)
            : buffer_(buffer), cursor_(buffer), end_(buffer + size) {}

    // Returns the number of bytes written in the buffer.
    std::size_t num_bytes_written() const { return static_cast<std::size_t>(cursor_ - buffer_); }

    // Appends string from "str" and updates the internal cursor.
    void AppendString(const char* str) {
        ptrdiff_t i = 0;
        while (str[i] != '\0' && cursor_ + i < end_) {
            cursor_[i] = str[i];
            ++i;
        }
        cursor_ += i;
    }

    // Formats "number" in "radix" and updates the internal cursor.
    // Lowercase letters are used for 'a' - 'z'.
    void AppendUint64(uint64 number, unsigned radix) {
        unsigned i = 0;
        while (cursor_ + i < end_) {
            const uint64 tmp = number % radix;
            number /= radix;
            cursor_[i] = static_cast<char>(tmp < 10 ? '0' + tmp : 'a' + tmp - 10);
            ++i;
            if (number == 0) {
                break;
            }
        }
        // Reverse the bytes written.
        std::reverse(cursor_, cursor_ + i);
        cursor_ += i;
    }

private:
    char* buffer_;
    char* cursor_;
    const char* const end_;
};

// Writes the given data with the size to the standard error.
void WriteToStderr(const char* data, size_t size) {
    if (write(STDERR_FILENO, data, size) < 0) {
        // Ignore errors.
    }
}

// The writer function can be changed by InstallFailureWriter().
void (*g_failure_writer)(const char* data, size_t size) = WriteToStderr;

// Dumps time information.  We don't dump human-readable time information
// as localtime() is not guaranteed to be async signal safe.
void DumpTimeInfo() {
    time_t time_in_sec = time(nullptr);
    char buf[256]; // Big enough for time info.
    MinimalFormatter formatter(buf, sizeof(buf));
    formatter.AppendString("*** Query id: ");
    formatter.AppendUint64(query_id_hi, 16);
    formatter.AppendString("-");
    formatter.AppendUint64(query_id_lo, 16);
    formatter.AppendString(" ***\n");
    formatter.AppendString("*** tablet id: ");
    formatter.AppendUint64(tablet_id, 10);
    formatter.AppendString(" ***\n");
    formatter.AppendString("*** Aborted at ");
    formatter.AppendUint64(static_cast<uint64>(time_in_sec), 10);
    formatter.AppendString(" (unix time)");
    formatter.AppendString(" try \"date -d @");
    formatter.AppendUint64(static_cast<uint64>(time_in_sec), 10);
    formatter.AppendString("\" if you are using GNU date ***\n");
    formatter.AppendString("*** Current BE git commitID: ");
    formatter.AppendString(version::doris_build_short_hash());
    formatter.AppendString(" ***\n");
    g_failure_writer(buf, formatter.num_bytes_written());
}

// Dumps information about the signal to STDERR.
void DumpSignalInfo(int signal_number, siginfo_t* siginfo) {
    // Get the signal name.
    const char* signal_name = nullptr;
    for (size_t i = 0; i < ARRAYSIZE_UNSAFE(kFailureSignals); ++i) {
        if (signal_number == kFailureSignals[i].number) {
            signal_name = kFailureSignals[i].name;
        }
    }

    char buf[256]; // Big enough for signal info.
    MinimalFormatter formatter(buf, sizeof(buf));

    formatter.AppendString("*** ");
    if (signal_name) {
        formatter.AppendString(signal_name);
    } else {
        // Use the signal number if the name is unknown.  The signal name
        // should be known, but just in case.
        formatter.AppendString("Signal ");
        formatter.AppendUint64(static_cast<uint64>(signal_number), 10);
    }
    formatter.AppendString(" ");
    // Detail reason explain
    auto reason = signal_reason(signal_number, siginfo->si_code);

    // If we can't find a reason code make a best effort to print the (int) code.
    if (reason != nullptr) {
        formatter.AppendString(reason);
    } else {
        formatter.AppendString("unknown detail explain");
    }
    formatter.AppendString(" (@0x");
    formatter.AppendUint64(reinterpret_cast<uintptr_t>(siginfo->si_addr), 16);
    formatter.AppendString(")");
    formatter.AppendString(" received by PID ");
    formatter.AppendUint64(static_cast<uint64>(getpid()), 10);
    formatter.AppendString(" (TID ");
    uint64_t tid;
#ifdef __APPLE__
    pthread_threadid_np(nullptr, &tid);
#else
    tid = syscall(SYS_gettid);
#endif
    formatter.AppendUint64(tid, 10);
    formatter.AppendString(" OR 0x");
    // We assume pthread_t is an integral number or a pointer, rather
    // than a complex struct.  In some environments, pthread_self()
    // returns an uint64 but in some other environments pthread_self()
    // returns a pointer.
    pthread_t id = pthread_self();
    formatter.AppendUint64(reinterpret_cast<uint64>(reinterpret_cast<const char*>(id)), 16);
    formatter.AppendString(") ");
    // Only linux has the PID of the signal sender in si_pid.
    formatter.AppendString("from PID ");
    formatter.AppendUint64(static_cast<uint64>(siginfo->si_pid), 10);
    formatter.AppendString("; ");
    formatter.AppendString("stack trace: ***\n");
    g_failure_writer(buf, formatter.num_bytes_written());
}

// Invoke the default signal handler.
void InvokeDefaultSignalHandler(int signal_number) {
    struct sigaction sig_action;
    memset(&sig_action, 0, sizeof(sig_action));
    sigemptyset(&sig_action.sa_mask);
    sig_action.sa_handler = SIG_DFL;
    sigaction(signal_number, &sig_action, nullptr);
    kill(getpid(), signal_number);
}

// This variable is used for protecting FailureSignalHandler() from
// dumping stuff while another thread is doing it.  Our policy is to let
// the first thread dump stuff and let other threads wait.
// See also comments in FailureSignalHandler().
static pthread_t* g_entered_thread_id_pointer = nullptr;

// Wrapper of __sync_val_compare_and_swap. If the GCC extension isn't
// defined, we try the CPU specific logics (we only support x86 and
// x86_64 for now) first, then use a naive implementation, which has a
// race condition.
template <typename T>
T sync_val_compare_and_swap(T* ptr, T oldval, T newval) {
#if defined(HAVE___SYNC_VAL_COMPARE_AND_SWAP)
    return __sync_val_compare_and_swap(ptr, oldval, newval);
#elif defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))
    T ret;
    __asm__ __volatile__("lock; cmpxchg %1, (%2);"
                         : "=a"(ret)
                         // GCC may produces %sil or %dil for
                         // constraint "r", but some of apple's gas
                         // dosn't know the 8 bit registers.
                         // We use "q" to avoid these registers.
                         : "q"(newval), "q"(ptr), "a"(oldval)
                         : "memory", "cc");
    return ret;
#else
    T ret = *ptr;
    if (ret == oldval) {
        *ptr = newval;
    }
    return ret;
#endif
}

// Dumps signal and stack frame information, and invokes the default
// signal handler once our job is done.
void FailureSignalHandler(int signal_number, siginfo_t* signal_info, void* ucontext) {
    // First check if we've already entered the function.  We use an atomic
    // compare and swap operation for platforms that support it.  For other
    // platforms, we use a naive method that could lead to a subtle race.

    // We assume pthread_self() is async signal safe, though it's not
    // officially guaranteed.
    pthread_t my_thread_id = pthread_self();
    // NOTE: We could simply use pthread_t rather than pthread_t* for this,
    // if pthread_self() is guaranteed to return non-zero value for thread
    // ids, but there is no such guarantee.  We need to distinguish if the
    // old value (value returned from __sync_val_compare_and_swap) is
    // different from the original value (in this case NULL).
    pthread_t* old_thread_id_pointer = sync_val_compare_and_swap(
            &g_entered_thread_id_pointer, static_cast<pthread_t*>(nullptr), &my_thread_id);
    if (old_thread_id_pointer != nullptr) {
        // We've already entered the signal handler.  What should we do?
        if (pthread_equal(my_thread_id, *g_entered_thread_id_pointer)) {
            // It looks the current thread is reentering the signal handler.
            // Something must be going wrong (maybe we are reentering by another
            // type of signal?).  Kill ourself by the default signal handler.
            InvokeDefaultSignalHandler(signal_number);
        }
        // Another thread is dumping stuff.  Let's wait until that thread
        // finishes the job and kills the process.
        while (true) {
            sleep(1);
        }
    }
    // This is the first time we enter the signal handler.  We are going to
    // do some interesting stuff from here.
    // TODO(satorux): We might want to set timeout here using alarm(), but
    // mixing alarm() and sleep() can be a bad idea.

    // First dump time info.
    DumpTimeInfo();
    DumpSignalInfo(signal_number, signal_info);

    // *** TRANSITION ***
    //
    // BEFORE this point, all code must be async-termination-safe!
    // (See WARNING above.)
    //
    // AFTER this point, we do unsafe things, like using LOG()!
    // The process could be terminated or hung at any time.  We try to
    // do more useful things first and riskier things later.

    // Use boost stacktrace to print more detail info
    std::cout << boost::stacktrace::stacktrace() << std::endl;

    // Flush the logs before we do anything in case 'anything'
    // causes problems.
    google::FlushLogFilesUnsafe(0);

    // Kill ourself by the default signal handler.
    InvokeDefaultSignalHandler(signal_number);
}

} // namespace

inline void set_signal_task_id(PUniqueId tid) {
    query_id_hi = tid.hi();
    query_id_lo = tid.lo();
}

inline void set_signal_task_id(TUniqueId tid) {
    query_id_hi = tid.hi;
    query_id_lo = tid.lo;
}

inline void InstallFailureSignalHandler() {
    // Build the sigaction struct.
    struct sigaction sig_action;
    memset(&sig_action, 0, sizeof(sig_action));
    sigemptyset(&sig_action.sa_mask);
    sig_action.sa_flags |= SA_SIGINFO;
    sig_action.sa_sigaction = &FailureSignalHandler;

    for (size_t i = 0; i < ARRAYSIZE_UNSAFE(kFailureSignals); ++i) {
        CHECK_ERR(sigaction(kFailureSignals[i].number, &sig_action, nullptr));
    }
    kFailureSignalHandlerInstalled = true;
}

} // namespace doris::signal
