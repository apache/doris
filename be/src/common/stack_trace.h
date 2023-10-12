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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/StackTrace.h
// and modified by Doris

#pragma once

#include <array>
#include <csignal>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#ifdef __clang__
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#endif
#define _XOPEN_SOURCE 700
#endif
#include <ucontext.h>

struct NoCapture {};

/// Tries to capture current stack trace using libunwind or signal context
/// NOTE: StackTrace calculation is signal safe only if updatePHDRCache() was called beforehand.
class StackTrace {
public:
    struct Frame {
        const void* virtual_addr = nullptr;
        void* physical_addr = nullptr;
        std::optional<std::string> symbol;
        std::optional<std::string> object;
        std::optional<std::string> file;
        std::optional<uint64_t> line;
    };

    /* NOTE: It cannot be larger right now, since otherwise it
     * will not fit into minimal PIPE_BUF (512) in TraceCollector.
     */
    static constexpr size_t capacity = 45;

    using FramePointers = std::array<void*, capacity>;
    using Frames = std::array<Frame, capacity>;

    /// Tries to capture stack trace
    inline StackTrace() { tryCapture(); }

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
    explicit StackTrace(const ucontext_t& signal_context);

    /// Creates empty object for deferred initialization
    explicit inline StackTrace(NoCapture) {}

    [[nodiscard]] constexpr size_t getSize() const { return size; }
    [[nodiscard]] constexpr size_t getOffset() const { return offset; }
    [[nodiscard]] const FramePointers& getFramePointers() const { return frame_pointers; }
    [[nodiscard]] std::string toString(int start_pointers_index = 0) const;

    static std::string toString(void** frame_pointers, size_t offset, size_t size);
    static void dropCache();
    static void symbolize(const FramePointers& frame_pointers, size_t offset, size_t size,
                          StackTrace::Frames& frames);

    void toStringEveryLine(std::function<void(std::string_view)> callback) const;

    /// Displaying the addresses can be disabled for security reasons.
    /// If you turn off addresses, it will be more secure, but we will be unable to help you with debugging.
    /// Please note: addresses are also available in the system.stack_trace and system.trace_log tables.
    static void setShowAddresses(bool show);

protected:
    void tryCapture();

    size_t size = 0;
    size_t offset = 0; /// How many frames to skip while displaying.
    FramePointers frame_pointers {};
};

std::string signalToErrorMessage(int sig, const siginfo_t& info, const ucontext_t& context);
