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

#ifndef DORIS_BE_SRC_UTIL_SCOPED_CLEANUP_H
#define DORIS_BE_SRC_UTIL_SCOPED_CLEANUP_H

#include <utility>

#include "gutil/macros.h"

// Run the given function body (which is typically a block of code surrounded by
// curly-braces) when the current scope exits.
//
// Example:
//   int fd = open(...);
//   SCOPED_CLEANUP({ close(fd); });
//
// NOTE: in the case that you want to cancel the cleanup, use the more verbose
// (non-macro) form below.
#define SCOPED_CLEANUP(func_body) \
    auto VARNAME_LINENUM(scoped_cleanup) = MakeScopedCleanup([&] { func_body });

namespace doris {

// A scoped object which runs a cleanup function when going out of scope. Can
// be used for scoped resource cleanup.
//
// Use 'MakeScopedCleanup()' below to instantiate.
template <typename F>
class ScopedCleanup {
public:
    explicit ScopedCleanup(F f) : cancelled_(false), f_(std::move(f)) {}
    ~ScopedCleanup() {
        if (!cancelled_) {
            f_();
        }
    }
    void cancel() { cancelled_ = true; }

private:
    bool cancelled_;
    F f_;
};

// Creates a new scoped cleanup instance with the provided function.
template <typename F>
ScopedCleanup<F> MakeScopedCleanup(F f) {
    return ScopedCleanup<F>(f);
}

} // namespace doris

#endif //DORIS_BE_SRC_UTIL_SCOPED_CLEANUP_H
