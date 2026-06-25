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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/base/phdr_cache.h
// and modified by Doris

#pragma once

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

/** Collects all dl_phdr_info items and caches them in a static array.
  * Doris uses this snapshot only inside the BE stack-trace signal handler. That handler may
  * interrupt a target thread while the dynamic loader lock is already held, so libunwind must not
  * call glibc dl_iterate_phdr from that interrupted context.
  *
  * Normal code paths must keep using the original glibc dl_iterate_phdr. Process-wide use of a
  * snapshot can hide libraries loaded after the last refresh from sanitizers, JVM/Jindo native
  * code, C++ exception handling, and ordinary libunwind callers. Use ScopedPHDRCacheRead only
  * around the minimal signal-handler unwind section.
  *
  * Old cache snapshots are intentionally leaked and remain readable by concurrent signal-handler
  * unwinders.
  *
  * NOTE: It is disabled with Thread Sanitizer because TSan can only use original "dl_iterate_phdr" function.
  */
void updatePHDRCache();

/** Check if a PHDR snapshot is available for ScopedPHDRCacheRead. */
bool hasPHDRCache();

class ScopedPHDRCacheRead {
public:
    ScopedPHDRCacheRead();
    ~ScopedPHDRCacheRead();

    ScopedPHDRCacheRead(const ScopedPHDRCacheRead&) = delete;
    ScopedPHDRCacheRead& operator=(const ScopedPHDRCacheRead&) = delete;

private:
    bool _previous = false;
};
