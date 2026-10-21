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

#pragma once

#include <sys/statvfs.h>

#include <cstdint>
#include <string>

namespace doris::snii::writer {

// Scratch directory for spill runs and section temp files. Uses Doris's configured
// tmp_file_dirs (ExecEnv::get_tmp_file_dirs) in production; when those are not
// initialized (unit tests / standalone) it falls back to SNII_TEMP_DIR -> TMPDIR ->
// /tmp. Defined in temp_dir.cpp to keep exec_env.h out of this header.
//
// The fallback (SNII_TEMP_DIR / TMPDIR) should point at a REAL disk (SSD/NVMe):
// /tmp is often tmpfs (RAM-backed), where spilling does NOT reduce RSS.
std::string resolve_temp_dir();

// Best-effort free bytes on the filesystem backing `dir`. Returns UINT64_MAX when
// statvfs fails, so a caller's space pre-check never false-positives on an
// unstattable path. CAVEATS: this is best-effort only -- it is subject to TOCTOU
// (free space can drop before/while the write runs), and on tmpfs it reports
// RAM-backed space (use the temp-dir config to avoid tmpfs in the first place).
inline uint64_t temp_dir_available_bytes(const std::string& dir) {
    struct statvfs vfs;
    if (::statvfs(dir.c_str(), &vfs) != 0) return UINT64_MAX;
    return static_cast<uint64_t>(vfs.f_bavail) * static_cast<uint64_t>(vfs.f_frsize);
}

} // namespace doris::snii::writer
