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

#include <gtest/gtest.h>

#include <filesystem>
#include <set>
#include <string>
#include <system_error>

#include "storage/index/snii/writer/temp_dir.h"

// Observing SNII build-time staging files from a test.
//
// WHY THIS IS NOT A ONE-LINE DIRECTORY SCAN. TmpFileDirs::get_tmp_file_dir()
// round-robins over its configured paths with an atomic counter, and every
// resolve_temp_dir() advances it -- so do IndexFileWriter's constructor and
// every StagedBlobFile::create. The doris_be_test binary is monolithic and some
// fixtures install two or three StorePath roots without putting the previous
// TmpFileDirs back, so a test cannot assume there is only one root, nor that two
// scans land on the same one. A before/after count taken through
// resolve_temp_dir() can therefore compare two different directories and report
// "nothing left behind" while a staging file is still linked.
namespace doris::snii_test {

// Every configured SNII temp root. A round-robin returns to its first value
// after exactly one cycle, which is what bounds the probe. Deliberately does not
// install a single-root TmpFileDirs instead: that would pull the temp root out
// from under whichever test runs next.
inline std::set<std::string> snii_temp_roots() {
    std::set<std::string> roots;
    const std::string first = snii::writer::resolve_temp_dir();
    roots.insert(first);
    for (int i = 0; i < 64; ++i) {
        const std::string next = snii::writer::resolve_temp_dir();
        if (next == first) {
            return roots;
        }
        roots.insert(next);
    }
    ADD_FAILURE() << "resolve_temp_dir() did not cycle within 64 calls, so the staged-file "
                     "probe cannot enumerate the temp roots";
    return roots;
}

// Full paths of the staging files named for `tag`, across every root.
// StagedBlobFile::create names them "snii_bkdstage_<tag>_<pid>_<seq>.stage", so
// the tag selects one producer's sub-files and leaves other suites' staging
// alone. Compare the returned SETS, not their sizes: equal counts can still hide
// one file leaking while another is created.
//
// A filesystem error fails the calling test rather than silently reporting an
// empty set, which would make "nothing was left behind" unfalsifiable.
inline std::set<std::string> snii_staged_files(const std::string& tag) {
    const std::string prefix = "snii_bkdstage_" + tag;
    std::set<std::string> found;
    for (const std::string& root : snii_temp_roots()) {
        std::error_code ec;
        std::filesystem::directory_iterator it(root, ec);
        if (ec) {
            ADD_FAILURE() << "cannot scan SNII temp root " << root << ": " << ec.message();
            continue;
        }
        while (it != std::filesystem::directory_iterator()) {
            if (it->path().filename().string().starts_with(prefix)) {
                found.insert(it->path().string());
            }
            it.increment(ec);
            if (ec) {
                ADD_FAILURE() << "cannot walk SNII temp root " << root << ": " << ec.message();
                break;
            }
        }
    }
    return found;
}

} // namespace doris::snii_test
