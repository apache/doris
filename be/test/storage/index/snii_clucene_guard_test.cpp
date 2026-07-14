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

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

namespace {
namespace fs = std::filesystem;

const std::regex kCluceneRefPattern(R"(clucene|lucene::|CL_NS|<CLucene)",
                                    std::regex::ECMAScript | std::regex::icase);

// Locates the SNII core source directory (be/src/storage/index/snii).
std::optional<fs::path> locate_snii_core_dir() {
    std::vector<fs::path> candidates;
#ifdef SNII_CORE_SOURCE_DIR
    // Optional exact path injected by the build system (highest priority). If injected
    // by CMake it must be a quoted string literal, e.g.
    // add_definitions(-DSNII_CORE_SOURCE_DIR="${CMAKE_SOURCE_DIR}/src/storage/index/snii").
    candidates.emplace_back(SNII_CORE_SOURCE_DIR);
#endif

    std::error_code ec;
    fs::path source_path(__FILE__);
    if (source_path.is_relative()) {
        source_path = fs::absolute(source_path, ec);
    }
    // Walk up from this test's source directory; matches either <repo_root>/be/... or
    // <be>/... so it is robust to layout changes without hardcoding the depth.
    for (fs::path dir = source_path.parent_path(); !dir.empty();) {
        candidates.push_back(dir / "be" / "src" / "storage" / "index" / "snii");
        candidates.push_back(dir / "src" / "storage" / "index" / "snii");
        if (dir == dir.root_path()) {
            break;
        }
        dir = dir.parent_path();
    }

    // Fallback: the DORIS_HOME env var (consistent with existing test utilities).
    if (const char* doris_home = std::getenv("DORIS_HOME"); doris_home != nullptr) {
        candidates.push_back(fs::path(doris_home) / "be" / "src" / "storage" / "index" / "snii");
    }

    for (const auto& candidate : candidates) {
        if (fs::is_directory(candidate, ec)) {
            auto canonical = fs::weakly_canonical(candidate, ec);
            return ec ? candidate : canonical;
        }
    }
    return std::nullopt;
}

struct CluceneHit {
    std::string file;
    int line = 0;
    std::string text;
};

// Reads one file and collects matching lines; returns false if it cannot be opened
// (treated as a scan gap).
bool scan_file_for_clucene(const fs::path& file, std::vector<CluceneHit>* hits) {
    std::ifstream input(file);
    if (!input.is_open()) {
        return false;
    }
    std::string line;
    int line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        if (std::regex_search(line, kCluceneRefPattern)) {
            hits->push_back({file.string(), line_no, line});
        }
    }
    return true;
}

} // namespace

// The SNII core engine (the be/src/storage/index/snii module subdirs) must contain
// zero CLucene references; CLucene is allowed only in the top-level Doris integration
// layer (snii_doris_adapter / snii_index_*).
TEST(SniiCluceneDecouplingGuardTest, CoreHasNoCluceneRef) {
    const auto snii_core_dir = locate_snii_core_dir();
    ASSERT_TRUE(snii_core_dir.has_value())
            << "could not locate the SNII core source directory be/src/storage/index/snii. "
            << "Run the test inside the source tree, or set DORIS_HOME / -DSNII_CORE_SOURCE_DIR. "
            << "(__FILE__=" << __FILE__ << ")";

    std::error_code ec;
    fs::recursive_directory_iterator it(*snii_core_dir, ec);
    ASSERT_TRUE(!ec) << "cannot iterate " << snii_core_dir->string() << ": " << ec.message();
    const fs::recursive_directory_iterator end;

    std::vector<CluceneHit> hits;
    std::vector<std::string> unreadable;
    int scanned_files = 0;
    for (; it != end; it.increment(ec)) {
        ASSERT_TRUE(!ec) << "error iterating " << snii_core_dir->string() << ": " << ec.message();
        const fs::directory_entry& entry = *it;
        std::error_code stat_ec;
        if (!entry.is_regular_file(stat_ec) || stat_ec) {
            continue;
        }
        // Guard only the SNII core engine (the module subdirs), which must stay
        // CLucene-free. Skip the top-level Doris integration files (snii_doris_adapter
        // / snii_index_*), which legitimately bridge to CLucene, and non-source files
        // (docs/*.md).
        const std::string ext = entry.path().extension().string();
        if (ext != ".h" && ext != ".cpp" && ext != ".cc") {
            continue;
        }
        if (entry.path().parent_path() == *snii_core_dir) {
            continue;
        }
        ++scanned_files;
        if (!scan_file_for_clucene(entry.path(), &hits)) {
            unreadable.push_back(entry.path().string());
        }
    }

    // Sanity: we must actually scan some files; otherwise a broken locate would make
    // the guard silently pass.
    ASSERT_GT(scanned_files, 0) << "scanned no files under " << snii_core_dir->string()
                                << "; the guard is ineffective.";

    // Unreadable files create scan gaps (possible missed detections); surface them
    // non-fatally.
    EXPECT_TRUE(unreadable.empty())
            << "some files could not be read (possible missed detections): " << unreadable.size()
            << " total, first is " << (unreadable.empty() ? std::string {} : unreadable.front());

    if (!hits.empty()) {
        std::ostringstream oss;
        oss << "the SNII core " << snii_core_dir->string() << " has " << hits.size()
            << " CLucene reference(s) (must be 0; violates the SNII/CLucene decoupling "
               "constraint). CLucene is allowed only in the integration layer:\n";
        for (const auto& hit : hits) {
            oss << "  " << hit.file << ":" << hit.line << ": " << hit.text << "\n";
        }
        FAIL() << oss.str();
    }
}
