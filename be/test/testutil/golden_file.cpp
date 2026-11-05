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

#include "testutil/golden_file.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

namespace doris::test {

namespace {

std::string env(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

std::string key_of(const std::string& name, const std::string& part) {
    return name + " " + part;
}

} // namespace

GoldenFile::GoldenFile(std::string path) : _path(std::move(path)) {
    std::ifstream in(_path);
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        std::istringstream fields(line);
        std::string name;
        std::string part;
        Frozen frozen;
        fields >> name >> part >> frozen.count >> std::hex >> frozen.digest;
        EXPECT_FALSE(fields.fail()) << "malformed line in " << _path << ": " << line;
        EXPECT_TRUE(_frozen.emplace(key_of(name, part), frozen).second)
                << "duplicate line in " << _path << ": " << line;
    }
}

void GoldenFile::check(const std::string& name, const std::string& part, uint64_t count,
                       uint64_t digest, const std::string& context) {
    EXPECT_EQ(name.find_first_of(" \t\n"), std::string::npos) << name;
    EXPECT_EQ(part.find_first_of(" \t\n"), std::string::npos) << part;
    const std::string key = key_of(name, part);
    EXPECT_TRUE(_checked.insert(key).second) << key << " is checked twice";
    _computed.push_back(fmt::format("{} {} {:016x}", key, count, digest));
    const auto it = _frozen.find(key);
    if (it == _frozen.end()) {
        _missing.push_back(_computed.back());
        return;
    }
    EXPECT_TRUE(it->second.count == count && it->second.digest == digest)
            << "the output of " << key << " changed: frozen "
            << fmt::format("{} {:016x}", it->second.count, it->second.digest) << ", now "
            << fmt::format("{} {:016x}", count, digest) << "\n"
            << context;
}

void GoldenFile::finish(const std::function<bool(const std::string& name)>& owns, bool complete) {
    if (complete) {
        for (const auto& [key, frozen] : _frozen) {
            EXPECT_TRUE(!owns(key.substr(0, key.find(' '))) || _checked.contains(key))
                    << _path << " has " << key
                    << ", which nothing computes any more. Lines are never removed: keep what "
                       "produced it.";
        }
    }
    if (const std::string write_path = env("ANALYSIS_GOLDEN_WRITE"); !write_path.empty()) {
        // Appends, so one run of several golden tests collects all of their lines.
        std::ofstream out(write_path, std::ios::app);
        for (const std::string& line : _computed) {
            out << line << "\n";
        }
    }
    if (_missing.empty()) {
        return;
    }
    // The value names the commit of the code the digests come from.
    const std::string commit = env("ANALYSIS_GOLDEN_REGEN");
    if (commit.empty() || !complete) {
        ADD_FAILURE() << _missing.size() << " digests are not in " << _path
                      << ", the first is: " << _missing.front()
                      << ". Run the whole test with ANALYSIS_GOLDEN_REGEN=<commit> to append "
                         "them.";
        return;
    }
    const bool exists = std::filesystem::exists(_path);
    std::ofstream out(_path, std::ios::app);
    ASSERT_TRUE(out.good()) << "cannot write " << _path;
    if (!exists) {
        out << "# Licensed to the Apache Software Foundation (ASF) under one\n"
               "# or more contributor license agreements.  See the NOTICE file\n"
               "# distributed with this work for additional information\n"
               "# regarding copyright ownership.  The ASF licenses this file\n"
               "# to you under the Apache License, Version 2.0 (the\n"
               "# \"License\"); you may not use this file except in compliance\n"
               "# with the License.  You may obtain a copy of the License at\n"
               "#\n"
               "#   http://www.apache.org/licenses/LICENSE-2.0\n"
               "#\n"
               "# Unless required by applicable law or agreed to in writing,\n"
               "# software distributed under the License is distributed on an\n"
               "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
               "# KIND, either express or implied.  See the License for the\n"
               "# specific language governing permissions and limitations\n"
               "# under the License.\n"
               "\n"
               "# Frozen digests. Lines are added, never changed or removed: a changed line is a\n"
               "# changed output. See the test that checks this file.\n"
               "# <name> <part> <count> <digest>\n";
    }
    out << "# added at " << commit << "\n";
    for (const std::string& line : _missing) {
        out << line << "\n";
    }
}

} // namespace doris::test
