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
#include <sstream>
#include <string>

#include "agent/utils.h"
#include "common/config.h"
#include "runtime/memory/heap_profiler.h"
#include "util/pprof_utils.h"

namespace doris {

// Scripts check the shell boundary. The jeprof integration test uses the bundled tool.
class ProfilerCommandTest : public testing::Test {
protected:
    void SetUp() override {
        _old_home = getenv("DORIS_HOME");
        _old_path = getenv("PATH");
        _old_profile_dir = config::pprof_profile_dir;
        std::string pattern =
                (std::filesystem::current_path() / "profiler_command.XXXXXX").string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        _root = pattern;
        _home = _root / "tools 'one''two' $(printf injected)";
        _profile_dir = _root / "profiles 'one''two' $(printf injected);#";
        std::filesystem::create_directories(_profile_dir);
        config::pprof_profile_dir = _profile_dir.string();
        ASSERT_EQ(setenv("DORIS_HOME", _home.c_str(), 1), 0);
        ASSERT_EQ(setenv("PATH", (_home.string() + "/bin:" + _old_path).c_str(), 1), 0);
        write_tool(_home / "bin/jeprof", "printf '%s\\n' \"$@\"\n");
        write_tool(_home / "tools/bin/pprof",
                   "if [ \"$1\" = --version ]; then exit 0; fi\nprintf '%s\\n' \"$@\"\n");
        write_tool(_home / "bin/perf", R"(case "$1" in
--version) exit 0 ;;
record)
    while [ "$#" -gt 0 ]; do
        if [ "$1" = -o ]; then
            shift
            printf 'sample\n' > "$1"
            exit
        fi
        shift
    done
    exit 1 ;;
script) cat -- "$3" ;;
esac
)");
        write_tool(_home / "FlameGraph/stackcollapse-perf.pl", "cat\n");
        write_tool(_home / "FlameGraph/flamegraph.pl", "cat\n");
    }

    void TearDown() override {
        EXPECT_EQ(setenv("DORIS_HOME", _old_home.c_str(), 1), 0);
        EXPECT_EQ(setenv("PATH", _old_path.c_str(), 1), 0);
        config::pprof_profile_dir = _old_profile_dir;
        std::filesystem::remove_all(_root);
    }

    static void write_tool(const std::filesystem::path& path, const std::string& body) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream file(path);
        file << "#!/bin/sh\n" << body;
        file.close();
        ASSERT_TRUE(file.good());
        std::filesystem::permissions(path, std::filesystem::perms::owner_all);
    }

    std::filesystem::path _root;
    std::filesystem::path _home;
    std::filesystem::path _profile_dir;
    std::string _old_home;
    std::string _old_path;
    std::string _old_profile_dir;
};

TEST_F(ProfilerCommandTest, JeprofKeepsPathsLiteral) {
    const std::string profile = (_profile_dir / "jeheap_dump.heap").string();
    EXPECT_EQ("--dot\n--\n" + (_home / "lib/doris_be").string() + "\n" + profile + "\n",
              HeapProfiler::jeprof_profile_to_dot(profile));
}

TEST_F(ProfilerCommandTest, BundledJeprofKeepsLocalProfile) {
    const auto jeprof = std::filesystem::current_path() / "tools/jeprof";
    // Keep failure output in this fixture. The curl stub prevents remote requests.
    write_tool(_home / "bin/jeprof",
               "export JEPROF_TMPDIR=" + AgentUtils::shell_quote(_root.string()) + "\nexec " +
                       AgentUtils::shell_quote(jeprof.string()) + " \"$@\"\n");
    write_tool(_home / "bin/curl", "echo 'Unexpected remote profile' >&2\nexit 1\n");
    std::filesystem::create_directories(_home / "lib");
    std::filesystem::create_symlink("/bin/true", _home / "lib/doris_be");
    const auto profile_dir = _profile_dir / "safe@1";
    std::filesystem::create_directories(profile_dir);
    const std::string profile = (profile_dir / "jeheap_dump.heap").string();
    // Two lines describe a valid profile with no samples. No process sampling is needed.
    std::ofstream(profile) << "heap_v2/1\n  t*: 0: 0 [0: 0]\n";
    EXPECT_EQ("Using local file " + (_home / "lib/doris_be").string() + ".\nUsing local file " +
                      profile + ".\nNo nodes to print\n",
              HeapProfiler::jeprof_profile_to_dot(profile));
}

TEST_F(ProfilerCommandTest, BundledJeprofUnitTests) {
    const auto jeprof = std::filesystem::current_path() / "tools/jeprof";
    std::string output;
    AgentUtils util;
    ASSERT_TRUE(util.exec_cmd(AgentUtils::shell_quote(jeprof.string()) + " --test", &output))
            << output;
    EXPECT_NE(output.find("ShellEscape round trip passed\n"), std::string::npos);
}

TEST_F(ProfilerCommandTest, PprofKeepsFilePathLiteral) {
    const std::string profile = (_profile_dir / "cpu_profile").string();
    std::ofstream(profile) << "sample";
    std::string binary;
    ASSERT_TRUE(PprofUtils::get_self_cmdline(&binary).ok());
    std::stringstream output;
    const auto status = PprofUtils::get_readable_profile(profile, true, &output);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ("Profile(Sample 30 seconds)\n--text\n--\n" + binary + "\n" + profile + "\n\n",
              output.str());
    EXPECT_FALSE(std::filesystem::exists(profile));
}

TEST_F(ProfilerCommandTest, PprofKeepsGeneratedPathLiteral) {
    std::stringstream output;
    const auto status = PprofUtils::get_readable_profile("sample", false, &output);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_NE(output.str().find(_profile_dir.string() + "/pprof_profile."), std::string::npos);
    EXPECT_TRUE(std::filesystem::is_empty(_profile_dir));
}

TEST_F(ProfilerCommandTest, FlamegraphKeepsPathsLiteral) {
    // Both response modes use the same profile path. File mode also tests shell redirection.
    for (bool return_file : {false, true}) {
        std::string result;
        const auto status = PprofUtils::generate_flamegraph(0, (_home / "FlameGraph").string(),
                                                            return_file, &result);
        ASSERT_TRUE(status.ok()) << status;
        if (return_file) {
            EXPECT_EQ(std::filesystem::path(result).parent_path(), _profile_dir);
            std::ifstream file(result);
            std::stringstream content;
            content << file.rdbuf();
            EXPECT_EQ("sample\n", content.str());
        } else {
            EXPECT_EQ("sample\n", result);
        }
    }
}

} // namespace doris
