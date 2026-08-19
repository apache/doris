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

#include "util/jni_plugin_registry.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "util/jvm_launcher.h"

namespace doris::Jni {

// BE names a Java factory by a plugin and a factory name; it used to name a Java class. The
// two look alike in source - both are strings sitting in a reader - and nothing downstream
// tells them apart: a leftover class name is a plugin name that happens to contain slashes,
// and it fails as "Java plugin 'org/apache/doris/paimon/PaimonJniScanner' is not deployed"
// on the first query that reaches that connector, which reads like an operator's mistake
// rather than a missed conversion. The cases below hold the table to the shape a deployment
// directory can actually have.
class PluginRefTableTest : public testing::Test {
protected:
    struct Entry {
        const char* symbol;
        PluginRef ref;
    };

    // Every entry BE addresses. A new connector belongs here as well as in plugin::, or
    // nothing below has an opinion about it.
    static std::vector<Entry> table() {
        return {
                {"PAIMON_SCANNER", plugin::PAIMON_SCANNER},
                {"HUDI_SCANNER", plugin::HUDI_SCANNER},
                {"ICEBERG_SYS_TABLE_SCANNER", plugin::ICEBERG_SYS_TABLE_SCANNER},
                {"MAX_COMPUTE_SCANNER", plugin::MAX_COMPUTE_SCANNER},
                {"MAX_COMPUTE_WRITER", plugin::MAX_COMPUTE_WRITER},
                {"JDBC_SCANNER", plugin::JDBC_SCANNER},
                {"JDBC_WRITER", plugin::JDBC_WRITER},
                {"JDBC_CONNECTION_TESTER", plugin::JDBC_CONNECTION_TESTER},
                {"TRINO_CONNECTOR_SCANNER", plugin::TRINO_CONNECTOR_SCANNER},
                {"JAVA_UDF_SCALAR", plugin::JAVA_UDF_SCALAR},
                {"JAVA_UDF_AGGREGATE", plugin::JAVA_UDF_AGGREGATE},
        };
    }
};

// The plugin half is a directory name under plugins/jni/, so it has to survive being
// one. A Java class name - the thing each of these replaced - does not: it carries the
// package separators that make it a path instead of a name.
TEST_F(PluginRefTableTest, PluginNamesCanBeDirectoryNames) {
    for (const auto& [symbol, ref] : table()) {
        EXPECT_FALSE(ref.plugin.empty()) << symbol;
        EXPECT_EQ(ref.plugin.find('/'), std::string_view::npos)
                << symbol << " still names a Java class rather than a plugin directory";
        EXPECT_EQ(ref.plugin.find('.'), std::string_view::npos)
                << symbol << " still names a Java package rather than a plugin directory";
        EXPECT_EQ(ref.plugin.find(' '), std::string_view::npos) << symbol;
    }
}

// The factory half is looked up in a map keyed by JniScannerFactory#getName(), so the same
// two rules apply for the same reason - it must be spellable in a plugin's source.
TEST_F(PluginRefTableTest, FactoryNamesAreSimpleKeys) {
    for (const auto& [symbol, ref] : table()) {
        EXPECT_FALSE(ref.factory.empty()) << symbol;
        EXPECT_EQ(ref.factory.find('/'), std::string_view::npos)
                << symbol << " still names a Java class rather than a factory";
        EXPECT_EQ(ref.factory.find(' '), std::string_view::npos) << symbol;
    }
}

// A pair is the whole of what BE sends: PluginRegistry receives a plugin name and a factory
// name and nothing that says which kind was wanted, so it keeps one namespace across scanners,
// writers and UDF executors and refuses to load a plugin whose factories collide. Two entries
// here sharing a pair therefore do not mean "one is a scanner and one is a writer" - they mean
// the plugin that implements them fails to load at all, with a message about duplicate factory
// names rather than about this table. A plugin that both reads and writes names the two halves
// for what they do: (jdbc, reader) and (jdbc, writer).
TEST_F(PluginRefTableTest, EveryPairIsClaimedOnce) {
    std::map<std::pair<std::string_view, std::string_view>, const char*> seen;
    for (const auto& [symbol, ref] : table()) {
        auto [existing, inserted] = seen.emplace(std::make_pair(ref.plugin, ref.factory), symbol);
        EXPECT_TRUE(inserted) << symbol << " and " << existing->second << " both claim "
                              << ref.plugin << "/" << ref.factory
                              << ", which the Java plugin loader rejects as a duplicate factory "
                                 "name";
    }
}

// Dropping a function must not be the thing that starts a JVM. A BE that runs no Java at all
// still gets DROP FUNCTION forwarded to it, and the registry is the only place left that
// knows whether any plugin was ever loaded - the guard moved here when the class loader that
// used to answer this went away.
TEST(PluginRegistryTest, CleanUdfCacheIsSilentWhenNoPluginWasEverLoaded) {
    EXPECT_TRUE(PluginRegistry::clean_udf_cache(4242, "f(INT)").ok());
}

// A BE that reads no Java table format runs with no JVM at all. Everything else reaches Java
// only because a query asked it to, so warmup - the one thing that runs unprompted at startup
// - is the only way that property can be lost, and it is lost the moment warmup decides there
// is something to warm when there isn't.
class WarmupTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_dir = config::jni_plugin_dir;
        _root = std::filesystem::temp_directory_path() / "doris_jni_plugin_registry_test";
        std::filesystem::remove_all(_root);
        std::filesystem::create_directories(_root);
        config::jni_plugin_dir = _root.string();
    }

    void TearDown() override {
        config::jni_plugin_dir = _saved_dir;
        std::filesystem::remove_all(_root);
    }

    void touch(const std::string& name) const { std::ofstream {_root / name} << "x"; }

    std::filesystem::path _root;
    std::string _saved_dir;
};

// The ordinary state of a BE nobody deployed a plugin on. Not an error, and not worth a JVM.
TEST_F(WarmupTest, AMissingDirectoryIsNotADeployment) {
    config::jni_plugin_dir = (_root / "never-created").string();
    EXPECT_FALSE(PluginRegistry::any_plugin_deployed());
}

TEST_F(WarmupTest, AnEmptyDirectoryIsNotADeployment) {
    EXPECT_FALSE(PluginRegistry::any_plugin_deployed());
}

// A plugin is a directory. Counting entries instead would let a README, or a jar an operator
// dropped one level too high, start a JVM in order to discover it holds no plugin.
TEST_F(WarmupTest, LooseFilesAreNotPlugins) {
    touch("README.txt");
    touch("paimon-scanner-jar-with-dependencies.jar");
    EXPECT_FALSE(PluginRegistry::any_plugin_deployed());
}

TEST_F(WarmupTest, ADirectoryIsAPlugin) {
    std::filesystem::create_directories(_root / "paimon");
    EXPECT_TRUE(PluginRegistry::any_plugin_deployed());
}

// The check has to happen before anything reaches Java, not inside it: asking the registry
// whether it has plugins is already enough to create the JVM.
//
// The second assertion is the one that carries the property this whole PR rests on. Returning OK
// is not evidence: warmup() returns OK both when it decided there was nothing to do and when it
// reached Java and Java had nothing to report. Only "no JVM exists afterwards" separates the two,
// and it is what "a BE that touches no Java feature pays for no JVM" means.
//
// Reading process-wide state is what makes this case order-dependent, so it skips rather than
// fails once a JVM is there: it has nothing left to say, and a case that cannot tell should not
// be the one reporting. Not that a JVM in doris_be_test would go unnoticed - the process
// segfaults on its way out, ASAN unmapping its shadow while the JVM's threads still run - which
// is the failure that actually needs fixing, at whichever case brought the JVM up.
TEST_F(WarmupTest, WarmupTouchesNoJavaWithNothingDeployed) {
    if (JvmLauncher::vm() != nullptr) {
        GTEST_SKIP() << "some earlier test in this binary already started a JVM, so this one "
                        "cannot tell whether warmup starts one";
    }
    EXPECT_TRUE(PluginRegistry::warmup().ok());
    EXPECT_EQ(nullptr, JvmLauncher::vm()) << "warmup created a JVM with no plugin deployed";
}

} // namespace doris::Jni
