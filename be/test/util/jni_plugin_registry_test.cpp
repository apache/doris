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

#include <set>
#include <string_view>
#include <utility>
#include <vector>

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
        };
    }
};

// The plugin half is a directory name under lib/java/plugins/, so it has to survive being
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

// Two connectors pointed at the same pair would silently run each other's scanner. Scanners
// and writers are looked up in separate maps, so a scanner and a writer of one plugin may
// share a name - and jdbc and max-compute both do - which is why the pairs are only required
// to be unique per kind. The kind is not in the table, so this asserts the weaker rule the
// table can actually see: no pair appears more than twice.
TEST_F(PluginRefTableTest, NoPairIsOverloadedBeyondScannerAndWriter) {
    std::map<std::pair<std::string_view, std::string_view>, int> seen;
    for (const auto& [symbol, ref] : table()) {
        ++seen[{ref.plugin, ref.factory}];
    }
    for (const auto& [pair, count] : seen) {
        EXPECT_LE(count, 2) << pair.first << "/" << pair.second << " is claimed " << count
                            << " times, so at least two of them cannot both be reachable";
    }
}

// Dropping a function must not be the thing that starts a JVM. A BE that runs no Java at all
// still gets DROP FUNCTION forwarded to it, and the registry is the only place left that
// knows whether any plugin was ever loaded - the guard moved here when the class loader that
// used to answer this went away.
TEST(PluginRegistryTest, CleanUdfCacheIsSilentWhenNoPluginWasEverLoaded) {
    EXPECT_TRUE(PluginRegistry::clean_udf_cache("f(INT)").ok());
}

} // namespace doris::Jni
