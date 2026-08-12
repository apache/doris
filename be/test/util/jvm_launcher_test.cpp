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

#include "util/jvm_launcher.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/config.h"

namespace doris::Jni {

// The BE creates the JVM that libhdfs used to create. These options are the whole of what
// tells the two apart, so each case below pins down one way they could drift.
class JvmLauncherOptionsTest : public testing::Test {
protected:
    void SetUp() override {
        for (const char* name : {"CLASSPATH", "DORIS_CLASSPATH", "JAVA_OPTS", "LIBHDFS_OPTS",
                                 "DORIS_HOME", "HADOOP_CONF_DIR"}) {
            if (const char* value = getenv(name); value != nullptr) {
                _saved.emplace_back(name, value);
            }
            unsetenv(name);
        }
    }

    void TearDown() override {
        for (const auto& [name, value] : _saved) {
            setenv(name.c_str(), value.c_str(), 1);
        }
    }

    static std::vector<std::string> options() { return JvmLauncher::_build_options(); }

    static bool has(const std::vector<std::string>& options, const std::string& option) {
        return std::find(options.begin(), options.end(), option) != options.end();
    }

    static int count_prefixed(const std::vector<std::string>& options, const std::string& prefix) {
        return static_cast<int>(std::count_if(
                options.begin(), options.end(),
                [&](const std::string& option) { return option.starts_with(prefix); }));
    }

private:
    std::vector<std::pair<std::string, std::string>> _saved;
};

// start_be.sh publishes the class path under two names and only CLASSPATH carries conf/ -
// where the hadoop *-site.xml files live - and plugins/java_extensions. Reading the other
// one builds a JVM that cannot see its own configuration.
TEST_F(JvmLauncherOptionsTest, ClassPathComesFromTheListThatCarriesConf) {
    setenv("CLASSPATH", "/opt/be/conf/:/opt/be/lib/preload.jar", 1);
    setenv("DORIS_CLASSPATH", "-Djava.class.path=/opt/be/lib/preload.jar", 1);

    EXPECT_TRUE(has(options(), "-Djava.class.path=/opt/be/conf/:/opt/be/lib/preload.jar"));
    EXPECT_EQ(1, count_prefixed(options(), "-Djava.class.path="));
}

// DORIS_CLASSPATH is exported as a whole JVM option rather than a bare path.
TEST_F(JvmLauncherOptionsTest, DorisClassPathIsNotWrappedTwice) {
    setenv("DORIS_CLASSPATH", "-Djava.class.path=/opt/be/lib/preload.jar", 1);

    EXPECT_TRUE(has(options(), "-Djava.class.path=/opt/be/lib/preload.jar"));
    EXPECT_EQ(1, count_prefixed(options(), "-Djava.class.path="));
}

// Both of these used to reach the JVM only because libhdfs read LIBHDFS_OPTS out of the
// environment. The BE passes them itself now, and it has to do so whether or not the
// deployment supplied any options of its own.
TEST_F(JvmLauncherOptionsTest, KerberosAndProcessReaperAreAlwaysPassed) {
    setenv("JAVA_OPTS", "-Xmx4096m --add-opens=java.base/java.lang=ALL-UNNAMED", 1);

    EXPECT_EQ(1, count_prefixed(options(), "-Djava.security.krb5.conf="));
    EXPECT_EQ(1, count_prefixed(options(), "-Djdk.lang.processReaperUseDefaultStackSize="));
    // ... without dropping what the deployment asked for.
    EXPECT_TRUE(has(options(), "-Xmx4096m"));
    EXPECT_TRUE(has(options(), "--add-opens=java.base/java.lang=ALL-UNNAMED"));
}

TEST_F(JvmLauncherOptionsTest, KerberosAndProcessReaperAreAlsoPassedWithoutJavaOpts) {
    setenv("DORIS_HOME", "/opt/be", 1);

    EXPECT_EQ(1, count_prefixed(options(), "-Djava.security.krb5.conf="));
    EXPECT_EQ(1, count_prefixed(options(), "-Djdk.lang.processReaperUseDefaultStackSize="));
    EXPECT_TRUE(has(options(), "-Xmx1g"));
}

// LIBHDFS_OPTS is what the JVM of a running BE was built from; a deployment that sets only
// that one must not silently lose its heap size and its --add-opens.
TEST_F(JvmLauncherOptionsTest, LibhdfsOptsAreUsedWhenJavaOptsIsUnset) {
    setenv("LIBHDFS_OPTS", "-Xmx2g -Dhadoop.shell.setsid.enabled=false", 1);

    EXPECT_TRUE(has(options(), "-Xmx2g"));
    EXPECT_TRUE(has(options(), "-Dhadoop.shell.setsid.enabled=false"));
    EXPECT_FALSE(has(options(), "-Xmx1g"));
}

TEST_F(JvmLauncherOptionsTest, JavaOptsWinsOverLibhdfsOpts) {
    setenv("JAVA_OPTS", "-Xmx4096m", 1);
    setenv("LIBHDFS_OPTS", "-Xmx2g", 1);

    EXPECT_TRUE(has(options(), "-Xmx4096m"));
    EXPECT_FALSE(has(options(), "-Xmx2g"));
}

// PluginRegistry reads the plugin directory from a system property, so the property has to be
// on the options the JVM is created from or the registry silently looks somewhere else. It is
// derived from BE config here rather than exported by the startup script for the same reason
// the class path is now built once: a path with two sources is a path that disagrees with
// itself.
TEST_F(JvmLauncherOptionsTest, PluginDirectoryReachesTheJvmFromBeConfig) {
    const std::string saved = config::java_plugin_dir;
    config::java_plugin_dir = "/opt/be/lib/java/plugins";

    EXPECT_TRUE(has(options(), "-Ddoris.jni.plugin.dir=/opt/be/lib/java/plugins"));
    EXPECT_EQ(1, count_prefixed(options(), "-Ddoris.jni.plugin.dir="));

    config::java_plugin_dir = saved;
}

} // namespace doris::Jni
