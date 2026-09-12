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
#include <csignal>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/config.h"
#include "util/defer_op.h"

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

// start_be.sh publishes the class path under two names and only CLASSPATH carries conf/,
// where the hadoop *-site.xml files live. Reading the other one builds a JVM that cannot
// see its own configuration.
TEST_F(JvmLauncherOptionsTest, ClassPathComesFromTheListThatCarriesConf) {
    setenv("CLASSPATH", "/opt/be/conf/:/opt/be/lib/plugin-spi.jar", 1);
    setenv("DORIS_CLASSPATH", "-Djava.class.path=/opt/be/lib/plugin-spi.jar", 1);

    EXPECT_TRUE(has(options(), "-Djava.class.path=/opt/be/conf/:/opt/be/lib/plugin-spi.jar"));
    EXPECT_EQ(1, count_prefixed(options(), "-Djava.class.path="));
}

// DORIS_CLASSPATH is exported as a whole JVM option rather than a bare path.
TEST_F(JvmLauncherOptionsTest, DorisClassPathIsNotWrappedTwice) {
    setenv("DORIS_CLASSPATH", "-Djava.class.path=/opt/be/lib/plugin-spi.jar", 1);

    EXPECT_TRUE(has(options(), "-Djava.class.path=/opt/be/lib/plugin-spi.jar"));
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
    const std::string saved = config::jni_plugin_dir;
    config::jni_plugin_dir = "/opt/be/plugins/jni";

    EXPECT_TRUE(has(options(), "-Ddoris.jni.plugin.dir=/opt/be/plugins/jni"));
    EXPECT_EQ(1, count_prefixed(options(), "-Ddoris.jni.plugin.dir="));

    config::jni_plugin_dir = saved;
}

// Same again for the directory a plugin reads hadoop's *-site.xml from. A plugin cannot see
// BE's classpath, so this property is the only way one of them finds a core-site.xml at all.
TEST_F(JvmLauncherOptionsTest, HadoopConfDirectoryReachesTheJvmFromBeConfig) {
    const std::string saved = config::jni_plugin_hadoop_conf_dir;
    config::jni_plugin_hadoop_conf_dir = "/opt/be/plugins/hadoop_conf";

    EXPECT_TRUE(has(options(), "-Ddoris.jni.hadoop.conf.dir=/opt/be/plugins/hadoop_conf"));
    EXPECT_EQ(1, count_prefixed(options(), "-Ddoris.jni.hadoop.conf.dir="));

    config::jni_plugin_hadoop_conf_dir = saved;
}

// -Xrs is what keeps the JVM from installing handlers for SIGINT and SIGTERM. Without it the
// BE's own shutdown path is replaced, for the window a JVM takes to start, by Java's
// Shutdown.exit() - an ::exit() from a JVM thread while the BE is serving. It has to be
// passed even when the deployment supplies its own options, so it goes on last.
TEST_F(JvmLauncherOptionsTest, TheJvmIsAskedToLeaveTheShutdownSignalsAlone) {
    EXPECT_TRUE(has(options(), "-Xrs"));

    setenv("JAVA_OPTS", "-Xmx4096m", 1);
    const std::vector<std::string> with_java_opts = options();
    EXPECT_TRUE(has(with_java_opts, "-Xrs"));
    const auto xrs = std::find(with_java_opts.begin(), with_java_opts.end(), "-Xrs");
    const auto xmx = std::find(with_java_opts.begin(), with_java_opts.end(), "-Xmx4096m");
    EXPECT_LT(xmx, xrs) << "a JAVA_OPTS entry must not be able to override -Xrs";
}

namespace {

// Stands in for the BE's own shutdown handler: what matters is only whether it is still
// the installed one after the JVM has started.
void mark_shutdown_requested(int /*signo*/) {}

bool handles(int signo, void (*handler)(int)) {
    struct sigaction current = {};
    EXPECT_EQ(0, sigaction(signo, nullptr, &current));
    return current.sa_handler == handler;
}

} // namespace

// A JVM started without -Xrs installs its own SIGINT, SIGTERM and SIGQUIT handlers. For the two
// shutdown signals its handler answers with a Java Shutdown.exit() - an ::exit() from a JVM thread,
// which runs the C++ global destructors while the BE's threads are still working and skips the
// orderly shutdown main() does. For SIGQUIT it prints a thread dump, which is harmless but is not
// the disposition doris_main.cpp installed. All three have to still be the BE's afterwards, whether
// that is because -Xrs kept the JVM away from them or because BeOwnedSignalGuard put them back for
// a VM somebody else created.
//
// All three, because that is what BE_OWNED_SIGNALS holds: this case asserted two of them while the
// third was added in the same change that made SIGQUIT the BE's, which is exactly the drift a
// signal test exists to catch.
//
// DISABLED, and by the same precedent as jni_util_test.cpp, which disables its own JVM test
// permanently: creating a JVM inside doris_be_test makes ASAN report the JVM's own allocations as
// leaks unless ASAN_OPTIONS carries detect_leaks=0, and run-be-ut.sh does not export it - nor
// should it, since that switch is process-wide and would take leak detection away from every other
// BE test. Run this one by hand, alone, when touching the signal handling:
//
//     ASAN_OPTIONS=detect_leaks=0 ./be/build_.../doris_be_test \
//         --gtest_also_run_disabled_tests --gtest_filter='*TheJvmDoesNotKeepTheShutdownSignals'
//
// Alone matters: only the first ensure_jvm() of a process starts a VM, so the guard below turns
// this into a free pass once any other test has bootstrapped one.
TEST(JvmLauncherSignalTest, DISABLED_TheJvmDoesNotKeepTheShutdownSignals) {
    if (JvmLauncher::vm() != nullptr) {
        GTEST_SKIP() << "another test started the JVM first, so this one would prove nothing; "
                        "run it alone to exercise the bootstrap";
    }

    struct sigaction ours = {};
    ours.sa_handler = &mark_shutdown_requested;
    sigemptyset(&ours.sa_mask);

    struct sigaction saved_int = {};
    struct sigaction saved_term = {};
    struct sigaction saved_quit = {};
    ASSERT_EQ(0, sigaction(SIGINT, &ours, &saved_int));
    ASSERT_EQ(0, sigaction(SIGTERM, &ours, &saved_term));
    ASSERT_EQ(0, sigaction(SIGQUIT, &ours, &saved_quit));
    Defer restore {[&]() {
        sigaction(SIGINT, &saved_int, nullptr);
        sigaction(SIGTERM, &saved_term, nullptr);
        sigaction(SIGQUIT, &saved_quit, nullptr);
    }};

    if (Status status = JvmLauncher::ensure_jvm(); !status.ok()) {
        GTEST_SKIP() << "no JVM available in this environment: " << status;
    }

    EXPECT_TRUE(handles(SIGINT, &mark_shutdown_requested)) << "the JVM took SIGINT over";
    EXPECT_TRUE(handles(SIGTERM, &mark_shutdown_requested)) << "the JVM took SIGTERM over";
    EXPECT_TRUE(handles(SIGQUIT, &mark_shutdown_requested)) << "the JVM took SIGQUIT over";
}

} // namespace doris::Jni
