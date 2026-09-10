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

package org.apache.doris.extension.loader;

import org.apache.doris.extension.loader.testplugins.AbsentDependencyProbe;
import org.apache.doris.extension.loader.testplugins.AbsentStaticInitTestPluginFactory;
import org.apache.doris.extension.loader.testplugins.AbsentSuperclassTestPluginFactory;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

/**
 * A plugin whose dependency is absent must fail alone.
 *
 * <p>This is the shape of "the plugin is installed but the library bundle it needs is not": the
 * missing class is reported by the JVM as {@code NoClassDefFoundError}, an {@link Error}, which is
 * not a {@link ReflectiveOperationException} and so used to walk straight out of {@code loadAll} and
 * out of the FE's startup path with it. One uninstalled bundle must cost the plugins that need it,
 * not the FE.
 *
 * <p>The two cases are the two places the loader touches plugin bytecode, and they fail one step
 * apart: resolving the class (a supertype the jar does not carry) and first initializing it (a
 * static initializer that reaches outside the jar).
 *
 * <p>The advice sentence itself is asserted separately, against fabricated throwables. The message it
 * has to reject - the JVM's wording for a class whose initializer had already failed - only appears on
 * the second attempt at initializing one class, and no plugin makes the loader do that: {@code loadAll}
 * gives up and closes the classloader on the first.
 */
class DirectoryPluginRuntimeManagerLinkageTest {

    @TempDir
    Path tempDir;

    @Test
    void testAbsentSuperclassIsAFailureNotAThrow() throws IOException {
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("absent-superclass").resolve("absent-superclass.jar"),
                AbsentSuperclassTestPluginFactory.class, "1.0");

        assertReportsMissingDependency(load(root));
    }

    @Test
    void testAbsentStaticInitDependencyIsAFailureNotAThrow() throws IOException {
        Path root = tempDir.resolve("plugins");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("absent-static-init").resolve("absent-static-init.jar"),
                AbsentStaticInitTestPluginFactory.class, "1.0");

        assertReportsMissingDependency(load(root));
    }

    @Test
    void testAlreadyFailedInitializationIsNotReportedAsAMissingClass() {
        // "Could not initialize class X" means X was found and its initializer threw - a broken plugin,
        // not an uninstalled bundle. Read as a missing class, its first token would advertise a class
        // named "Could" and send the reader off to look at the shared library root.
        String advice = DirectoryPluginRuntimeManager.missingClassAdvice(
                new NoClassDefFoundError("Could not initialize class org.example.BrokenFactory"));

        Assertions.assertEquals("", advice,
                () -> "an initializer that already failed is not a missing dependency: " + advice);
    }

    @Test
    void testAMissingClassIsNamedInEveryFormTheJvmReportsItIn() {
        Assertions.assertTrue(adviceFor(new NoClassDefFoundError("org/example/Probe"))
                        .contains("The class org/example/Probe is"),
                "the plain form names the class directly");
        Assertions.assertTrue(
                adviceFor(new NoClassDefFoundError("org/example/Probe (wrong name: org/other/Probe)"))
                        .contains("The class org/example/Probe is"),
                "the wrong-name form carries a trailing explanation the class name comes before");
        Assertions.assertTrue(
                adviceFor(new ExceptionInInitializerError(new ClassNotFoundException("org.example.Probe")))
                        .contains("The class org.example.Probe is"),
                "a static initializer's first failure wraps the real miss");

        // An initializer failure on top of a genuine miss: skipping the wording must not stop the walk.
        NoClassDefFoundError alreadyFailed =
                new NoClassDefFoundError("Could not initialize class org.example.BrokenFactory");
        alreadyFailed.initCause(new NoClassDefFoundError("org/example/Probe"));
        Assertions.assertTrue(adviceFor(alreadyFailed).contains("The class org/example/Probe is"),
                "the miss below the initializer failure is what the reader needs");
    }

    private static String adviceFor(Throwable failure) {
        String advice = DirectoryPluginRuntimeManager.missingClassAdvice(failure);
        Assertions.assertTrue(advice.contains("shared library bundle"),
                () -> "the advice must say where a missing dependency is expected to come from: " + advice);
        return advice;
    }

    /**
     * Loads {@code root} against a parent that refuses {@link AbsentDependencyProbe}. The fabricated
     * jars leave that class out, so the plugin classloader cannot find it in its own jars either -
     * exactly the reachability a plugin has when the shared bundle carrying its dependency is not
     * installed.
     */
    private LoadReport<PluginFactory> load(Path root) {
        ClassLoader hidingParent = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (AbsentDependencyProbe.class.getName().equals(name)) {
                    throw new ClassNotFoundException(name + " is withheld by this test");
                }
                return super.loadClass(name, resolve);
            }
        };
        // Assertions.assertDoesNotThrow is the point of the test: before the loader caught LinkageError
        // this call threw, and nothing between here and FE startup would have caught it.
        return Assertions.assertDoesNotThrow(() -> new DirectoryPluginRuntimeManager<PluginFactory>().loadAll(
                Collections.singletonList(root),
                hidingParent,
                PluginFactory.class,
                null,
                DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE));
    }

    private static void assertReportsMissingDependency(LoadReport<PluginFactory> report) {
        Assertions.assertTrue(report.getSuccesses().isEmpty(), "the plugin cannot have loaded");
        Assertions.assertEquals(1, report.getFailures().size());

        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_INSTANTIATE, failure.getStage());
        Assertions.assertInstanceOf(LinkageError.class, failure.getCause(),
                () -> "expected the linkage failure to be preserved as the cause, got " + failure.getCause());
        Assertions.assertTrue(
                failure.getMessage().contains(AbsentDependencyProbe.class.getName().replace('.', '/')),
                () -> "the message must name the class that is missing: " + failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("shared library bundle"),
                () -> "the message must say where a missing dependency is expected to come from: "
                        + failure.getMessage());
    }
}
