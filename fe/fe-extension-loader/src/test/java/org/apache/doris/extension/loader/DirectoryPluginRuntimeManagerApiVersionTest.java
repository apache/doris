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

import org.apache.doris.extension.loader.testplugins.MetadataTestPluginFactory;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

/**
 * The gate as the directory loader applies it: what a real plugin jar declares in its MANIFEST decides
 * whether the plugin is published at all.
 *
 * <p>This is the family-neutral half of the contract, and it is tested once here rather than four times: the
 * loader has no family-specific code, so CONNECTOR / FILESYSTEM / AUTHENTICATION / LINEAGE all run exactly
 * this code path. What each family owns — that it passes a gate at all, and that the gate names the right
 * attribute and reads the right resource — is asserted in that family's own wiring test.
 *
 * <p>Note what is <em>not</em> asserted: that the refused plugin's classloader was closed. The loader never
 * hands a rejected classloader back, so closure is not observable from here; it rides on the same
 * {@code catch (PluginLoadException)} block that every other post-classloader reject path uses.
 */
class DirectoryPluginRuntimeManagerApiVersionTest {

    private static final ApiVersionGate GATE = DirectoryPluginRuntimeManagerMetadataTest.TEST_GATE;

    @TempDir
    Path tempDir;

    @Test
    void testDeclaredMajorMatchesSoThePluginLoads() throws IOException {
        LoadReport<PluginFactory> report = loadWithDeclaredVersion("match", "7.0");

        Assertions.assertEquals(1, report.getSuccesses().size(), () -> describe(report));
        Assertions.assertEquals("metadata-test", report.getSuccesses().get(0).getPluginName());
    }

    @Test
    void testDeclaredMajorDiffersSoThePluginIsRefused() throws IOException {
        LoadReport<PluginFactory> report = loadWithDeclaredVersion("mismatch", "6.0");

        assertRefused(report, "6.0", "7.2");
    }

    @Test
    void testUndeclaredVersionIsRefusedFailClosed() throws IOException {
        // A plugin built before this contract existed, or one whose pom never stamped the attribute. It must
        // not load: "declares nothing" is the case the previous, always-true check let through.
        LoadReport<PluginFactory> report = loadWithDeclaredVersion("undeclared", null);

        assertRefused(report, "Doris-Test-Plugin-Api-Version", "7.2");
    }

    @Test
    void testMalformedDeclarationIsRefused() throws IOException {
        LoadReport<PluginFactory> report = loadWithDeclaredVersion("malformed", "seven-point-two");

        assertRefused(report, "seven-point-two", "7.2");
    }

    @Test
    void testRefusedPluginLeavesNoTraceAndRepeatsIdentically() throws IOException {
        Path root = tempDir.resolve("repeat");
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("metadata-test").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4", "6.0");
        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();

        LoadReport<PluginFactory> first = manager.loadAll(Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(), PluginFactory.class, null, GATE);
        LoadReport<PluginFactory> second = manager.loadAll(Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(), PluginFactory.class, null, GATE);

        // A refusal must not half-register the plugin: neither run may publish a handle, and the second run
        // must report the version problem again rather than a spurious duplicate-name conflict.
        Assertions.assertFalse(manager.get("metadata-test").isPresent());
        Assertions.assertTrue(manager.list().isEmpty());
        for (LoadReport<PluginFactory> report : Arrays.asList(first, second)) {
            Assertions.assertTrue(report.getSuccesses().isEmpty(), () -> describe(report));
            Assertions.assertEquals(LoadFailure.STAGE_API_VERSION, report.getFailures().get(0).getStage());
        }
    }

    @Test
    void testGateIsMandatory() {
        // An optional gate would be a way to end up with no version check at all without anyone noticing —
        // the exact shape of the bug this whole mechanism replaces.
        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        Assertions.assertThrows(NullPointerException.class,
                () -> manager.loadAll(Collections.singletonList(tempDir),
                        Thread.currentThread().getContextClassLoader(), PluginFactory.class, null, null));
    }

    private LoadReport<PluginFactory> loadWithDeclaredVersion(String rootName, String declaredApiVersion)
            throws IOException {
        Path root = tempDir.resolve(rootName);
        DirectoryPluginRuntimeManagerMetadataTest.createPluginJar(
                root.resolve("metadata-test").resolve("metadata-test.jar"),
                MetadataTestPluginFactory.class, "3.1.4", declaredApiVersion);
        return new DirectoryPluginRuntimeManager<PluginFactory>().loadAll(
                Collections.singletonList(root),
                Thread.currentThread().getContextClassLoader(),
                PluginFactory.class,
                null,
                GATE);
    }

    private static void assertRefused(LoadReport<PluginFactory> report, String... mustMention) {
        Assertions.assertTrue(report.getSuccesses().isEmpty(),
                "an incompatible plugin must never be published: " + describe(report));
        Assertions.assertEquals(1, report.getFailures().size(), () -> describe(report));
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_API_VERSION, failure.getStage());
        for (String fragment : mustMention) {
            // An operator reading only the FE log has to be able to tell what the plugin claimed and what
            // this FE wanted; a bare "rejected" would send them straight back to guessing.
            Assertions.assertTrue(failure.getMessage().contains(fragment),
                    "failure message must mention '" + fragment + "': " + failure.getMessage());
        }
        Assertions.assertTrue(failure.getPluginDir().toString().endsWith("metadata-test"),
                failure.getPluginDir().toString());
    }

    private static String describe(LoadReport<PluginFactory> report) {
        StringBuilder sb = new StringBuilder("successes=").append(report.getSuccesses().size())
                .append(", failures:");
        for (LoadFailure failure : report.getFailures()) {
            sb.append(" [").append(failure.getStage()).append("] ").append(failure.getMessage());
        }
        return sb.toString();
    }
}
