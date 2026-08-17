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

package org.apache.doris.jni.bootstrap;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.SpiVersion;
import org.apache.doris.jni.spi.UdfExecutorFactory;
import org.apache.doris.jni.testplugin.CollidingPlugin;
import org.apache.doris.jni.testplugin.NamedScannerFactory;
import org.apache.doris.jni.testplugin.TestPlugin;
import org.apache.doris.jni.testplugin.TestScanner;
import org.apache.doris.jni.testplugin.TestScannerFactory;
import org.apache.doris.jni.testplugin.TestUdfExecutorFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Exercises the runtime against plugin directories built as real jars.
 *
 * <p>The properties under test are the ones the architecture rests on: a plugin cannot reach BE's
 * classpath, cannot substitute its own copy of the SPI, and cannot take BE or another plugin down
 * with it.
 */
class PluginRuntimeTest {

    private static final Class<?>[] SAMPLE_CLASSES = {
        TestPlugin.class, TestScannerFactory.class, TestScanner.class, TestUdfExecutorFactory.class,
    };

    private PluginRuntime runtimeOver(Path pluginsDir) {
        return new PluginRuntime(pluginsDir, getClass().getClassLoader());
    }

    // ------------------------------------------------------------------
    // Discovery and the class space.
    // ------------------------------------------------------------------

    @Test
    void pluginIsFoundThroughItsServiceDescriptorAndIndexedByFactoryName(@TempDir Path plugins)
            throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);

        PluginHandle handle = runtimeOver(plugins).plugin("sample");

        Assertions.assertEquals(PluginHandle.State.READY, handle.state(), handle.failure());
        Assertions.assertEquals(Collections.singleton("sample"), handle.scannerFactories().keySet());
        Assertions.assertEquals(Collections.singleton("udf"), handle.udfExecutorFactories().keySet());
        Assertions.assertEquals(Collections.emptySet(), handle.writerFactories().keySet());
    }

    /**
     * The whole point of the design: plugin classes come from the plugin's own jars even when an
     * identically named class exists on BE's classpath. If the loader ever fell back to its parent,
     * these two would be the same class and every version conflict would silently resolve to
     * whatever BE ships.
     */
    @Test
    void pluginClassesAreLoadedFromThePluginNotFromBeClasspath(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginHandle handle = runtimeOver(plugins).plugin("sample");

        Class<?> insidePlugin = handle.scannerFactories().get("sample").getClass();
        Assertions.assertEquals(TestScannerFactory.class.getName(), insidePlugin.getName());
        Assertions.assertNotSame(TestScannerFactory.class, insidePlugin,
                "the plugin's class must not be the copy on BE's classpath");
        Assertions.assertSame(handle.classLoader(), insidePlugin.getClassLoader());
    }

    /**
     * ...and the mirror image: SPI types must be the one class both sides hold, or nothing could be
     * handed across the boundary at all.
     */
    @Test
    void spiClassesAreShared(@TempDir Path plugins) throws IOException, ClassNotFoundException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginHandle handle = runtimeOver(plugins).plugin("sample");

        Class<?> factoryInterface = handle.scannerFactories().get("sample").getClass().getInterfaces()[0];
        Assertions.assertSame(org.apache.doris.jni.spi.JniScannerFactory.class, factoryInterface);
        Assertions.assertSame(DorisPlugin.class,
                handle.classLoader().loadClass(DorisPlugin.class.getName()));
    }

    /** BE's system classpath is not on the plugin's search path at all. */
    @Test
    void bePrivateClassesAreInvisibleToThePlugin(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginHandle handle = runtimeOver(plugins).plugin("sample");

        Assertions.assertThrows(ClassNotFoundException.class,
                () -> handle.classLoader().loadClass(PluginRegistry.class.getName()),
                "the plugin runtime itself must not be reachable from a plugin");
        // The JDK still is, through the platform classloader.
        Assertions.assertDoesNotThrow(() -> handle.classLoader().loadClass("java.util.ArrayList"));
    }

    // ------------------------------------------------------------------
    // Instantiation.
    // ------------------------------------------------------------------

    @Test
    void createInstanceRunsTheFactoryUnderThePluginClassLoader(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginRuntime runtime = runtimeOver(plugins);
        PluginHandle handle = runtime.plugin("sample");

        Object instance = runtime.createInstance("sample", "sample", 64,
                Collections.singletonMap("k", "v"));

        Assertions.assertTrue(instance instanceof JniScanner,
                "the SPI base class is shared, so BE can talk to the instance");
        Map<String, String> statistics = ((JniScanner) instance).getStatistics();
        Assertions.assertEquals(String.valueOf(handle.classLoader()),
                statistics.get("contextClassLoaderAtCreate"),
                "the factory must run with the plugin's classloader as context classloader:"
                        + " BE's own threads do not have one that can see plugin classes");
        Assertions.assertEquals("64", statistics.get("batchSize"));
        Assertions.assertEquals("{k=v}", statistics.get("params"));
    }

    @Test
    void udfParametersReachThePluginAsUnmodifiedBytes(@TempDir Path plugins) throws Exception {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);

        Object executor = runtimeOver(plugins).createUdfExecutor("sample", "udf",
                "thrift-payload".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("thrift-payload", executor);
    }

    @Test
    void anUnknownFactoryNameListsWhatThePluginActuallyProvides(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginRuntime runtime = runtimeOver(plugins);

        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> runtime.createInstance("sample", "typo", 8, Collections.emptyMap()));
        Assertions.assertTrue(failure.getMessage().contains("[sample]"), failure.getMessage());
    }

    // ------------------------------------------------------------------
    // Failure containment.
    // ------------------------------------------------------------------

    @Test
    void anAbsentPluginIsNotDeployedAndSaysWhereItWasExpected(@TempDir Path plugins) {
        PluginRuntime runtime = runtimeOver(plugins);

        Assertions.assertEquals(PluginHandle.State.NOT_DEPLOYED, runtime.plugin("absent").state());
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> runtime.createInstance("absent", "any", 8, Collections.emptyMap()));
        Assertions.assertTrue(failure.getMessage().contains("is not deployed"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains(plugins.resolve("absent").toString()),
                failure.getMessage());
    }

    /**
     * A corrupt jar used to be fatal: the old loader eagerly loaded every class of every extension
     * at startup and exited the process on failure. Now it costs exactly the one plugin.
     */
    @Test
    void corruptPluginFailsAloneAndLeavesItsNeighbourWorking(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "healthy", TestPlugin.class, SAMPLE_CLASSES);
        Path broken = Files.createDirectories(plugins.resolve("broken"));
        PluginJars.addRawJar(broken, "broken.jar", "this is not a zip file".getBytes(StandardCharsets.UTF_8));

        PluginRuntime runtime = runtimeOver(plugins);
        runtime.warmup();

        Assertions.assertEquals(PluginHandle.State.FAILED, runtime.plugin("broken").state());
        Assertions.assertEquals(PluginHandle.State.READY, runtime.plugin("healthy").state());
        Assertions.assertNotNull(
                runtime.createInstance("healthy", "sample", 8, Collections.emptyMap()));
    }

    @Test
    void serviceDescriptorPointingAtAMissingClassFailsThatPluginOnly(@TempDir Path plugins)
            throws IOException {
        PluginJars.deploy(plugins, "ghost", "org.apache.doris.jni.testplugin.NoSuchPlugin",
                SpiVersion.version(), new Class<?>[0]);

        PluginHandle handle = runtimeOver(plugins).plugin("ghost");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains("NoSuchPlugin"), handle.failure());
    }

    @Test
    void directoryWithoutAServiceDescriptorSaysSo(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "nameless", null, SpiVersion.version(), SAMPLE_CLASSES);

        PluginHandle handle = runtimeOver(plugins).plugin("nameless");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains("META-INF/services"), handle.failure());
    }

    /**
     * The failure is diagnosed at load time. Left to surface on its own it would appear as a
     * ClassCastException between two classes with the same name, which is the least actionable
     * error message in Java.
     */
    @Test
    void pluginThatPackagesTheSpiIsRejectedWithTheFix(@TempDir Path plugins) throws IOException {
        Path dir = PluginJars.deploy(plugins, "greedy", TestPlugin.class, SAMPLE_CLASSES);
        PluginJars.addSpiClass(dir, "bundled-spi.jar");

        PluginHandle handle = runtimeOver(plugins).plugin("greedy");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains("provided"), handle.failure());
    }

    @Test
    void twoFactoriesWithOneNameAreRejected(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "colliding", CollidingPlugin.class,
                NamedScannerFactory.class, TestScanner.class);

        PluginHandle handle = runtimeOver(plugins).plugin("colliding");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains("dup"), handle.failure());
    }

    /** The same failure is replayed rather than retried, so the tenth query reads like the first. */
    @Test
    void failedPluginIsNotRetried(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "ghost", "org.apache.doris.jni.testplugin.NoSuchPlugin",
                SpiVersion.version(), new Class<?>[0]);
        PluginRuntime runtime = runtimeOver(plugins);

        PluginHandle first = runtime.plugin("ghost");
        PluginHandle second = runtime.plugin("ghost");

        Assertions.assertSame(first, second);
    }

    // ------------------------------------------------------------------
    // Version gate.
    // ------------------------------------------------------------------

    @Test
    void pluginBuiltAgainstAnotherMajorIsRejectedWithBothVersions(@TempDir Path plugins)
            throws IOException {
        PluginJars.deploy(plugins, "stale", TestPlugin.class.getName(),
                (SpiVersion.major() + 1) + ".0", SAMPLE_CLASSES);

        PluginHandle handle = runtimeOver(plugins).plugin("stale");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains(String.valueOf(SpiVersion.major() + 1)),
                handle.failure());
        Assertions.assertTrue(handle.failure().contains(SpiVersion.version()), handle.failure());
    }

    /** A differing minor is compatible by definition: any surface change bumps the major. */
    @Test
    void differingMinorIsAccepted(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "newer", TestPlugin.class.getName(),
                SpiVersion.major() + ".99", SAMPLE_CLASSES);

        Assertions.assertEquals(PluginHandle.State.READY, runtimeOver(plugins).plugin("newer").state());
    }

    /**
     * No attribute means "not built by this build". Admitting it would make the gate pointless,
     * since a hand-assembled jar is exactly the case it exists to catch.
     */
    @Test
    void pluginWithoutTheVersionAttributeIsRejected(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "unstamped", TestPlugin.class.getName(), null, SAMPLE_CLASSES);

        PluginHandle handle = runtimeOver(plugins).plugin("unstamped");

        Assertions.assertEquals(PluginHandle.State.FAILED, handle.state());
        Assertions.assertTrue(handle.failure().contains(SpiVersion.MANIFEST_ATTRIBUTE), handle.failure());
    }

    // ------------------------------------------------------------------
    // DROP FUNCTION and status.
    // ------------------------------------------------------------------

    @Test
    void droppingAFunctionReachesEveryLoadedPlugin(@TempDir Path plugins) throws Exception {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        PluginRuntime runtime = runtimeOver(plugins);
        runtime.warmup();

        runtime.cleanUdfCache("db.fn(INT)");

        UdfExecutorFactory factory = runtime.plugin("sample").udfExecutorFactories().get("udf");
        List<?> invalidated = (List<?>) factory.getClass()
                .getMethod("invalidatedSignatures").invoke(factory);
        Assertions.assertEquals(Collections.singletonList("db.fn(INT)"), invalidated);
    }

    @Test
    void statusReportsEveryPluginItHasTouched(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        Path broken = Files.createDirectories(plugins.resolve("broken"));
        PluginJars.addRawJar(broken, "broken.jar", "nope".getBytes(StandardCharsets.UTF_8));
        PluginRuntime runtime = runtimeOver(plugins);
        runtime.warmup();

        String json = runtime.statusJson();

        Assertions.assertTrue(json.contains("\"name\":\"sample\",\"state\":\"READY\""), json);
        Assertions.assertTrue(json.contains("\"name\":\"broken\",\"state\":\"FAILED\""), json);
        Assertions.assertTrue(json.contains("\"scanners\":[\"sample\"]"), json);
        Assertions.assertTrue(json.contains("\"apiVersion\":\"" + SpiVersion.version() + "\""), json);
    }

    @Test
    void statusCountsWhatLoadedAndWhatIsDeployed(@TempDir Path plugins) throws IOException {
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        Path broken = Files.createDirectories(plugins.resolve("broken"));
        PluginJars.addRawJar(broken, "broken.jar", "nope".getBytes(StandardCharsets.UTF_8));
        PluginRuntime runtime = runtimeOver(plugins);

        // Deployed and known before anything is loaded, which is the state an operator asking
        // "why is my plugin not listed" is actually in: plugins load on first use.
        String beforeUse = runtime.statusJson();
        Assertions.assertTrue(beforeUse.contains("\"deployed\":[\"broken\",\"sample\"]"), beforeUse);
        Assertions.assertTrue(beforeUse.contains("\"loadedCount\":0"), beforeUse);
        Assertions.assertTrue(beforeUse.contains("\"plugins\":[]"), beforeUse);

        runtime.warmup();

        String afterWarmup = runtime.statusJson();
        Assertions.assertTrue(afterWarmup.contains("\"loadedCount\":1"), afterWarmup);
        Assertions.assertTrue(afterWarmup.contains("\"failedCount\":1"), afterWarmup);
    }

    @Test
    void warmupOnAnAbsentDirectoryIsNotAnError(@TempDir Path parent) {
        PluginRuntime runtime = runtimeOver(parent.resolve("never-created"));

        Assertions.assertDoesNotThrow(runtime::warmup);
        Assertions.assertTrue(runtime.statusJson().contains("\"plugins\":[]"), runtime.statusJson());
    }

    // ------------------------------------------------------------------
    // Names that come from SQL.
    // ------------------------------------------------------------------

    /**
     * A plugin name reaches the runtime from the {@code writer_class} property, so it is user text.
     * Resolving one that is a path rather than a name would load jars from somewhere else on the
     * host and report it as a plugin.
     */
    @Test
    void pluginNameThatIsAPathIsRejected(@TempDir Path plugins) {
        PluginRuntime runtime = runtimeOver(plugins);

        for (String name : new String[] {"", ".", "..", "../..", "/etc", "a/b", "a\\b"}) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> runtime.plugin(name),
                    "accepted '" + name + "' as a plugin name");
        }
    }

    /** ... and a name nobody deployed is answered without being remembered. */
    @Test
    void namesThatWereNeverDeployedDoNotAccumulate(@TempDir Path plugins) {
        PluginRuntime runtime = runtimeOver(plugins);

        for (int i = 0; i < 100; i++) {
            Assertions.assertEquals(PluginHandle.State.NOT_DEPLOYED,
                    runtime.plugin("made-up-" + i).state());
        }

        Assertions.assertTrue(runtime.statusJson().contains("\"plugins\":[]"), runtime.statusJson());
    }

    // ------------------------------------------------------------------
    // Hadoop configuration files.
    // ------------------------------------------------------------------

    /**
     * A plugin cannot reach BE's classpath, and conf/ is on it - so a hadoop Configuration built
     * inside a plugin would find no core-site.xml at all. The conf drop point is the one place
     * outside the plugin directory its classloader answers resources from.
     */
    @Test
    void pluginsReadHadoopConfigurationFromTheConfDirectory(@TempDir Path root) throws IOException {
        Path plugins = Files.createDirectories(root.resolve("jni"));
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        Path hadoopConf = Files.createDirectories(root.resolve("hadoop_conf"));
        Files.write(hadoopConf.resolve("core-site.xml"),
                "<configuration/>".getBytes(StandardCharsets.UTF_8));

        PluginHandle handle =
                new PluginRuntime(plugins, getClass().getClassLoader(), hadoopConf).plugin("sample");

        Assertions.assertEquals(PluginHandle.State.READY, handle.state(), handle.failure());
        URL found = handle.classLoader().getResource("core-site.xml");
        Assertions.assertNotNull(found, "the plugin cannot see the hadoop conf drop point");
        Assertions.assertTrue(found.toString().endsWith("hadoop_conf/core-site.xml"),
                found.toString());
    }

    /** Resources only: the drop point must not become a second route to classes. */
    @Test
    void theConfDirectoryDoesNotLoadClasses(@TempDir Path root) throws IOException {
        Path plugins = Files.createDirectories(root.resolve("jni"));
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);
        Path hadoopConf = Files.createDirectories(root.resolve("hadoop_conf"));
        Files.write(hadoopConf.resolve("Marker.class"), "nope".getBytes(StandardCharsets.UTF_8));

        PluginHandle handle =
                new PluginRuntime(plugins, getClass().getClassLoader(), hadoopConf).plugin("sample");

        Assertions.assertEquals(PluginHandle.State.READY, handle.state(), handle.failure());
        // Visible as a resource, because that is what the directory is searched for...
        Assertions.assertNotNull(handle.classLoader().getResource("Marker.class"));
        // ... and not as a class, which is what keeps this from being a second class space.
        Assertions.assertThrows(ClassNotFoundException.class,
                () -> handle.classLoader().loadClass("Marker"));
    }

    /** A conf directory that does not exist is the ordinary case, not a failure. */
    @Test
    void anAbsentConfDirectoryIsFine(@TempDir Path root) throws IOException {
        Path plugins = Files.createDirectories(root.resolve("jni"));
        PluginJars.deploy(plugins, "sample", TestPlugin.class, SAMPLE_CLASSES);

        PluginHandle handle = new PluginRuntime(plugins, getClass().getClassLoader(),
                root.resolve("never-created")).plugin("sample");

        Assertions.assertEquals(PluginHandle.State.READY, handle.state(), handle.failure());
        Assertions.assertNull(handle.classLoader().getResource("core-site.xml"));
    }
}
