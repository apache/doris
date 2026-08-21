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

package org.apache.doris.trinoconnector;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.trinoconnector.testing.TestingTrinoPlugin;

import io.trino.connector.ConnectorName;
import io.trino.server.PluginClassLoader;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.BigintType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * BE addresses this plugin by name and never by class name, so none of that is checked at compile
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin trino-connector has no factory named reader" on the query that needed it. The names
 * asserted here are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>What this file does <em>not</em> check is whether the deployed plugin directory contains
 * everything a scan needs. Surefire puts {@code provided} dependencies on the test classpath, so
 * these tests would still pass with a dependency marked provided and therefore absent from the
 * plugin directory. Only loading the deployed directory through the plugin registry - see the
 * acceptance recipe in the migration notes - can catch that.
 *
 * <p>The Trino connectors themselves are not on any classpath maven controls: an operator installs
 * them under {@code plugins/trino_plugins}, and this plugin loads each one in a classloader of its
 * own. The tests below install one there - a real connector, in {@code testing} - so the nesting is
 * exercised rather than described.
 */
public class TrinoConnectorPluginTest {

    private static final int ROWS = 3;
    private static final String CATALOG = "test_catalog";
    /** Also the directory name, which is what Trino uses as the classloader's id. */
    private static final String INSTALLED_PLUGIN = "testing";

    private static File pluginsDir;
    private static ClassLoader connectorClassLoader;
    private static Map<String, String> handleJson;

    /**
     * A batch lives in memory BE allocates through a native method BE registers, which no plain JVM
     * can link. Off heap has a switch for exactly this, and it swaps in plain Unsafe allocation.
     */
    @BeforeClass
    public static void installOneTrinoConnector() throws Exception {
        OffHeap.setTesting();
        pluginsDir = Files.createTempDirectory("trino_plugins").toFile();
        packageTestingConnectorInto(new File(pluginsDir, INSTALLED_PLUGIN));

        // Through the production path: the factory is what applies BE's directory parameter, and
        // the connectors are loaded once for the whole process on first use.
        new TrinoConnectorScannerFactory().create(1, baseParams());

        ConnectorFactory factory = TrinoConnectorPluginLoader.getTrinoConnectorPluginManager()
                .getConnectorFactories().get(new ConnectorName(TestingTrinoPlugin.CONNECTOR_NAME));
        Assert.assertNotNull("the installed Trino connector must have been loaded from "
                + pluginsDir + " - if this is null the plugin directory parameter never arrived",
                factory);
        connectorClassLoader = factory.getClass().getClassLoader();
        handleJson = serializeHandlesOfTheInstalledConnector();
    }

    @AfterClass
    public static void removeInstalledConnector() throws IOException {
        deleteRecursively(pluginsDir);
    }

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                TrinoConnectorPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assert.assertEquals("this module must declare exactly one DorisPlugin in META-INF/services",
                1, found.size());
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assert.assertTrue(loadPlugin() instanceof TrinoConnectorPlugin);
    }

    /** "reader", not "trino-connector": a factory is named for its job inside the plugin. */
    @Test
    public void publishesItsScannerUnderThePublishedName() {
        List<String> names = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            names.add(factory.getName());
        }
        Assert.assertEquals(Collections.singletonList("reader"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNeitherWritersNorUdfs() {
        Assert.assertFalse(loadPlugin().getWriterFactories().iterator().hasNext());
        Assert.assertFalse(loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /**
     * The nesting this plugin is built on: an installed Trino connector gets a classloader whose
     * parent is the platform loader, so it shares nothing with this plugin except the packages
     * Trino calls its SPI. That is why isolating this module needed no filesystems of its own - a
     * connector reads on its own terms, out of its own directory - and why an installed connector
     * cannot be broken by what this plugin's directory happens to contain.
     *
     * <p>Asserted three ways, because each half can hold while the other does not: the connector's
     * classes are its own (not the identical ones on this classpath), its loader does not reach
     * this one, and the SPI type both sides name is nonetheless one class.
     */
    @Test
    public void loadsAnInstalledConnectorInItsOwnClassloader() {
        Assert.assertTrue("Trino must load an installed connector through its own PluginClassLoader",
                connectorClassLoader instanceof PluginClassLoader);
        Assert.assertNotSame("the connector's classes must be its own copy, not this classpath's",
                TestingTrinoPlugin.Factory.class,
                classOfTheInstalledConnector(TestingTrinoPlugin.Factory.class.getName()));

        for (ClassLoader parent = connectorClassLoader.getParent(); parent != null;
                parent = parent.getParent()) {
            Assert.assertNotSame("an installed connector must not reach this plugin's classpath",
                    TrinoConnectorPluginTest.class.getClassLoader(), parent);
        }

        Assert.assertSame("the Trino SPI must be one class on both sides of the boundary",
                io.trino.spi.connector.ConnectorFactory.class,
                classOfTheInstalledConnector(TestingTrinoPlugin.Factory.class.getName())
                        .getInterfaces()[0]);
    }

    /**
     * End to end over the real wire format. FE plans a scan with the same connector this plugin
     * loads and sends the split, the table handle, the column handles and the transaction handle as
     * JSON; the {@code @type} in each names the classloader that defined it, so deserializing is
     * also what proves the connector was reached. Rows come back through the connector's page
     * source and land in the batch BE reads.
     */
    @Test
    public void readsRowsThroughAnInstalledConnector() throws Exception {
        Map<String, String> params = baseParams();
        params.put("required_fields", TestingTrinoPlugin.COLUMN_NAME);
        params.put("columns_types", "bigint");
        params.put("trino_connector_split", handleJson.get("split"));
        params.put("trino_connector_table_handle", handleJson.get("table"));
        params.put("trino_connector_column_handles", handleJson.get("columns"));
        params.put("trino_connector_column_metadata", handleJson.get("metadata"));
        params.put("trino_connector_predicate", "");
        params.put("trino_connector_trascation_handle", handleJson.get("transaction"));

        JniScanner scanner = loadPlugin().getScannerFactories().iterator().next()
                .create(16, params);
        scanner.open();
        try {
            Assert.assertNotEquals(0, scanner.getNextBatchMeta());
            Assert.assertEquals(ROWS, scanner.getTable().getNumRows());
            scanner.releaseTable();
            Assert.assertEquals("0 means end of stream", 0, scanner.getNextBatchMeta());
        } finally {
            scanner.close();
        }
    }

    /**
     * Every scan reports how long it spent handing columns to BE, one counter per column, and BE
     * copies them into the query profile under the plugin's name. The counters are per column, so
     * a scanner that read one column must publish exactly one.
     */
    @Test
    public void reportsPerColumnAppendTime() throws Exception {
        Map<String, String> params = baseParams();
        params.put("required_fields", TestingTrinoPlugin.COLUMN_NAME);
        params.put("columns_types", "bigint");
        params.put("trino_connector_split", handleJson.get("split"));
        params.put("trino_connector_table_handle", handleJson.get("table"));
        params.put("trino_connector_column_handles", handleJson.get("columns"));
        params.put("trino_connector_column_metadata", handleJson.get("metadata"));
        params.put("trino_connector_predicate", "");
        params.put("trino_connector_trascation_handle", handleJson.get("transaction"));

        JniScanner scanner = loadPlugin().getScannerFactories().iterator().next()
                .create(16, params);
        scanner.open();
        try {
            scanner.getNextBatchMeta();
            scanner.releaseTable();
            Assert.assertEquals(Collections.singleton("timer:AppendDataTime[0]"),
                    scanner.getStatistics().keySet());
        } finally {
            scanner.close();
        }
    }

    /** The catalog, the connector name and the plugin directory every scan carries. */
    private static Map<String, String> baseParams() {
        Map<String, String> params = new HashMap<>();
        params.put(TrinoConnectorScannerFactory.PLUGIN_DIR, pluginsDir.getAbsolutePath());
        params.put("catalog_name", CATALOG);
        params.put("required_fields", TestingTrinoPlugin.COLUMN_NAME);
        params.put("columns_types", "bigint");
        params.put("trino.connector.name", TestingTrinoPlugin.CONNECTOR_NAME);
        params.put("trino.create_time", "2026-01-01 00:00:00");
        return params;
    }

    private static Class<?> classOfTheInstalledConnector(String name) {
        try {
            return connectorClassLoader.loadClass(name);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Serializes handles built by the installed connector, the way FE serializes the ones it plans
     * with - same modules, same codec factory. Reflection is not incidental here: these objects
     * have to come from the connector's own classloader, because that is what Trino's handle
     * resolver writes into the {@code @type} it will later resolve them back through.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> serializeHandlesOfTheInstalledConnector() throws Exception {
        Map<String, Object> handles = (Map<String, Object>) classOfTheInstalledConnector(
                TestingTrinoPlugin.class.getName())
                .getMethod("handles", int.class)
                .invoke(null, ROWS);

        TrinoConnectorHandleCodec codec = new TrinoConnectorHandleCodec(
                TrinoConnectorPluginLoader.getTrinoConnectorPluginManager());
        Map<String, String> json = new HashMap<>();
        json.put("split", codec.toJson(handles.get("split")));
        json.put("table", codec.toJson(handles.get("table")));
        json.put("transaction", codec.toJson(handles.get("transaction")));
        json.put("columns", codec.toJson(Collections.singletonList(handles.get("column"))));
        json.put("metadata", codec.toJson(Collections.singletonList(new TrinoColumnMetadata(
                TestingTrinoPlugin.COLUMN_NAME, BigintType.BIGINT, true, null, null, false,
                Collections.emptyMap()))));
        return json;
    }

    /**
     * Packages the fixture connector the way an operator installs one: a directory named after the
     * connector, holding its jars. Trino names the classloader after that directory, and the name
     * ends up inside every serialized handle.
     */
    private static void packageTestingConnectorInto(File installDir) throws Exception {
        Assert.assertTrue(installDir.mkdirs());
        File classesRoot = new File(TestingTrinoPlugin.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        String packagePath = TestingTrinoPlugin.class.getPackage().getName().replace('.', '/');
        File[] classFiles = new File(classesRoot, packagePath).listFiles(
                (dir, name) -> name.endsWith(".class"));
        Assert.assertNotNull("the fixture connector must be compiled before it can be installed",
                classFiles);

        try (JarOutputStream jar = new JarOutputStream(
                new FileOutputStream(new File(installDir, "testing-connector.jar")))) {
            jar.putNextEntry(new JarEntry("META-INF/services/io.trino.spi.Plugin"));
            jar.write(TestingTrinoPlugin.class.getName().getBytes("UTF-8"));
            jar.closeEntry();
            for (File classFile : classFiles) {
                jar.putNextEntry(new JarEntry(packagePath + "/" + classFile.getName()));
                try (InputStream in = Files.newInputStream(classFile.toPath())) {
                    byte[] buffer = new byte[8192];
                    for (int read; (read = in.read(buffer)) > 0; ) {
                        jar.write(buffer, 0, read);
                    }
                }
                jar.closeEntry();
            }
        }
    }

    private static void deleteRecursively(File file) throws IOException {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        Files.deleteIfExists(file.toPath());
    }
}
