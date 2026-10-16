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

package org.apache.doris.jdbc;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.JniWriter;
import org.apache.doris.jni.spi.JniWriterFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE addresses this plugin by name and never by class name, so none of it is checked at compile
 * time: a services file naming a class that moved, a factory name tidied up, or a factory the
 * plugin forgot to declare all compile, and then fail as "plugin jdbc has no factory named ..."
 * on the query that needed it.
 *
 * <p>The names asserted below are the deployment contract with the table in BE's
 * jni_plugin_registry.h. Changing one here without changing it there breaks JDBC catalogs.
 */
public class JdbcPluginTest {

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                JdbcPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assertions.assertEquals(1, found.size(),
                "this module must declare exactly one DorisPlugin in META-INF/services");
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory lists. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assertions.assertTrue(loadPlugin() instanceof JdbcPlugin);
    }

    /**
     * Reading a table and testing a catalog's connection arrive as two different names on the same
     * plugin, because BE calls them at unrelated times - one per query, one when a catalog is
     * created or altered.
     */
    @Test
    public void publishesBothScannersUnderTheirPublishedNames() {
        List<String> names = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            names.add(factory.getName());
        }
        Assertions.assertEquals(Arrays.asList("reader", "connection-tester"), names);
    }

    /**
     * A factory name is unique within its plugin whatever its kind, because BE sends only a
     * plugin name and a factory name - nothing that says which of the two it wants back.
     */
    @Test
    public void publishesTheWriterUnderItsPublishedName() {
        List<String> names = new ArrayList<>();
        for (JniWriterFactory factory : loadPlugin().getWriterFactories()) {
            names.add(factory.getName());
        }
        Assertions.assertEquals(Collections.singletonList("writer"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNoUdfs() {
        Assertions.assertTrue(!loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /**
     * Each name has to reach a different implementation. Two factories that answer to different
     * names but build the same object would pass every name assertion above and then run a scan
     * where a connection test was asked for.
     */
    @Test
    public void eachFactoryBuildsItsOwnKindOfScanner() {
        Map<String, JniScanner> built = new HashMap<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            built.put(factory.getName(), factory.create(1024, scanParams()));
        }
        Assertions.assertTrue(built.get("reader") instanceof JdbcJniScanner);
        Assertions.assertTrue(built.get("connection-tester") instanceof JdbcConnectionTester);

        JniWriter writer = loadPlugin().getWriterFactories().iterator().next()
                .create(1024, new HashMap<>());
        Assertions.assertTrue(writer instanceof JdbcJniWriter);
    }

    /**
     * Statistics reach the query profile through a hook the base class calls, and a subclass that
     * simply did not implement it reports nothing while everything else keeps working - the one
     * way this port could have gone wrong without failing to compile.
     */
    @Test
    public void reportsItsCountersThroughTheSpiEntryPoint() {
        JniScanner scanner = new JdbcScannerFactory().create(1024, scanParams());
        Map<String, String> stats = scanner.getStatistics();
        Assertions.assertEquals("0", stats.get("counter:ReadRows"));
        Assertions.assertEquals("0", stats.get("timer:ReadTime"));
    }

    /** What BE sends down for a two column scan of a MySQL table. */
    private static Map<String, String> scanParams() {
        Map<String, String> params = new HashMap<>();
        params.put("jdbc_url", "jdbc:mysql://127.0.0.1:3306/test");
        params.put("jdbc_driver_class", "com.mysql.cj.jdbc.Driver");
        params.put("jdbc_driver_url", "file:///nonexistent/driver.jar");
        params.put("query_sql", "SELECT id, name FROM t");
        params.put("table_type", "MYSQL");
        params.put("required_fields", "id,name");
        params.put("columns_types", "int#string");
        return params;
    }
}
