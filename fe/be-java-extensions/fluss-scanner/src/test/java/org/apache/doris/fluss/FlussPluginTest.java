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

package org.apache.doris.fluss;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScannerFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE addresses this plugin by name and never by class name, so none of that is checked at compile
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin fluss has no factory named reader" on the query that needed it. The names asserted here
 * are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>What this file does <em>not</em> check is whether the deployed plugin directory contains
 * everything a scan needs. Surefire puts {@code provided} dependencies on the test classpath, so
 * these tests would still pass with a dependency marked provided and therefore absent from the
 * plugin directory. Only loading the deployed directory through the plugin registry can catch that;
 * what the layout check in tools/be-java-plugins can see statically, it checks at build time.
 */
public class FlussPluginTest {

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                FlussPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assertions.assertEquals(1, found.size(),
                "this module must declare exactly one DorisPlugin in META-INF/services");
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assertions.assertTrue(loadPlugin() instanceof FlussPlugin);
    }

    /** "reader", not "fluss": a factory is named for its job inside the plugin. */
    @Test
    public void publishesItsScannerUnderThePublishedName() {
        List<String> names = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            names.add(factory.getName());
        }
        Assertions.assertEquals(Collections.singletonList("reader"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNeitherWritersNorUdfs() {
        Assertions.assertFalse(loadPlugin().getWriterFactories().iterator().hasNext());
        Assertions.assertFalse(loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /** The factory builds the scanner rather than merely naming it. */
    @Test
    public void buildsTheScanner() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "id");
        params.put("columns_types", "int");
        params.put("fluss.range_type", "LOG");
        params.put("fluss.bucket_id", "0");
        params.put("fluss.log_start_offset", "0");
        params.put("fluss.log_stop_offset", "1");
        Assertions.assertTrue(loadPlugin().getScannerFactories().iterator().next().create(1024, params)
                instanceof FlussJniScanner);
    }

    /**
     * The one slf4j API in this plugin's directory is the 1.7 copy shaded into fluss-client, which
     * binds through org.slf4j.impl.StaticLoggerBinder - so the JUL binding deployed beside it has
     * to be a 1.7 one too, or the plugin logs nothing. The pom keeps the test classpath shaped like
     * the plugin directory (the tree's slf4j 2.x and log4j narrowed to test, log4j's provider kept
     * off surefire's classpath) precisely so that this assertion means what it says.
     */
    @Test
    public void bindsSlf4jToJulLikeTheDeployedPlugin() {
        Assertions.assertEquals("org.slf4j.impl.JDK14LoggerFactory",
                LoggerFactory.getILoggerFactory().getClass().getName());
    }
}
