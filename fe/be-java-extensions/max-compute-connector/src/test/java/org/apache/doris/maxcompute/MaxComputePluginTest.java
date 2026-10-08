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

package org.apache.doris.maxcompute;

import org.apache.doris.jni.spi.DorisPlugin;
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
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin max-compute has no factory named ..." on the query that needed it. The names asserted
 * here are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>Whether the deployed plugin directory actually contains what these classes need at runtime is
 * a different question this file cannot answer - surefire puts {@code provided} dependencies on the
 * test classpath. Only loading the deployed directory through the plugin registry catches that.
 */
public class MaxComputePluginTest {

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                MaxComputePluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assertions.assertEquals(1, found.size(),
                "this module must declare exactly one DorisPlugin in META-INF/services");
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory lists. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assertions.assertTrue(loadPlugin() instanceof MaxComputePlugin);
    }

    /**
     * Reading and writing are two names on one plugin, and the names have to differ from each other
     * whatever their kind: BE sends a plugin name and a factory name, and nothing that says which
     * of the two kinds it wants back.
     */
    @Test
    public void publishesReaderAndWriterUnderTheirPublishedNames() {
        List<String> scanners = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            scanners.add(factory.getName());
        }
        List<String> writers = new ArrayList<>();
        for (JniWriterFactory factory : loadPlugin().getWriterFactories()) {
            writers.add(factory.getName());
        }
        Assertions.assertEquals(Collections.singletonList("reader"), scanners);
        Assertions.assertEquals(Collections.singletonList("writer"), writers);
        Assertions.assertFalse(loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /** A writer BE can build offline - everything up to open() is local. */
    @Test
    public void theWriterFactoryBuildsTheWriter() {
        JniWriter writer = loadPlugin().getWriterFactories().iterator().next().create(1024, writeParams());
        Assertions.assertTrue(writer instanceof MaxComputeJniWriter);
    }

    /**
     * The scanner cannot be built without a read session FE serialized into the parameters, so it is
     * pinned by where it fails instead: everything before that point - the ODPS client, the
     * credential and environment builders - has to have run for this message to be the one raised.
     */
    @Test
    public void theScannerFactoryBuildsTheScanner() {
        Map<String, String> params = scanParams();
        params.put("scan_serializer", "bm90LWEtc2Vzc2lvbg==");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> loadPlugin().getScannerFactories().iterator().next().create(1024, params));
        Assertions.assertEquals("Failed to deserialize table batch read session.", e.getMessage());
    }

    /**
     * The parameter names are FE's, and FE spells them in its own copy of these constants
     * (MCCatalogProperties in fe-connector-maxcompute). Two copies of a wire contract drift
     * silently, so the ones this plugin reads are pinned to their literal text here.
     */
    @Test
    public void readsTheParameterNamesFeEmits() {
        Assertions.assertEquals(
                Arrays.asList("mc.access_key", "mc.secret_key", "mc.auth.type", "mc.ram_role_arn",
                        "mc.ecs_ram_role", "mc.write_max_block_bytes"),
                Arrays.asList(MCProperties.ACCESS_KEY, MCProperties.SECRET_KEY, MCProperties.AUTH_TYPE,
                        MCProperties.RAM_ROLE_ARN, MCProperties.ECS_RAM_ROLE,
                        MCProperties.WRITE_MAX_BLOCK_BYTES));
        Assertions.assertEquals(Arrays.asList("ak_sk", "ram_role_arn", "ecs_ram_role"),
                Arrays.asList(MCProperties.AUTH_TYPE_AK_SK, MCProperties.AUTH_TYPE_RAM_ROLE_ARN,
                        MCProperties.AUTH_TYPE_ECS_RAM_ROLE));
    }

    /** What BE sends down for a two column scan. */
    private static Map<String, String> scanParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "id,name");
        params.put("columns_types", "int#string");
        params.put("endpoint", "http://service.cn.maxcompute.aliyun.com/api");
        params.put("quota", "pay-as-you-go");
        params.put("project", "p");
        params.put("table", "t");
        params.put("session_id", "s");
        params.put("time_zone", "UTC");
        params.put(MCProperties.ACCESS_KEY, "ak");
        params.put(MCProperties.SECRET_KEY, "sk");
        return params;
    }

    /** What BE sends down for a write, minus the block id FE hands out per block. */
    private static Map<String, String> writeParams() {
        Map<String, String> params = new HashMap<>();
        params.put("endpoint", "http://service.cn.maxcompute.aliyun.com/api");
        params.put("project", "p");
        params.put("table", "t");
        params.put("txn_id", "1");
        params.put("write_session_id", "w");
        // Where to ask for the next block id. No connection is made until the write starts.
        params.put("fe_host", "127.0.0.1");
        params.put("fe_port", "9020");
        params.put(MCProperties.ACCESS_KEY, "ak");
        params.put(MCProperties.SECRET_KEY, "sk");
        return params;
    }
}
