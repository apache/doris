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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link HmsConf} — the deployment-level half of this plugin's settings, read from its own
 * {@code hms.conf} with a fall back to the {@code fe.conf} key each setting used to live under.
 *
 * <p>The assertions go through {@code HmsConf} rather than {@code ConnectorConf.get} with hand-passed
 * keys: a test that passes its own key names proves only what the test passed, and would stay green if
 * production stopped consulting the fe.conf fallback.
 */
class HmsConfTest {

    private static ConnectorContext context(Map<String, String> conf, Map<String, String> env) {
        return new FakeConnectorContext("c", 1L, env, conf);
    }

    @Test
    void defaultFileFormatFallsBackToOrcWhenNothingIsConfigured() {
        Assertions.assertEquals("orc",
                HmsConf.defaultFileFormat(context(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    void defaultFileFormatFallsBackToTheFeConfKey() {
        Map<String, String> env = new HashMap<>();
        env.put(HmsConf.ENV_HIVE_DEFAULT_FILE_FORMAT, "parquet");
        Assertions.assertEquals("parquet", HmsConf.defaultFileFormat(context(Collections.emptyMap(), env)));
    }

    @Test
    void thePluginConfWinsOverTheFeConfKey() {
        Map<String, String> conf = new HashMap<>();
        conf.put(HmsConf.CONF_DEFAULT_FILE_FORMAT, "text");
        Map<String, String> env = new HashMap<>();
        env.put(HmsConf.ENV_HIVE_DEFAULT_FILE_FORMAT, "parquet");
        Assertions.assertEquals("text", HmsConf.defaultFileFormat(context(conf, env)));
    }

    @Test
    void bucketTableCreationIsOffUnlessTurnedOn() {
        Assertions.assertFalse(
                HmsConf.enableCreateBucketTable(context(Collections.emptyMap(), Collections.emptyMap())));
    }

    @Test
    void bucketTableCreationReadsThePluginConfThenTheFeConfKey() {
        Map<String, String> env = new HashMap<>();
        env.put(HmsConf.ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "true");
        Assertions.assertTrue(HmsConf.enableCreateBucketTable(context(Collections.emptyMap(), env)));

        Map<String, String> conf = new HashMap<>();
        conf.put(HmsConf.CONF_ENABLE_CREATE_BUCKET_TABLE, "false");
        Assertions.assertFalse(HmsConf.enableCreateBucketTable(context(conf, env)));
    }

    /**
     * The settings file is named after {@code ConnectorProvider.name()}, not after the plugin
     * directory: this plugin lives in {@code plugins/connector/hive/} but reads {@code hms.conf}.
     */
    @Test
    void theShippedTemplateIsNamedAfterTheProviderName() {
        String name = new HiveConnectorProvider().getType();
        Assertions.assertEquals("hms", name);
        Assertions.assertNotNull(HmsConfTest.class.getClassLoader().getResource(name + ".conf.template"),
                "the plugin must ship " + name + ".conf.template");
    }
}
