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

package org.apache.doris.connector.lance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * How this connector's one deployment-level setting is resolved: lance.conf, then the built-in default.
 *
 * <p>Asserted rather than trusted because every failure direction here is silent: a template under the
 * wrong name deploys a file the engine never opens, and a read that consults fe.conf would keep a setting
 * alive through a channel the engine no longer feeds.
 */
class LanceConnectorConfTest {

    @Test
    void theConfTemplateIsNamedAfterTheProviderAndDocumentsTheKey() throws Exception {
        String expected = new LanceConnectorProvider().name() + ".conf.template";
        try (InputStream template = getClass().getClassLoader().getResourceAsStream(expected)) {
            Assertions.assertNotNull(template, "the plugin must ship " + expected + " on its classpath");
            String text = new String(template.readAllBytes(), StandardCharsets.UTF_8);
            Assertions.assertTrue(text.contains("# " + LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES + "="
                    + LanceConf.DEFAULT_ARROW_MEMORY_LIMIT_BYTES), text);
        }
    }

    @Test
    void theMemoryLimitComesFromLanceConfThenTheDefault() {
        Assertions.assertEquals(LanceConf.DEFAULT_ARROW_MEMORY_LIMIT_BYTES,
                LanceConf.arrowMemoryLimitBytes(new LanceTestContext()));
        Assertions.assertEquals(1048576L, LanceConf.arrowMemoryLimitBytes(
                new LanceTestContext().withConf(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES, " 1048576 ")));
        // Blank means unset.
        Assertions.assertEquals(LanceConf.DEFAULT_ARROW_MEMORY_LIMIT_BYTES, LanceConf.arrowMemoryLimitBytes(
                new LanceTestContext().withConf(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES, "  ")));
    }

    @Test
    void theMemoryLimitMustBeAPositiveNumber() {
        Assertions.assertEquals(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES + " must be positive",
                Assertions.assertThrows(IllegalArgumentException.class, () -> LanceConf.arrowMemoryLimitBytes(
                        new LanceTestContext().withConf(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES, "0")))
                        .getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceConf.arrowMemoryLimitBytes(
                new LanceTestContext().withConf(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES, "256MB")));
    }

    /** The key was an fe.conf @ConfField before Lance became a plugin; an environment carrying it is ignored. */
    @Test
    void feConfIsNotConsulted() {
        LanceTestContext context = new LanceTestContext() {
            @Override
            public java.util.Map<String, String> getEnvironment() {
                return java.util.Collections.singletonMap(LanceConf.CONF_ARROW_MEMORY_LIMIT_BYTES, "1");
            }
        };
        Assertions.assertEquals(LanceConf.DEFAULT_ARROW_MEMORY_LIMIT_BYTES, LanceConf.arrowMemoryLimitBytes(context));
    }
}
