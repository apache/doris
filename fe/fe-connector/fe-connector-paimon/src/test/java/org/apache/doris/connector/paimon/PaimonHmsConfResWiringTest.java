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

package org.apache.doris.connector.paimon;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * FIX-HMS-CONFRES: proves an external {@code hive.conf.resources} hive-site.xml actually reaches the
 * assembled {@link HiveConf} — intent: no silent drop of the operator's file (the original defect).
 *
 * <p>The connector resolves and parses the file itself ({@code PaimonCatalogFactory.addConfResources})
 * rather than receiving pre-flattened keys from an engine hook, so this asserts the REAL file->HiveConf
 * behavior instead of a mock handshake: it writes a real XML under a real directory and reads the value
 * back off the HiveConf.
 */
public class PaimonHmsConfResWiringTest {

    private static final String QOP_KEY = "hive.metastore.sasl.qop";

    /** The engine-set system property carrying {@code Config.hadoop_config_dir} into the plugin. */
    private static final String CONFIG_DIR_KEY = "doris.hadoop.config.dir";

    private static void writeHiveSite(File dst, String key, String value) throws Exception {
        Files.write(dst.toPath(), ("<?xml version=\"1.0\"?><configuration><property><name>" + key
                + "</name><value>" + value + "</value></property></configuration>")
                .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Runs {@code body} with the engine's config-dir bridge pointed at {@code dir}, restoring the previous
     * value afterwards (the property is process-global).
     */
    private static void withConfigDir(Path dir, ThrowingRunnable body) throws Exception {
        String prev = System.getProperty(CONFIG_DIR_KEY);
        System.setProperty(CONFIG_DIR_KEY, dir.toString() + File.separator);
        try {
            body.run();
        } finally {
            if (prev == null) {
                System.clearProperty(CONFIG_DIR_KEY);
            } else {
                System.setProperty(CONFIG_DIR_KEY, prev);
            }
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    public void externalHiveSiteXmlReachesTheHiveConf(@TempDir Path tmp) throws Exception {
        writeHiveSite(tmp.resolve("hive-site.xml").toFile(), QOP_KEY, "auth-conf");

        withConfigDir(tmp, () -> {
            HiveConf hc = PaimonCatalogFactory.assembleHiveConf("hive-site.xml", Collections.emptyMap());
            // WHY: a refactor that builds the HiveConf without consulting hive.conf.resources would
            // silently drop the operator's file — the very defect this path exists to fix.
            Assertions.assertEquals("auth-conf", hc.get(QOP_KEY),
                    "the external hive-site.xml named by hive.conf.resources must reach the HiveConf");
        });
    }

    @Test
    public void overridesBeatTheExternalFile(@TempDir Path tmp) throws Exception {
        writeHiveSite(tmp.resolve("hive-site.xml").toFile(), QOP_KEY, "auth-conf");

        withConfigDir(tmp, () -> {
            HiveConf hc = PaimonCatalogFactory.assembleHiveConf("hive-site.xml",
                    Collections.singletonMap(QOP_KEY, "auth"));
            // WHY (F2 ordering): the file is the BASE, the metastore-spi overrides are the connection
            // facts and must win. addResource + set() must not invert that precedence.
            Assertions.assertEquals("auth", hc.get(QOP_KEY),
                    "toHiveConfOverrides must override the external file, not the other way round");
        });
    }

    @Test
    public void missingFileFailsLoud(@TempDir Path tmp) throws Exception {
        withConfigDir(tmp, () -> {
            // WHY: a missing file must fail loud, never degrade to "connect with defaults" — the operator
            // named a file that carries connection-critical settings.
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonCatalogFactory.assembleHiveConf("absent.xml", Collections.emptyMap()));
            Assertions.assertTrue(e.getMessage().contains("Config resource file does not exist"),
                    "message must name the unresolvable file; was: " + e.getMessage());
        });
    }

    @Test
    public void blankResourcesIsANoOp() {
        // WHY: hive.conf.resources is optional; a catalog without it must not touch the filesystem or throw.
        HiveConf hc = PaimonCatalogFactory.assembleHiveConf(null, Collections.singletonMap(QOP_KEY, "auth"));
        Assertions.assertEquals("auth", hc.get(QOP_KEY));
    }
}
