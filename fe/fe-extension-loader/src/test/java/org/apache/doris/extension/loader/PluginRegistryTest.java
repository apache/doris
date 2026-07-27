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

import org.apache.doris.extension.loader.PluginRegistry.PluginRecord;
import org.apache.doris.extension.loader.PluginRegistry.PluginSource;
import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class PluginRegistryTest {

    @BeforeEach
    void setUp() {
        PluginRegistry.getInstance().clearForTest();
    }

    @AfterEach
    void tearDown() {
        PluginRegistry.getInstance().clearForTest();
    }

    @Test
    void testRegisterAndList() {
        Assertions.assertTrue(PluginRegistry.getInstance()
                .register("AUTHENTICATION", "ldap", "1.2.0", "LDAP authentication", PluginSource.EXTERNAL));

        List<PluginRecord> records = PluginRegistry.getInstance().list();
        Assertions.assertEquals(1, records.size());
        PluginRecord record = records.get(0);
        Assertions.assertEquals("AUTHENTICATION", record.getType());
        Assertions.assertEquals("ldap", record.getName());
        Assertions.assertEquals("1.2.0", record.getVersion());
        Assertions.assertEquals("LDAP authentication", record.getDescription());
        Assertions.assertEquals(PluginSource.EXTERNAL, record.getSource());
        Assertions.assertNotNull(record.getLoadTime());
    }

    @Test
    void testDuplicateKeyRejectedFirstWins() {
        Assertions.assertTrue(PluginRegistry.getInstance()
                .register("AUTHORIZATION", "ranger", "1.0", "builtin", PluginSource.BUILTIN));
        Assertions.assertFalse(PluginRegistry.getInstance()
                .register("AUTHORIZATION", "ranger", "9.9", "impostor", PluginSource.EXTERNAL));

        List<PluginRecord> records = PluginRegistry.getInstance().list();
        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(PluginSource.BUILTIN, records.get(0).getSource());
        Assertions.assertEquals("1.0", records.get(0).getVersion());
    }

    @Test
    void testSameNameAcrossFamiliesIsLegal() {
        Assertions.assertTrue(PluginRegistry.getInstance()
                .register("AUTHENTICATION", "ranger", null, "", PluginSource.EXTERNAL));
        Assertions.assertTrue(PluginRegistry.getInstance()
                .register("AUTHORIZATION", "ranger", null, "", PluginSource.EXTERNAL));

        Assertions.assertEquals(2, PluginRegistry.getInstance().list().size());
    }

    @Test
    void testInvalidNameRejected() {
        PluginRegistry registry = PluginRegistry.getInstance();
        Assertions.assertFalse(registry.register("FILESYSTEM", null, null, "", PluginSource.BUILTIN));
        Assertions.assertFalse(registry.register("FILESYSTEM", "   ", null, "", PluginSource.BUILTIN));
        Assertions.assertFalse(registry.register("FILESYSTEM", "bad name", null, "", PluginSource.BUILTIN));
        StringBuilder tooLong = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            tooLong.append('a');
        }
        Assertions.assertFalse(registry.register("FILESYSTEM", tooLong.toString(), null, "", PluginSource.BUILTIN));
        Assertions.assertTrue(registry.list().isEmpty());
    }

    @Test
    void testNameTrimmedBeforeKeying() {
        Assertions.assertTrue(PluginRegistry.getInstance()
                .register("CONNECTOR", " hive ", null, "", PluginSource.BUILTIN));
        Assertions.assertFalse(PluginRegistry.getInstance()
                .register("CONNECTOR", "hive", null, "", PluginSource.EXTERNAL));
        Assertions.assertEquals("hive", PluginRegistry.getInstance().list().get(0).getName());
    }

    @Test
    void testListSortedByTypeThenName() {
        PluginRegistry registry = PluginRegistry.getInstance();
        registry.register("FILESYSTEM", "s3", null, "", PluginSource.BUILTIN);
        registry.register("AUTHENTICATION", "oidc", null, "", PluginSource.BUILTIN);
        registry.register("AUTHENTICATION", "ldap", null, "", PluginSource.BUILTIN);

        List<PluginRecord> records = registry.list();
        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals("ldap", records.get(0).getName());
        Assertions.assertEquals("oidc", records.get(1).getName());
        Assertions.assertEquals("s3", records.get(2).getName());
    }

    @Test
    void testRegisterBuiltinSnapshotsFactoryMetadata() {
        Assertions.assertTrue(PluginRegistry.getInstance().registerBuiltin("LINEAGE", new PluginFactory() {
            @Override
            public String name() {
                return "console";
            }

            @Override
            public String description() {
                return "Console lineage sink";
            }

            @Override
            public Plugin create() {
                return null;
            }
        }));

        PluginRecord record = PluginRegistry.getInstance().list().get(0);
        Assertions.assertEquals("LINEAGE", record.getType());
        Assertions.assertEquals("console", record.getName());
        Assertions.assertEquals("Console lineage sink", record.getDescription());
        Assertions.assertEquals(PluginSource.BUILTIN, record.getSource());
    }
}
