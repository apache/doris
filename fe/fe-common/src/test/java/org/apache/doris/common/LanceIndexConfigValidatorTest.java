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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Coverage for the Lance index admission configuration items in {@link Config} and the
 * positive-value validators in {@link LanceIndexConfigValidator}: reviewed defaults, the
 * {@link ConfigBase.ConfField} wiring (mutable/masterOnly/callback/varType), rejection of
 * zero, negative and non-numeric values, and that an accepted value is really assigned to
 * the field (a bare validating handler that never assigns would let ADMIN SET pass without
 * any effect).
 */
public class LanceIndexConfigValidatorTest {

    @Test
    public void testReviewedDefaults() {
        Assertions.assertFalse(Config.enable_lance_index_mutation);
        Assertions.assertEquals(8L, Config.lance_index_job_max_unresolved_per_table);
        Assertions.assertEquals(64L, Config.lance_index_job_max_unresolved_per_catalog);
        Assertions.assertEquals(256L, Config.lance_index_job_max_unresolved_global);
        Assertions.assertEquals(4096, Config.lance_index_max_num_partitions);
        Assertions.assertEquals(256, Config.lance_index_max_num_sub_vectors);
    }

    @Test
    public void testConfFieldWiring() throws Exception {
        assertCallbackWiring("lance_index_job_max_unresolved_per_table", true,
                LanceIndexConfigValidator.PositiveLongConfigHandler.class);
        assertCallbackWiring("lance_index_job_max_unresolved_per_catalog", true,
                LanceIndexConfigValidator.PositiveLongConfigHandler.class);
        assertCallbackWiring("lance_index_job_max_unresolved_global", true,
                LanceIndexConfigValidator.PositiveLongConfigHandler.class);
        assertCallbackWiring("lance_index_max_num_partitions", false,
                LanceIndexConfigValidator.PositiveIntConfigHandler.class);
        assertCallbackWiring("lance_index_max_num_sub_vectors", false,
                LanceIndexConfigValidator.PositiveIntConfigHandler.class);

        ConfigBase.ConfField gate = Config.class.getField("enable_lance_index_mutation")
                .getAnnotation(ConfigBase.ConfField.class);
        Assertions.assertNotNull(gate);
        Assertions.assertTrue(gate.mutable());
        Assertions.assertTrue(gate.masterOnly());
        Assertions.assertEquals(VariableAnnotation.EXPERIMENTAL, gate.varType());
    }

    private static void assertCallbackWiring(String fieldName, boolean masterOnly, Class<?> callback)
            throws Exception {
        ConfigBase.ConfField anno = Config.class.getField(fieldName).getAnnotation(ConfigBase.ConfField.class);
        Assertions.assertNotNull(anno, fieldName);
        Assertions.assertTrue(anno.mutable(), fieldName);
        Assertions.assertEquals(masterOnly, anno.masterOnly(), fieldName);
        Assertions.assertEquals(callback, anno.callback(), fieldName);
    }

    @Test
    public void testPositiveLongHandlerAssignsAcceptedValue() throws Exception {
        assertLongAssigns("lance_index_job_max_unresolved_per_table");
        assertLongAssigns("lance_index_job_max_unresolved_per_catalog");
        assertLongAssigns("lance_index_job_max_unresolved_global");
    }

    private static void assertLongAssigns(String fieldName) throws Exception {
        Field field = Config.class.getField(fieldName);
        long original = field.getLong(null);
        try {
            new LanceIndexConfigValidator.PositiveLongConfigHandler().handle(field, " 12345 ");
            Assertions.assertEquals(12345L, field.getLong(null), fieldName);
        } finally {
            field.setLong(null, original);
        }
    }

    @Test
    public void testPositiveLongHandlerRejectsInvalidValues() throws Exception {
        assertLongRejected("lance_index_job_max_unresolved_per_table", "0");
        assertLongRejected("lance_index_job_max_unresolved_per_catalog", "0");
        assertLongRejected("lance_index_job_max_unresolved_global", "0");
        assertLongRejected("lance_index_job_max_unresolved_per_table", "-8");
        assertLongRejected("lance_index_job_max_unresolved_per_table", "not-a-number");
        assertLongRejected("lance_index_job_max_unresolved_per_table", "");
    }

    private static void assertLongRejected(String fieldName, String value) throws Exception {
        Field field = Config.class.getField(fieldName);
        long original = field.getLong(null);
        try {
            ConfigException e = Assertions.assertThrows(ConfigException.class,
                    () -> new LanceIndexConfigValidator.PositiveLongConfigHandler().handle(field, value),
                    fieldName + " <- \"" + value + "\"");
            Assertions.assertTrue(e.getMessage().contains(fieldName), e.getMessage());
            Assertions.assertEquals(original, field.getLong(null), fieldName + " must remain unchanged");
        } finally {
            field.setLong(null, original);
        }
    }

    @Test
    public void testPositiveIntHandlerAssignsAcceptedValue() throws Exception {
        assertIntAssigns("lance_index_max_num_partitions");
        assertIntAssigns("lance_index_max_num_sub_vectors");
    }

    private static void assertIntAssigns(String fieldName) throws Exception {
        Field field = Config.class.getField(fieldName);
        int original = field.getInt(null);
        try {
            new LanceIndexConfigValidator.PositiveIntConfigHandler().handle(field, " 777 ");
            Assertions.assertEquals(777, field.getInt(null), fieldName);
        } finally {
            field.setInt(null, original);
        }
    }

    @Test
    public void testPositiveIntHandlerRejectsInvalidValues() throws Exception {
        assertIntRejected("lance_index_max_num_partitions", "0");
        assertIntRejected("lance_index_max_num_sub_vectors", "0");
        assertIntRejected("lance_index_max_num_partitions", "-4096");
        assertIntRejected("lance_index_max_num_partitions", "1.5");
        assertIntRejected("lance_index_max_num_sub_vectors", "abc");
    }

    private static void assertIntRejected(String fieldName, String value) throws Exception {
        Field field = Config.class.getField(fieldName);
        int original = field.getInt(null);
        try {
            ConfigException e = Assertions.assertThrows(ConfigException.class,
                    () -> new LanceIndexConfigValidator.PositiveIntConfigHandler().handle(field, value),
                    fieldName + " <- \"" + value + "\"");
            Assertions.assertTrue(e.getMessage().contains(fieldName), e.getMessage());
            Assertions.assertEquals(original, field.getInt(null), fieldName + " must remain unchanged");
        } finally {
            field.setInt(null, original);
        }
    }

    /**
     * End-to-end through the ADMIN SET FRONTEND CONFIG machinery: the annotation callback
     * must both validate and assign, and a rejected value must leave the field untouched.
     */
    @Test
    public void testSetMutableConfigPath() throws Exception {
        Config config = new Config();
        Path tempFile = Files.createTempFile("fe_ut_lance_config_", ".conf");
        tempFile.toFile().deleteOnExit();
        config.init(tempFile.toAbsolutePath().toString());

        long originalQuota = Config.lance_index_job_max_unresolved_per_catalog;
        int originalBound = Config.lance_index_max_num_partitions;
        boolean originalGate = Config.enable_lance_index_mutation;
        try {
            ConfigBase.setMutableConfig("lance_index_job_max_unresolved_per_catalog", "96");
            Assertions.assertEquals(96L, Config.lance_index_job_max_unresolved_per_catalog);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("lance_index_job_max_unresolved_per_catalog", "0"));
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("lance_index_job_max_unresolved_per_catalog", "xyz"));
            Assertions.assertEquals(96L, Config.lance_index_job_max_unresolved_per_catalog);

            ConfigBase.setMutableConfig("lance_index_max_num_partitions", "8192");
            Assertions.assertEquals(8192, Config.lance_index_max_num_partitions);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("lance_index_max_num_partitions", "-1"));
            Assertions.assertEquals(8192, Config.lance_index_max_num_partitions);

            ConfigBase.setMutableConfig("enable_lance_index_mutation", "true");
            Assertions.assertTrue(Config.enable_lance_index_mutation);
        } finally {
            Config.lance_index_job_max_unresolved_per_catalog = originalQuota;
            Config.lance_index_max_num_partitions = originalBound;
            Config.enable_lance_index_mutation = originalGate;
        }
    }
}
