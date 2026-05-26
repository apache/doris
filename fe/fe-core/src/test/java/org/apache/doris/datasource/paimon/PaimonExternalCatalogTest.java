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

package org.apache.doris.datasource.paimon;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class PaimonExternalCatalogTest {
    private static final String DRIVER_BYTES = "paimon-jdbc-driver";
    private static final String DRIVER_CHECKSUM = "fcf85686ee55f29a45de9c562410d5e4";

    @Test
    public void testCheckWhenCreatingAddsJdbcDriverChecksum() throws Exception {
        runWithChecksumEnabled(() -> runWithDriverUrl(driverUrl -> {
            PaimonExternalCatalog catalog = createPaimonJdbcCatalog(driverUrl);

            catalog.checkWhenCreating();

            Assert.assertEquals(DRIVER_CHECKSUM, catalog.getCatalogProperty().getProperties()
                    .get(PaimonJdbcMetaStoreProperties.PAIMON_JDBC_DRIVER_CHECKSUM));
        }));
    }

    @Test
    public void testCheckWhenCreatingRejectsJdbcDriverChecksumMismatch() throws Exception {
        runWithChecksumEnabled(() -> runWithDriverUrl(driverUrl -> {
            PaimonExternalCatalog catalog = createPaimonJdbcCatalog(driverUrl);
            catalog.getCatalogProperty().addProperty(PaimonJdbcMetaStoreProperties.PAIMON_JDBC_DRIVER_CHECKSUM,
                    "bad-checksum");

            DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkWhenCreating);
            Assert.assertTrue(exception.getMessage().contains("does not match the computed checksum"));
        }));
    }

    private PaimonExternalCatalog createPaimonJdbcCatalog(String driverUrl) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put(PaimonExternalCatalog.PAIMON_CATALOG_TYPE, PaimonExternalCatalog.PAIMON_JDBC);
        props.put("uri", "jdbc:postgresql://127.0.0.1:5442/postgres");
        props.put("warehouse", "s3://warehouse/path");
        props.put(PaimonJdbcMetaStoreProperties.PAIMON_JDBC_DRIVER_URL, driverUrl);
        props.put(PaimonJdbcMetaStoreProperties.PAIMON_JDBC_DRIVER_CLASS, "org.postgresql.Driver");
        return new PaimonExternalCatalog(1L, "paimon_catalog", null, props, "");
    }

    private void runWithChecksumEnabled(ThrowingRunnable runnable) throws Exception {
        synchronized (FeConstants.class) {
            boolean oldRunningUnitTest = FeConstants.runningUnitTest;
            FeConstants.runningUnitTest = false;
            try {
                runnable.run();
            } finally {
                FeConstants.runningUnitTest = oldRunningUnitTest;
            }
        }
    }

    private void runWithDriverUrl(ThrowingConsumer<String> consumer) throws Exception {
        Path driverPath = Files.createTempFile("paimon-jdbc-driver", ".jar");
        try {
            Files.write(driverPath, DRIVER_BYTES.getBytes(StandardCharsets.UTF_8));
            consumer.accept(driverPath.toUri().toString());
        } finally {
            Files.deleteIfExists(driverPath);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }
}
