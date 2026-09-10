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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Real-instance coverage for the index admission additions of {@link LanceExternalCatalog}: the
 * local admission epoch, the sanitized failure wrapper around the admission snapshot read, and
 * the REST rejection that fires before any catalog initialization or object storage access.
 */
public class LanceExternalCatalogIndexAdmissionTest {

    @Test
    public void testIndexTargetVersionStartsAtZeroAndAdvancesByOne() {
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                300, "lance_epoch", null, Collections.emptyMap(), "");

        Assertions.assertEquals(0L, catalog.getIndexTargetVersion());
        catalog.advanceIndexTargetVersion();
        Assertions.assertEquals(1L, catalog.getIndexTargetVersion());
        catalog.advanceIndexTargetVersion();
        Assertions.assertEquals(2L, catalog.getIndexTargetVersion());
    }

    @Test
    public void testAdmissionSnapshotLoadFailureSanitizesCredentials() {
        String accessKey = "sentinel-admission-access-key";
        String secretKey = "sentinel-admission-secret-key";
        String datasetUri = "s3://" + accessKey + ":" + secretKey + "@bucket/private/table.lance";
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "lance");
        catalogProperties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE,
                LanceExternalCatalog.LANCE_FILESYSTEM);
        catalogProperties.put(LanceExternalCatalog.WAREHOUSE, "/unused/lance-warehouse");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                301, "lance_admission_failure", null, catalogProperties, "");
        Map<String, String> runtimeStorageOptions = new HashMap<>();
        runtimeStorageOptions.put("aws_access_key_id", accessKey);
        runtimeStorageOptions.put("aws_secret_access_key", secretKey);
        RuntimeException providerFailure = new RuntimeException("dataset open failed for "
                + datasetUri + " with access=" + accessKey + " secret=" + secretKey);

        RuntimeException exposed = catalog.indexAdmissionSnapshotLoadFailure(
                "db", "tbl", providerFailure, datasetUri, runtimeStorageOptions);

        Assertions.assertTrue(exposed.getMessage().startsWith(
                "Failed to load Lance index admission snapshot for db.tbl:"), exposed.getMessage());
        for (String sentinel : Arrays.asList(accessKey, secretKey, datasetUri)) {
            Assertions.assertFalse(exposed.getMessage().contains(sentinel), exposed.getMessage());
            Assertions.assertFalse(exposed.getCause().getMessage().contains(sentinel),
                    exposed.getCause().getMessage());
        }
        Assertions.assertNotSame(providerFailure, exposed.getCause());
        Assertions.assertEquals(RuntimeException.class, exposed.getCause().getClass());
    }

    @Test
    public void testAdmissionSnapshotLoadFailurePreservesIllegalArgumentCauseType() {
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                302, "lance_admission_argument", null, Collections.emptyMap(), "");
        IllegalArgumentException providerFailure = new IllegalArgumentException("bad request");

        RuntimeException exposed = catalog.indexAdmissionSnapshotLoadFailure(
                "db", "tbl", providerFailure, null, null);

        Assertions.assertTrue(exposed.getCause() instanceof IllegalArgumentException);
        Assertions.assertNotSame(providerFailure, exposed.getCause());
    }

    @Test
    public void testLoadTableIndexAdmissionSnapshotRejectsRestCatalogBeforeInit() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, "http://127.0.0.1:1/");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                303, "lance_rest_admission", null, properties, "");

        Assertions.assertFalse(catalog.isInitialized());
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> catalog.loadTableIndexAdmissionSnapshot("db", "tbl"));
        Assertions.assertEquals("Lance index admission is not supported for Lance REST catalogs",
                exception.getDetailMessage());
        Assertions.assertFalse(catalog.isInitialized());
    }
}
