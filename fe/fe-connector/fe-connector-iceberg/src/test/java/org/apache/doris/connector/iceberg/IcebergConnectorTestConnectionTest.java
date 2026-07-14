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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IcebergConnector#testConnection}. These cover the deterministic pieces
 * that the CREATE CATALOG connectivity regression relies on:
 * <ul>
 *   <li>the failure-message wording (must contain the exact substrings the regression matches on),</li>
 *   <li>the s3:// location normalization used to pick a storage probe target, and</li>
 *   <li>that a catalog with nothing to probe returns success without any network access.</li>
 * </ul>
 * The failing meta/storage probes themselves require a live REST/MinIO endpoint and are exercised
 * by the {@code test_iceberg_rest_minio_connectivity} regression suite.
 */
public class IcebergConnectorTestConnectionTest {

    private static final ConnectorContext CTX = new ConnectorContext() {
        @Override
        public String getCatalogName() {
            return "test_iceberg";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }
    };

    @Test
    public void metaFailureMessageContainsRequiredSubstrings() {
        // The regression asserts the CREATE CATALOG error contains BOTH "Iceberg REST" and the
        // lowercase phrase "connectivity test failed"; if either drops the error is unactionable.
        String msg = IcebergConnector.metaFailureMessage("rest",
                new RuntimeException("Connection refused"));
        Assertions.assertTrue(msg.contains("Iceberg REST"), msg);
        Assertions.assertTrue(msg.contains("connectivity test failed"), msg);
        Assertions.assertTrue(msg.contains("Connection refused"), msg);
    }

    @Test
    public void storageFailureMessageContainsRequiredSubstring() {
        String msg = IcebergConnector.storageFailureMessage(
                new RuntimeException("Access Denied"));
        Assertions.assertTrue(msg.contains("connectivity test failed"), msg);
        Assertions.assertTrue(msg.contains("Access Denied"), msg);
    }

    @Test
    public void rootCauseMessageUnwrapsNestedCause() {
        Throwable root = new IllegalStateException("181812 is out of range");
        Throwable wrapped = new RuntimeException("wrapper", new RuntimeException("mid", root));
        Assertions.assertEquals("181812 is out of range",
                IcebergConnector.rootCauseMessage(wrapped));
        // Falls back to the class name when the root cause has no message.
        Assertions.assertEquals("NullPointerException",
                IcebergConnector.rootCauseMessage(new NullPointerException()));
    }

    @Test
    public void toS3LocationNormalizesAndFilters() {
        Assertions.assertEquals("s3://bucket/warehouse",
                IcebergConnector.toS3Location("s3a://bucket/warehouse"));
        Assertions.assertEquals("s3://bucket/warehouse",
                IcebergConnector.toS3Location("s3n://bucket/warehouse"));
        Assertions.assertEquals("s3://bucket/warehouse",
                IcebergConnector.toS3Location("  s3://bucket/warehouse  "));
        // Non-s3 warehouse names (e.g. a Polaris catalog name) are not probeable.
        Assertions.assertNull(IcebergConnector.toS3Location("doris_test"));
        Assertions.assertNull(IcebergConnector.toS3Location(null));
    }

    /**
     * Pins the metastore-probe scope to what the legacy fe-core coordinator probed: it built a
     * MetaConnectivityTester for Iceberg HMS / Glue / REST / S3Tables only. Filesystem-backed catalogs
     * (hadoop) got the no-op default. Widening this set would fail CREATE CATALOG for a hadoop catalog
     * whose warehouse is not yet reachable — a behavior change, not a parity restore.
     */
    @Test
    public void probesMetastoreOnlyForRemoteMetastoreTypes() {
        Assertions.assertTrue(IcebergConnector.probesMetastore(IcebergConnectorProperties.TYPE_REST));
        Assertions.assertTrue(IcebergConnector.probesMetastore(IcebergConnectorProperties.TYPE_HMS));
        Assertions.assertTrue(IcebergConnector.probesMetastore(IcebergConnectorProperties.TYPE_GLUE));
        Assertions.assertTrue(IcebergConnector.probesMetastore(IcebergConnectorProperties.TYPE_S3_TABLES));
        Assertions.assertFalse(IcebergConnector.probesMetastore(IcebergConnectorProperties.TYPE_HADOOP));
        Assertions.assertFalse(IcebergConnector.probesMetastore(""));
    }

    /** An HMS-backed catalog must name HMS in the failure, which is what the CREATE CATALOG regression asserts. */
    @Test
    public void metaFailureMessageTagsTheCatalogType() {
        String msg = IcebergConnector.metaFailureMessage(IcebergConnectorProperties.TYPE_HMS,
                new RuntimeException("connection refused"));
        Assertions.assertTrue(msg.contains("Iceberg HMS"), msg);
        Assertions.assertTrue(msg.contains("connectivity test failed"), msg);
        // A blank type must not produce a doubled space in the tag.
        Assertions.assertTrue(IcebergConnector.metaFailureMessage("", new RuntimeException("boom"))
                .startsWith("Iceberg connectivity test failed"));
    }

    @Test
    public void testConnectionSucceedsWhenNothingToProbe() {
        // Filesystem-backed (hadoop) catalog with no S3 credentials: the meta probe is skipped (see
        // probesMetastore) and the storage probe is skipped (no s3.* creds), so testConnection
        // succeeds without any I/O.
        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE,
                IcebergConnectorProperties.TYPE_HADOOP);
        try (IcebergConnector connector = new IcebergConnector(props, CTX)) {
            ConnectorTestResult result = connector.testConnection(null);
            Assertions.assertTrue(result.isSuccess(), result.getMessage());
        } catch (Exception e) {
            throw new AssertionError("close() should not fail", e);
        }
    }
}
