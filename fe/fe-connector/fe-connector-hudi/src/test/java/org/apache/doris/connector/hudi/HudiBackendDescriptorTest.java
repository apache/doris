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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests the BE-facing surface for hudi tables:
 * <ul>
 *   <li>{@code buildTableDescriptor} -> TTableType.HIVE_TABLE + THiveTable (legacy hudi rode HIVE_TABLE; without
 *       this the SPI default null degrades BE to a generic SchemaTableDescriptor).</li>
 *   <li>{@code getScanNodeProperties} storage -> BE-canonical creds (for the native FILE_S3 reader) + the
 *       hadoop-format passthrough (for the Hudi JNI reader), the legacy getLocationProperties dual merge.</li>
 * </ul>
 */
public class HudiBackendDescriptorTest {

    @Test
    public void buildTableDescriptorIsHiveTable() {
        HudiConnectorMetadata md = new HudiConnectorMetadata(null, Collections.emptyMap());

        TTableDescriptor desc = md.buildTableDescriptor(null, 7L, "t", "db", "t", 3, 100L);

        Assertions.assertEquals(TTableType.HIVE_TABLE, desc.getTableType(),
                "legacy hudi rides HIVE_TABLE; a SCHEMA_TABLE default would build the wrong BE descriptor");
        Assertions.assertNotNull(desc.getHiveTable(), "the HIVE_TABLE descriptor must carry a THiveTable");
        Assertions.assertEquals("db", desc.getHiveTable().getDbName());
        Assertions.assertEquals("t", desc.getHiveTable().getTableName());
    }

    @Test
    public void scanNodePropertiesEmitsCanonicalCredsAndHadoopPassthrough() {
        // A private-bucket read needs BOTH: BE-canonical AWS_* (native FILE_S3 reader) from the context hook, and
        // the hadoop fs.s3a.* passthrough (Hudi JNI reader). The old code emitted only the raw passthrough, so the
        // native reader had no usable creds (403).
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("fs.s3a.access.key", "hadoopAK");
        HudiScanPlanProvider provider = new HudiScanPlanProvider(
                catalogProps, contextWithBackendProps(Collections.singletonMap("AWS_ACCESS_KEY", "canonAK")));

        Map<String, String> result = provider.getScanNodeProperties(
                null, new HudiTableHandle("db", "t", "s3://b/t", "COPY_ON_WRITE"),
                Collections.emptyList(), Optional.empty());

        Assertions.assertEquals("canonAK", result.get("location.AWS_ACCESS_KEY"),
                "BE-canonical creds must be emitted for the native reader");
        Assertions.assertEquals("hadoopAK", result.get("location.fs.s3a.access.key"),
                "the hadoop passthrough must be emitted for the JNI reader");
    }

    @Test
    public void scanNodePropertiesWithoutContextStillEmitsHadoopPassthrough() {
        // Offline / credential-less warehouse: no context -> no canonical overlay, but the hadoop passthrough
        // still flows (so a public bucket / HDFS still reads).
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("fs.s3a.access.key", "hadoopAK");
        HudiScanPlanProvider provider = new HudiScanPlanProvider(catalogProps, null);

        Map<String, String> result = provider.getScanNodeProperties(
                null, new HudiTableHandle("db", "t", "s3://b/t", "COPY_ON_WRITE"),
                Collections.emptyList(), Optional.empty());

        Assertions.assertFalse(result.containsKey("location.AWS_ACCESS_KEY"),
                "no context must not synthesize canonical creds");
        Assertions.assertEquals("hadoopAK", result.get("location.fs.s3a.access.key"));
    }

    private static ConnectorContext contextWithBackendProps(Map<String, String> backendProps) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public Map<String, String> getBackendStorageProperties() {
                return backendProps;
            }
        };
    }
}
