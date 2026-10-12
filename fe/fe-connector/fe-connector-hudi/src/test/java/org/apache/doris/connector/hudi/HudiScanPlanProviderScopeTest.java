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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Where the scan-planning path opens its filesystems.
 *
 * <p>Hadoop caches a filesystem under the UGI current when it is opened, and that is the only handle
 * {@code HudiConnector.close()} has to close it again ({@code FileSystem.closeAllForUGI}). The engine
 * calls {@code planScan} under no {@code doAs} at all, so unless the provider itself runs its planning
 * inside the connector's execute-wrapper, every metaClient it builds caches its filesystem under the
 * FE login user - shared with every other catalog, closable by nobody, and left behind by each catalog
 * configuration that comes and goes. These cases pin the provider's half of that: the whole of
 * {@code planScan}, and the metaClient of {@code getScanNodeProperties}, run inside the executor the
 * provider was built with. {@link HudiConnectorFileSystemScopeTest} pins the connector's half, that
 * the executor it hands over is the one running under its filesystem scope.
 */
public class HudiScanPlanProviderScopeTest {

    @TempDir
    public Path warehouse;

    /**
     * Runs the action under a UGI of its own and counts. The UGI is what makes the cases observable:
     * {@link UgiRecordingLocalFileSystem} records who opened it, and a UGI nobody else has ever used
     * cannot hit a cache entry left by an earlier case.
     */
    private static final class RecordingExecutor implements HudiMetaClientExecutor {
        private final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("planning-scope");
        private int calls;

        @Override
        public <T> T execute(Callable<T> action) {
            calls++;
            try {
                return ugi.doAs((PrivilegedExceptionAction<T>) action::call);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** A {@code file://} filesystem that records the UGI it was opened under. */
    public static class UgiRecordingLocalFileSystem extends LocalFileSystem {
        static volatile UserGroupInformation openedBy;

        @Override
        public void initialize(URI name, Configuration conf) throws IOException {
            openedBy = UserGroupInformation.getCurrentUser();
            super.initialize(name, conf);
        }
    }

    @Test
    public void planScanOpensItsFilesystemsInsideTheExecutor() throws Exception {
        String basePath = emptyCopyOnWriteTable();
        RecordingExecutor executor = new RecordingExecutor();
        HudiScanPlanProvider provider = new HudiScanPlanProvider(recordingFilesystem(), null, executor);
        UgiRecordingLocalFileSystem.openedBy = null;

        List<ConnectorScanRange> ranges = provider.planScan(session(false),
                ConnectorScanRequest.builder(handle(basePath), Collections.emptyList()).build());

        Assertions.assertTrue(ranges.isEmpty(), "an empty timeline plans no splits");
        Assertions.assertEquals(1, executor.calls, "planScan is one call into the executor");
        // assertEquals, not assertSame: getCurrentUser() wraps the Subject anew on every call, and UGI
        // equality is Subject identity - which is also what Hadoop's cache key compares.
        Assertions.assertEquals(executor.ugi, UgiRecordingLocalFileSystem.openedBy,
                "the metaClient's filesystem was opened under the executor's UGI - that is the UGI the "
                        + "connector's close() releases, so this is what makes the planning path's filesystems "
                        + "closable at all");
    }

    @Test
    public void scanNodePropertiesBuildTheirMetaClientInsideTheExecutor() throws Exception {
        String basePath = emptyCopyOnWriteTable();
        RecordingExecutor executor = new RecordingExecutor();
        HudiScanPlanProvider provider = new HudiScanPlanProvider(recordingFilesystem(), null, executor);
        UgiRecordingLocalFileSystem.openedBy = null;

        provider.getScanNodeProperties(session(false), handle(basePath), Collections.emptyList(),
                Optional.empty());

        Assertions.assertEquals(1, executor.calls, "the schema-evolution dictionary's metaClient is built inside");
        Assertions.assertEquals(executor.ugi, UgiRecordingLocalFileSystem.openedBy);
    }

    /** Under force_jni no dictionary is built, so no metaClient is - and nothing goes through the executor. */
    @Test
    public void scanNodePropertiesUnderForceJniOpenNothing() throws Exception {
        String basePath = emptyCopyOnWriteTable();
        RecordingExecutor executor = new RecordingExecutor();
        HudiScanPlanProvider provider = new HudiScanPlanProvider(recordingFilesystem(), null, executor);

        provider.getScanNodeProperties(session(true), handle(basePath), Collections.emptyList(),
                Optional.empty());

        Assertions.assertEquals(0, executor.calls);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────────

    /** A hudi table with a {@code .hoodie} directory and no commits: enough for a metaClient to read. */
    private String emptyCopyOnWriteTable() throws IOException {
        String basePath = warehouse.resolve("t").toString();
        HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName("t")
                .initTable(new HadoopStorageConfiguration(new Configuration()), basePath);
        return basePath;
    }

    /** Catalog properties routing {@code file://} to the recording filesystem; buildHadoopConf copies fs.* keys. */
    private static Map<String, String> recordingFilesystem() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.file.impl", UgiRecordingLocalFileSystem.class.getName());
        return properties;
    }

    private static HudiTableHandle handle(String basePath) {
        return new HudiTableHandle.Builder("db", "t", basePath, "COPY_ON_WRITE").build();
    }

    private static ConnectorSession session(boolean forceJni) {
        Map<String, String> sessionProps = Collections.singletonMap("force_jni_scanner", String.valueOf(forceJni));
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return sessionProps;
            }
        };
    }
}
