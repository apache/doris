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

package org.apache.doris.tablefunction;

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TBrokerFileStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HadoopTvfReadAdmissionTest {
    private boolean originalEnable;
    private boolean originalUnitTest;

    @BeforeEach
    void setUp() {
        originalEnable = Config.enable_local_tvf;
        originalUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
    }

    @AfterEach
    void tearDown() {
        Config.enable_local_tvf = originalEnable;
        FeConstants.runningUnitTest = originalUnitTest;
    }

    @Test
    void hdfsAndFileRejectLocalRoutesBeforeFilesystemCreation() {
        Map<String, String> properties = properties("local");
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> new HdfsTableValuedFunction(properties));
                Assertions.assertThrows(AnalysisException.class,
                        () -> new FileTableValuedFunction(properties));
                factory.verifyNoInteractions();
            }
        }
    }

    @Test
    void remoteReadAdmissionAllowsTheExistingListingPath() throws Exception {
        Config.enable_local_tvf = false;
        HdfsTableValuedFunction tvf = new ListedHdfsTvf(properties("remote"), false);
        Assertions.assertEquals(1, tvf.getFileStatuses().size());
        Assertions.assertEquals("viewfs://review/remote/listed.csv", tvf.getFileStatuses().get(0).getPath());
    }

    @Test
    void returnedFilePathsAreCheckedBeforeSchemaOrDataAccess() {
        Config.enable_local_tvf = true;
        Assertions.assertThrows(AnalysisException.class,
                () -> new ListedHdfsTvf(properties("remote"), true));
    }

    @Test
    void fileKeepsTheBuiltInOssHdfsStorageSelection() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_local_tvf = false;
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "oss://bucket/file.csv");
        properties.put("fs.defaultFS", "oss://bucket");
        properties.put("fs.oss-hdfs.support", "true");
        properties.put("oss.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        properties.put("oss.access_key", "test-access-key");
        properties.put("oss.secret_key", "test-secret-key");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        FileTableValuedFunction file = new FileTableValuedFunction(properties);
        Assertions.assertEquals(org.apache.doris.thrift.TFileType.FILE_HDFS, file.getTFileType());
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem",
                file.getBackendConnectProperties().get("fs.oss.impl"));
        S3TableValuedFunction s3 = new S3TableValuedFunction(properties);
        Assertions.assertEquals("com.aliyun.jindodata.oss.JindoOssFileSystem",
                s3.getBackendConnectProperties().get("fs.oss.impl"));
    }

    @Test
    void listingPathTrimmingCannotSelectAnUncheckedLocalMount() {
        Config.enable_local_tvf = false;
        Map<String, String> properties = properties("local");
        properties.put("uri", "viewfs://review/local/file.csv ");
        properties.put("fs.viewfs.mounttable.review.link./local/file.csv /", "hdfs://remote/base");
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            Assertions.assertThrows(AnalysisException.class, () -> new HdfsTableValuedFunction(properties));
            Assertions.assertThrows(AnalysisException.class, () -> new FileTableValuedFunction(properties));
            factory.verifyNoInteractions();
        }
    }

    @Test
    void s3CsvWithHadoopStorageRejectsLocalRoutesBeforeListing() {
        Map<String, String> properties = properties("local");
        properties.put("fs.hdfs.support", "true");
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> new ListedS3Tvf(properties, "local"));
                factory.verifyNoInteractions();
            }
        }
    }

    @Test
    void s3HadoopProvidedFilesAreCheckedInBothConstructorBranches() {
        Config.enable_local_tvf = true;
        Map<String, String> properties = properties("remote");
        properties.put("fs.hdfs.support", "true");
        TBrokerFileStatus local = new TBrokerFileStatus("viewfs://review/local/file.csv", false, 10, true);
        for (boolean unitTest : new boolean[] {false, true}) {
            FeConstants.runningUnitTest = unitTest;
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> new S3TableValuedFunction(properties, Collections.singletonList(local)));
                factory.verifyNoInteractions();
            }
        }
    }

    @Test
    void s3HadoopRemoteCsvKeepsDeclaredSchemaAndHdfsRanges() throws Exception {
        Config.enable_local_tvf = false;
        Map<String, String> properties = properties("remote");
        properties.put("fs.hdfs.support", "true");
        S3TableValuedFunction tvf = new ListedS3Tvf(properties, "remote");
        Assertions.assertEquals("id", tvf.getTableColumns().get(0).getName());
        Assertions.assertEquals(1, tvf.getFileStatuses().size());
        String path = tvf.getFileStatuses().get(0).getPath();
        Assertions.assertEquals("viewfs://review/remote/listed.csv", path);
        Assertions.assertEquals(org.apache.doris.thrift.TFileType.FILE_HDFS,
                org.apache.doris.common.util.LocationPath.getTFileTypeForBE(path));
    }

    @Test
    void explicitRemoteFallbackKeepsCrossMountGlobAdmission() throws Exception {
        Config.enable_local_tvf = false;
        Map<String, String> properties = properties("remote");
        properties.put("uri", "viewfs://review/*/*.csv");
        properties.remove("fs.viewfs.mounttable.review.link./local");
        properties.put("fs.viewfs.mount.links.as.symlinks", "false");
        properties.put("fs.viewfs.mounttable.review.linkFallback", "hdfs://remote/fallback");
        Assertions.assertEquals(1, new ListedHdfsTvf(properties, false).getFileStatuses().size());
        FeConstants.runningUnitTest = true;
        Assertions.assertDoesNotThrow(() -> new FileTableValuedFunction(properties));
    }

    @Test
    void s3LocalAdapterRejectsBeforeListingEvenWithDeclaredSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/tmp/local_gate_dir");
        properties.put("fs.local.support", "true");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                Exception error = Assertions.assertThrows(Exception.class,
                        () -> new S3TableValuedFunction(properties));
                Assertions.assertTrue(error.getMessage().contains("Use local()"), error.getMessage());
                factory.verifyNoInteractions();
            }
        }
    }

    @Test
    void s3BrokerHadoopRangesCannotUseLocalImplementation() {
        Config.enable_local_tvf = false;
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "hdfs://remote/srv/private.csv");
        properties.put("fs.defaultFS", "hdfs://remote");
        properties.put("fs.broker.support", "true");
        properties.put("broker.name", "review-broker");
        properties.put("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> new S3TableValuedFunction(properties));
            factory.verifyNoInteractions();
        }
    }

    @Test
    void declaredSchemaDoesNotOpenTheUnusedLocalDefault() throws Exception {
        Config.enable_local_tvf = false;
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "hdfs://remote/data.csv");
        properties.put("fs.defaultFS", "file:///");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        HdfsTableValuedFunction hdfs = new HdfsTableValuedFunction(properties);
        Assertions.assertEquals("id", hdfs.getTableColumns().get(0).getName());
        FileTableValuedFunction file = new FileTableValuedFunction(properties);
        Assertions.assertEquals("id", file.getTableColumns().get(0).getName());
        properties.put("fs.hdfs.support", "true");
        Assertions.assertEquals("id", new S3TableValuedFunction(properties).getTableColumns().get(0).getName());
        properties.remove("csv_schema");
        FeConstants.runningUnitTest = false;
        Assertions.assertThrows(AnalysisException.class, () -> new SchemaListedHdfsTvf(properties));
    }

    @Test
    void s3ChecksSuppliedRangeConsumersIndependentlyFromBrokerListing() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "s3://bucket/listing.csv");
        properties.put("fs.broker.support", "true");
        properties.put("broker.name", "review-broker");
        properties.put("fs.defaultFS", "hdfs://remote");
        properties.put("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        for (boolean unitTest : new boolean[] {false, true}) {
            FeConstants.runningUnitTest = unitTest;
            for (String path : new String[] {"hdfs://remote/private.csv", "file:///tmp/private.csv"}) {
                List<TBrokerFileStatus> files = Collections.singletonList(
                        new TBrokerFileStatus(path, false, 10, true));
                Assertions.assertThrows(AnalysisException.class,
                        () -> new SuppliedS3Tvf(properties, files));
            }
        }
        FeConstants.runningUnitTest = true;
        properties.put("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        S3TableValuedFunction remote = new S3TableValuedFunction(properties, Collections.singletonList(
                new TBrokerFileStatus("hdfs://remote/public.csv", false, 10, true)));
        Assertions.assertEquals(1, remote.getFileStatuses().size());
        Assertions.assertEquals("id", remote.getTableColumns().get(0).getName());
    }

    @Test
    void suppliedHadoopFilesSkipDiscoveryAndKeepTheirLiteralPaths() throws Exception {
        Map<String, String> properties = properties("local");
        properties.put("fs.hdfs.support", "true");
        String remoteLiteral = "viewfs://review/remote/{../local}/file.csv";
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            List<TBrokerFileStatus> files = Collections.singletonList(
                    new TBrokerFileStatus(remoteLiteral, false, 10, true));
            S3TableValuedFunction tvf = new SuppliedS3Tvf(properties, files);
            Assertions.assertEquals(remoteLiteral, tvf.getFileStatuses().get(0).getPath());
            List<TBrokerFileStatus> localFiles = Collections.singletonList(
                    new TBrokerFileStatus("viewfs://review/local/{../remote}/file.csv", false, 10, true));
            Assertions.assertThrows(AnalysisException.class, () -> new SuppliedS3Tvf(properties, localFiles));
        }
    }

    private static class SuppliedS3Tvf extends S3TableValuedFunction {
        SuppliedS3Tvf(Map<String, String> properties, List<TBrokerFileStatus> files)
                throws org.apache.doris.common.AnalysisException {
            super(properties, files);
        }

        @Override
        protected void parseFile(List<TBrokerFileStatus> files) {
            fileStatuses.addAll(files);
        }
    }

    @Test
    void schemaDiscoveryAlsoChecksTheReturnedFilesDefaultRoute() throws Exception {
        Config.enable_local_tvf = false;
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "hdfs://initial/data.csv");
        properties.put("fs.defaultFS", "viewfs://schema");
        properties.put("fs.viewfs.mounttable.schema.link./hdfs:/initial", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.schema.linkFallback", "file:///tmp");
        properties.put("format", "csv");
        // The fake listing separates initial/data/schema identities without filesystem IO.
        Assertions.assertThrows(AnalysisException.class, () -> new SchemaListedHdfsTvf(properties));
        properties.put("csv_schema", "id:int");
        Assertions.assertEquals("id", new SchemaListedHdfsTvf(properties).getTableColumns().get(0).getName());
    }

    private static class SchemaListedHdfsTvf extends HdfsTableValuedFunction {
        SchemaListedHdfsTvf(Map<String, String> properties) throws org.apache.doris.common.AnalysisException {
            super(properties);
        }

        @Override
        protected void parseFile() {
            fileStatuses.add(new TBrokerFileStatus("hdfs://returned/data.csv", false, 10, true));
        }
    }

    private static Map<String, String> properties(String mount) {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "viewfs://review/" + mount + "/file.csv");
        properties.put("fs.defaultFS", "viewfs://review");
        properties.put("fs.viewfs.mounttable.review.link./remote", "hdfs://remote/base");
        properties.put("fs.viewfs.mounttable.review.link./local", "file:///tmp");
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        return properties;
    }

    private static class ListedS3Tvf extends S3TableValuedFunction {
        ListedS3Tvf(Map<String, String> properties, String mount)
                throws org.apache.doris.common.AnalysisException {
            super(withListing(properties, mount));
        }

        private static Map<String, String> withListing(Map<String, String> properties, String mount) {
            properties.put("fs.test.listing", mount);
            return properties;
        }

        @Override
        protected void parseFile(List<TBrokerFileStatus> files) {
            String mount = backendConnectProperties.get("fs.test.listing");
            fileStatuses.add(new TBrokerFileStatus("viewfs://review/" + mount + "/listed.csv", false, 10, true));
        }
    }

    private static class ListedHdfsTvf extends HdfsTableValuedFunction {
        // Constructor-time listing is the existing TVF lifecycle. Select the fake listing through
        // statement properties, which are available before super() calls parseFile().
        ListedHdfsTvf(Map<String, String> properties, boolean localListing)
                throws org.apache.doris.common.AnalysisException {
            super(withListing(properties, localListing));
        }

        private static Map<String, String> withListing(Map<String, String> properties, boolean localListing) {
            properties.put("fs.test.listing", localListing ? "local" : "remote");
            return properties;
        }

        @Override
        protected void parseFile() {
            String mount = backendConnectProperties.get("fs.test.listing");
            fileStatuses.add(new TBrokerFileStatus("viewfs://review/" + mount + "/listed.csv", false, 10, true));
        }
    }
}
