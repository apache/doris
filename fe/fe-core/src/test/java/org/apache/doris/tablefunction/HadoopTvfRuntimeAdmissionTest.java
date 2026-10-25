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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.TVFTableSink;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TUniqueId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.TvfGuard;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class HadoopTvfRuntimeAdmissionTest {
    @TempDir
    java.nio.file.Path temporary;
    private boolean originalEnable;
    private boolean originalUnitTest;
    private ConnectContext originalContext;
    private final List<URLClassLoader> ownedLoaders = new ArrayList<>();

    @BeforeEach
    void setUp() {
        originalEnable = Config.enable_local_tvf;
        originalUnitTest = FeConstants.runningUnitTest;
        originalContext = ConnectContext.get();
        Config.enable_local_tvf = false;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    void tearDown() throws IOException {
        Config.enable_local_tvf = originalEnable;
        FeConstants.runningUnitTest = originalUnitTest;
        ConnectContext.remove();
        if (originalContext != null) {
            originalContext.setThreadLocalInfo();
        }
        FileSystem.closeAll();
        for (URLClassLoader loader : ownedLoaders) {
            loader.close();
        }
    }

    @Test
    void implicitLocalWriteProviderRejectsBeforeInitialization() throws Exception {
        Map<String, String> properties = hdfs("hdfs://runtime");
        Map<String, String> guarded = HadoopTvfAdmission.protectNativeWrite("/data/prefix_", properties);
        Configuration consumer = defaults(Collections.singletonMap("fs.hdfs.impl", LocalFileSystem.class.getName()));
        overlay(consumer, guarded);
        nativeRequest(consumer, "hdfs://runtime", "/data/prefix_");
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            Assertions.assertThrows(IOException.class,
                    () -> FileSystem.newInstance(URI.create("hdfs://runtime"), consumer));
            factory.verify(() -> ReflectionUtils.newInstance(LocalFileSystem.class, consumer), Mockito.never());
        }
    }

    @Test
    void implicitOverloadedReadProviderRejectsWithLocalOptInToo() throws Exception {
        Config.enable_local_tvf = true;
        HdfsTableValuedFunction tvf = new HdfsTableValuedFunction(readProperties("hdfs://runtime/local/data.csv"));
        Configuration consumer = defaults(Collections.singletonMap(
                "fs.hdfs.impl", ViewFileSystemOverloadScheme.class.getName()));
        consumer.set("fs.viewfs.mounttable.runtime.link./local", "file:///tmp");
        overlay(consumer, tvf.getBackendConnectProperties());
        nativeRequest(consumer, "hdfs://runtime", "/local/data.csv");
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            Assertions.assertThrows(IOException.class,
                    () -> FileSystem.newInstance(URI.create("hdfs://runtime"), consumer));
            factory.verify(() -> ReflectionUtils.newInstance(
                    ViewFileSystemOverloadScheme.class, consumer), Mockito.never());
        }
    }

    @Test
    void implicitChildMountRejectsBeforeViewFsInitialization() throws Exception {
        Map<String, String> properties = hdfs("viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "/data/local/sub/prefix_", properties);
        Configuration consumer = defaults(Collections.singletonMap(
                "fs.viewfs.mounttable.cluster.link./data/local", "file:///tmp"));
        overlay(consumer, HadoopTvfAdmission.protectNativeWrite("/data/local/sub/prefix_", properties));
        nativeRequest(consumer, "viewfs://cluster", "/data/local/sub/prefix_");
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            Assertions.assertThrows(IOException.class,
                    () -> FileSystem.newInstance(URI.create("viewfs://cluster"), consumer));
            factory.verify(() -> ReflectionUtils.newInstance(ViewFileSystem.class, consumer), Mockito.never());
        }
    }

    @Test
    void remoteRoutesKeepUnrelatedLocalSiblingsAndCommaDirectories() throws Exception {
        Map<String, String> properties = hdfs("viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base,archive");
        Configuration consumer = defaults(Collections.singletonMap(
                "fs.viewfs.mounttable.cluster.link./local", "file:///tmp"));
        overlay(consumer, HadoopTvfAdmission.protectNativeWrite("/data/sub/prefix_", properties));
        nativeRequest(consumer, "viewfs://cluster", "/data/sub/prefix_");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("viewfs://cluster"), consumer));
        overlay(consumer, HadoopTvfAdmission.protectNativeRead(properties));
        nativeRequest(consumer, "viewfs://cluster", "/data/file.csv");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("viewfs://cluster"), consumer));
    }

    @Test
    void actualProviderClassAndExpandedDefaultValuesAreChecked() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("runtime.provider", LocalFileSystem.class.getName());
        values.put("fs.hdfs.impl", "${runtime.provider}");
        Configuration consumer = defaults(values);
        overlay(consumer, HadoopTvfAdmission.protectNativeWrite("/data/prefix_", hdfs("hdfs://runtime")));
        nativeRequest(consumer, "hdfs://runtime", "/data/prefix_");
        Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("hdfs://runtime"), consumer));
    }

    @Test
    void unusedConfigurationValuesAreNotEagerlyExpanded() throws Exception {
        Configuration consumer = defaults(Collections.emptyMap());
        consumer.set("cycle.a", "${cycle.b}x");
        consumer.set("cycle.b", "${cycle.a}y");
        consumer.set("fs.viewfs.mounttable.unused.link./data", "${cycle.a}");
        consumer.set("unrelated.setting", "${cycle.a}");
        overlay(consumer, HadoopTvfAdmission.protectNativeWrite("/data/prefix_", hdfs("hdfs://runtime")));
        nativeRequest(consumer, "hdfs://runtime", "/data/prefix_");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("hdfs://runtime"), consumer));
        Assertions.assertThrows(IllegalStateException.class, () -> consumer.get("unrelated.setting"));
    }

    @Test
    void ordinaryUnmarkedFilesystemRetainsItsBehavior() throws Exception {
        Configuration consumer = defaults(Collections.singletonMap("fs.hdfs.impl", LocalFileSystem.class.getName()));
        try (FileSystem fs = FileSystem.newInstance(URI.create("hdfs://runtime"), consumer)) {
            Assertions.assertInstanceOf(LocalFileSystem.class, fs);
        }
        Config.enable_local_tvf = true;
        Assertions.assertFalse(HadoopTvfAdmission.protectNativeWrite("/data/prefix_", hdfs("hdfs://runtime"))
                .containsKey(TvfGuard.POLICY_KEY));
    }

    @Test
    void listingUsesJavaPolicyAndReadSenderUsesNativePolicy() throws Exception {
        HdfsTableValuedFunction tvf = new HdfsTableValuedFunction(readProperties("hdfs://runtime/data.csv"));
        Map<String, String> javaProperties = tvf.getBrokerDesc().getStorageAdapter().getBackendConfigProperties();
        Assertions.assertNotNull(javaProperties.get(TvfGuard.POLICY_KEY));
        Configuration consumer = defaults(Collections.singletonMap("fs.hdfs.impl", LocalFileSystem.class.getName()));
        overlay(consumer, javaProperties);
        Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("hdfs://runtime"), consumer));
        Assertions.assertNotNull(tvf.getBackendConnectProperties().get(TvfGuard.POLICY_KEY));
    }

    @Test
    void schemaRequestCarriesPolicyInBothPropertiesAndHdfsParams() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(new TUniqueId(1, 2));
        context.setThreadLocalInfo();
        Map<String, String> properties = readProperties("hdfs://runtime/data.csv");
        properties.remove("csv_schema");
        HdfsTableValuedFunction tvf = new HdfsTableValuedFunction(properties);
        tvf.getFileStatuses().add(new TBrokerFileStatus("hdfs://runtime/returned.csv", false, 100, true));
        Method method = ExternalFileTableValuedFunction.class.getDeclaredMethod("getFetchTableStructureRequest");
        method.setAccessible(true);
        PFetchTableSchemaRequest request = (PFetchTableSchemaRequest) method.invoke(tvf);
        TFileScanRange range = new TFileScanRange();
        new TDeserializer().deserialize(range, request.getFileScanRange().toByteArray());
        String policy = range.getParams().getProperties().get(TvfGuard.POLICY_KEY);
        Assertions.assertNotNull(policy);
        Assertions.assertTrue(range.getParams().getHdfsParams().getHdfsConf().stream()
                .anyMatch(conf -> TvfGuard.POLICY_KEY.equals(conf.getKey()) && policy.equals(conf.getValue())));
    }

    @Test
    void unusedInitialSchemaPathDoesNotRejectTheActualReturnedFile() throws Exception {
        Map<String, String> properties = hdfs("viewfs://schema");
        properties.put("uri", "hdfs://initial/input.csv");
        properties.put("format", "csv");
        properties.put("fs.viewfs.mounttable.schema.linkFallback", "hdfs://remote/base");
        HdfsTableValuedFunction tvf = new HdfsTableValuedFunction(properties);
        tvf.getFileStatuses().add(new TBrokerFileStatus("hdfs://returned/data.csv", false, 100, true));
        Configuration consumer = defaults(Collections.singletonMap(
                "fs.viewfs.mounttable.schema.link./hdfs:/initial", "file:///tmp"));
        overlay(consumer, tvf.getBackendConnectProperties());
        nativeRequest(consumer, "viewfs://schema", "hdfs://returned/data.csv");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("viewfs://schema"), consumer));
    }

    @Test
    void brokerToHadoopDataSenderCannotDropRuntimePolicy() throws Exception {
        Map<String, String> properties = readProperties("hdfs://runtime/data.csv");
        properties.put("fs.broker.support", "true");
        properties.put("broker.name", "runtime-review");
        S3TableValuedFunction tvf = new S3TableValuedFunction(properties, Collections.singletonList(
                new TBrokerFileStatus("hdfs://runtime/returned.csv", false, 100, true)));
        String policy = tvf.getBackendConnectProperties().get(TvfGuard.POLICY_KEY);
        Assertions.assertNotNull(policy);
        Configuration consumer = defaults(Collections.singletonMap("fs.hdfs.impl", LocalFileSystem.class.getName()));
        overlay(consumer, tvf.getBackendConnectProperties());
        Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("hdfs://runtime"), consumer));
    }

    @Test
    void finalSinkOverridesUserPolicyAliasesAndEnabledWritesClearThem() throws Exception {
        Map<String, String> properties = hdfs("hdfs://runtime");
        properties.put("file_path", "/data/prefix_");
        properties.put("format", "csv");
        properties.put(TvfGuard.POLICY_KEY.toUpperCase(java.util.Locale.ROOT) + "\0suffix", "attacker-value");
        properties.put(TvfGuard.POLICY_KEY, "attacker-value");
        TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties, Collections.emptyList());
        sink.bindDataSink();
        Method method = TVFTableSink.class.getDeclaredMethod("toThrift");
        method.setAccessible(true);
        TDataSink thrift = (TDataSink) method.invoke(sink);
        Map<String, String> sent = thrift.getTvfTableSink().getProperties();
        Assertions.assertNotNull(sent.get(TvfGuard.POLICY_KEY));
        Assertions.assertNotEquals("attacker-value", sent.get(TvfGuard.POLICY_KEY));
        Assertions.assertFalse(sent.containsKey(TvfGuard.POLICY_KEY.toUpperCase(java.util.Locale.ROOT) + "\0suffix"));
        Config.enable_local_tvf = true;
        sink.bindDataSink();
        sent = ((TDataSink) method.invoke(sink)).getTvfTableSink().getProperties();
        Assertions.assertFalse(sent.containsKey(TvfGuard.POLICY_KEY));
    }

    @Test
    void nativeContextChecksEachPathAgainstTheConnectionSnapshot() throws Exception {
        Configuration consumer = defaults(Collections.emptyMap());
        consumer.set("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        consumer.set("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        consumer.set(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        nativeRequest(consumer, "viewfs://cluster", "/data/a.csv");
        TvfGuard.clearNativeContext();
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(ViewFileSystem.class),
                    Mockito.any(Configuration.class))).thenReturn(Mockito.mock(ViewFileSystem.class));
            FileSystem.newInstance(URI.create("viewfs://cluster"), consumer);
            TvfGuard.Context context = TvfGuard.takeNativeContext();
            Assertions.assertNotNull(context);
            Assertions.assertNull(TvfGuard.takeNativeContext());
            Assertions.assertDoesNotThrow(() -> context.checkNative("viewfs://cluster", "/data/b.csv"));
            Assertions.assertThrows(IOException.class,
                    () -> context.checkNative("viewfs://cluster", "/local/b.csv"));
            consumer.set("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
            Assertions.assertDoesNotThrow(() -> context.checkNative("viewfs://cluster", "/data/c.csv"));
        }
    }

    @Test
    void javaReadBindsActualPathAndRejectsUnmarkedCachedInstances() throws Exception {
        Configuration consumer = defaults(Collections.emptyMap());
        consumer.set("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/base");
        consumer.set("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        Path remote = new Path("viewfs://cluster/data/a%2fb c.csv");
        Configuration frozen = TvfGuard.prepareJavaRead(consumer, remote, TvfGuard.NATIVE_POLICY);
        FileSystem actual = Mockito.mock(FileSystem.class);
        Mockito.when(actual.getConf()).thenReturn(frozen);
        Assertions.assertDoesNotThrow(() -> TvfGuard.checkJavaRead(actual, remote, TvfGuard.NATIVE_POLICY));
        Assertions.assertThrows(IOException.class, () -> TvfGuard.checkJavaRead(actual,
                new Path("viewfs://cluster/local/data.csv"), TvfGuard.NATIVE_POLICY));
        Mockito.when(actual.getConf()).thenReturn(new Configuration(frozen));
        Assertions.assertDoesNotThrow(() -> TvfGuard.checkJavaRead(actual, remote, TvfGuard.NATIVE_POLICY));
        Assertions.assertThrows(IOException.class, () -> TvfGuard.checkJavaRead(actual,
                new Path("viewfs://cluster/local/data.csv"), TvfGuard.NATIVE_POLICY));
        Mockito.when(actual.getConf()).thenReturn(consumer);
        Assertions.assertThrows(IOException.class, () -> TvfGuard.checkJavaRead(actual, remote, TvfGuard.NATIVE_POLICY));
        Assertions.assertDoesNotThrow(() -> TvfGuard.checkJavaRead(actual, remote, null));
    }

    @Test
    void actualS3BucketConfigurationRetainsTheReadGuard() throws Exception {
        Configuration consumer = defaults(Collections.emptyMap());
        Path path = new Path("s3a://bucket/data.avro");
        Configuration frozen = TvfGuard.prepareJavaRead(consumer, path, TvfGuard.NATIVE_POLICY);
        Configuration derived = org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions(frozen, "bucket");
        Assertions.assertEquals(Configuration.class, derived.getClass());
        FileSystem actual = Mockito.mock(FileSystem.class);
        Mockito.when(actual.getConf()).thenReturn(derived);
        Assertions.assertDoesNotThrow(() -> TvfGuard.checkJavaRead(actual, path, TvfGuard.NATIVE_POLICY));
        Mockito.when(actual.getConf()).thenReturn(new org.apache.hadoop.mapred.JobConf(derived));
        Assertions.assertDoesNotThrow(() -> TvfGuard.checkJavaRead(actual, path, TvfGuard.NATIVE_POLICY));
    }

    @Test
    void nativeRequestRequiresActualPathAndConstantMarkerDoesNotGrowWithFileCount() throws Exception {
        Map<String, String> properties = HadoopTvfAdmission.protectNativeRead(hdfs("hdfs://runtime"));
        Assertions.assertEquals(TvfGuard.NATIVE_POLICY, properties.get(TvfGuard.POLICY_KEY));
        Configuration consumer = defaults(Collections.emptyMap());
        overlay(consumer, properties);
        Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("hdfs://runtime"), consumer));
    }

    private static void nativeRequest(Configuration configuration, String root, String path) {
        configuration.set(TvfGuard.ROOT_KEY, root);
        configuration.set(TvfGuard.PATH_KEY, path);
    }

    @Test
    void escapedListingComponentsAreCheckedBeforeDiscovery() throws Exception {
        Map<String, String> properties = readProperties("viewfs://cluster/da\\ta/*.csv");
        properties.put("fs.defaultFS", "viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "hdfs://remote/base");
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new HdfsTableValuedFunction(properties));
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/data");
        Assertions.assertDoesNotThrow(() -> new HdfsTableValuedFunction(properties));
        Configuration actual = defaults(Collections.singletonMap(
                "fs.viewfs.mounttable.cluster.link./data", "file:///tmp"));
        properties.remove("fs.viewfs.mounttable.cluster.link./data");
        overlay(actual, HadoopTvfAdmission.protectJava(
                HadoopTvfAdmission.Access.HADOOP_READ, properties.get("uri"), properties));
        Assertions.assertThrows(IOException.class,
                () -> TvfGuard.check(URI.create("viewfs://cluster"), actual));
    }

    @Test
    void globTraversalChecksIntermediateParentsAndEscapedDotSegments() {
        Map<String, String> properties = hdfs("viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "hdfs://remote/base");
        for (String path : new String[] {"viewfs://cluster/da\\ta/*/\\.\\./other/*.csv",
                "viewfs://cluster/{da\\ta/x,remote/y}/*.csv"}) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> HadoopTvfAdmission.checkReadDiscovery(path, properties));
        }
        properties.put("fs.viewfs.mounttable.cluster.link./data", "hdfs://remote/data");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/da\\ta/\\*/file.csv", properties));
        properties.put("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.link./literal\\name", "hdfs://remote/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                "viewfs://cluster/literal\\name/file.csv", properties));
    }

    @Test
    void explicitJuiceFsUsesTheSameKnownRemoteProviderAsRuntime() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            Map<String, String> properties = readProperties("jfs://cluster/data.csv");
            properties.put("fs.defaultFS", "jfs://cluster");
            properties.put("fs.jfs.impl", "io.juicefs.JuiceFileSystem");
            properties.put("juicefs.cluster.meta", "redis://meta:6379/1");
            Assertions.assertDoesNotThrow(() -> new HdfsTableValuedFunction(properties));
            properties.put("fs.jfs.impl", LocalFileSystem.class.getName());
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> new HdfsTableValuedFunction(properties));
        }
    }

    @Test
    void secureWasbDefaultAndDelegatedRoutesKeepRemoteAdmission() throws Exception {
        Configuration direct = defaults(Collections.emptyMap());
        Assertions.assertEquals("org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure",
                FileSystem.getFileSystemClass("wasbs", direct).getName());
        direct.set(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        nativeRequest(direct, "wasbs://container@account.blob.core.windows.net", "/exports/file");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("wasbs://container@account.blob.core.windows.net"), direct));
        Configuration delegated = new Configuration(direct);
        nativeRequest(delegated, "viewfs://cluster", "/exports/file");
        delegated.set("fs.viewfs.mounttable.cluster.link./exports",
                "wasbs://container@account.blob.core.windows.net/base");
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("viewfs://cluster"), delegated));
        delegated.set("fs.wasbs.impl", LocalFileSystem.class.getName());
        Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("viewfs://cluster"), delegated));
    }

    @Test
    void guardedSnapshotDisablesOnlyViewFsInnerCache() throws Exception {
        Configuration source = defaults(Collections.emptyMap());
        source.setBoolean("fs.viewfs.enable.inner.cache", true);
        Configuration frozen = TvfGuard.prepareJavaRead(source, new Path("hdfs://runtime/data.csv"), TvfGuard.NATIVE_POLICY);
        Assertions.assertFalse(frozen.getBoolean("fs.viewfs.enable.inner.cache", true));
        Assertions.assertTrue(source.getBoolean("fs.viewfs.enable.inner.cache", false));
        Assertions.assertSame(source, TvfGuard.prepareJavaRead(source, new Path("file:///unused"), null));
    }

    @Test
    void noGlobBackslashMatchesTheBranchListingConsumer() {
        Map<String, String> properties = hdfs("viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "hdfs://remote/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery("viewfs://cluster/da\\ta/file.csv", properties));
    }

    @Test
    void nestedViewFsCaseDistinctTargetsKeepTheirActualIdentityAfterWarmup() throws Exception {
        Configuration source = defaults(Collections.emptyMap());
        source.set(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
        nativeRequest(source, "viewfs://outer", "/warm/file.csv");
        source.setBoolean("fs.viewfs.enable.inner.cache", true);
        source.set("fs.viewfs.mounttable.outer.link./warm", "viewfs://A/remote");
        source.set("fs.viewfs.mounttable.outer.link./evil", "viewfs://a/secret");
        source.set("fs.viewfs.mounttable.A.link./remote", "hdfs://nn/base");
        source.set("fs.viewfs.mounttable.A.link./secret", "file:///unused-local");
        source.set("fs.viewfs.mounttable.a.linkFallback", "hdfs://nn/lower");
        java.util.List<String> accesses = new java.util.ArrayList<>();
        LocalFileSystem local = Mockito.mock(LocalFileSystem.class);
        Mockito.when(local.getUri()).thenReturn(URI.create("file:///"));
        Mockito.when(local.getWorkingDirectory()).thenReturn(new Path("/"));
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(DistributedFileSystem.class),
                    Mockito.any(Configuration.class))).thenAnswer(invocation -> {
                        DistributedFileSystem remote = Mockito.mock(DistributedFileSystem.class);
                        Mockito.when(remote.getConf()).thenReturn(invocation.getArgument(1));
                        Mockito.when(remote.getUri()).thenReturn(URI.create("hdfs://nn"));
                        Mockito.when(remote.getWorkingDirectory()).thenReturn(new Path("/"));
                        Mockito.when(remote.getFileStatus(Mockito.any(Path.class))).thenAnswer(call -> {
                            Path path = call.getArgument(0);
                            accesses.add(path.toUri().getPath());
                            return new org.apache.hadoop.fs.FileStatus(1, false, 1, 1, 0, path);
                        });
                        return remote;
                    });
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(LocalFileSystem.class),
                    Mockito.any(Configuration.class))).thenReturn(local);
            FileSystem outer = FileSystem.newInstance(URI.create("viewfs://outer"), source);
            TvfGuard.Context context = TvfGuard.takeNativeContext();
            Assertions.assertNotNull(context);
            Assertions.assertFalse(outer.getConf().getBoolean("fs.viewfs.enable.inner.cache", true));
            outer.getFileStatus(new Path("/warm/file.csv"));
            context.checkNative("viewfs://outer", "/evil/file.csv");
            outer.getFileStatus(new Path("/evil/file.csv"));
            outer.getFileStatus(new Path("/evil/next.csv"));
            Assertions.assertTrue(accesses.contains("/base/file.csv"), accesses.toString());
            Assertions.assertTrue(accesses.contains("/lower/secret/file.csv"), accesses.toString());
            Assertions.assertTrue(accesses.contains("/lower/secret/next.csv"), accesses.toString());
            Mockito.verify(local, Mockito.never()).getFileStatus(Mockito.any(Path.class));
        }
    }

    @Test
    void slashBracesCheckFlattenedRoutesBeforeStaticOrRuntimeDiscovery() throws Exception {
        Map<String, String> properties = hdfs("viewfs://braces");
        properties.put("fs.viewfs.mounttable.braces.link./remote", "hdfs://nn/base");
        properties.put("fs.viewfs.mounttable.braces.linkFallback", "hdfs://nn/fallback");
        for (String pattern : new String[] {"/remote/{../local}/*.csv",
                "/remote/{ok,../local}/*.csv", "/remote/{{../local}}/*.csv"}) {
            String uri = "viewfs://braces" + pattern;
            Map<String, String> statement = readProperties(uri);
            statement.putAll(properties);
            statement.put("fs.viewfs.mounttable.braces.link./local", "file:///unused-local");
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> HadoopTvfAdmission.checkReadDiscovery(uri, statement), uri);
            if ("/remote/{../local}/*.csv".equals(pattern)) {
                // The reported singleton URI reaches admission through the actual TVF constructor.
                // Other brace alternatives may be rejected earlier by the existing URI parameter parser.
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> new HdfsTableValuedFunction(statement), uri);
            }
            Configuration runtime = defaults(Collections.singletonMap(
                    "fs.viewfs.mounttable.braces.link./local", "file:///unused-local"));
            overlay(runtime, HadoopTvfAdmission.protectJava(HadoopTvfAdmission.Access.HADOOP_READ, uri, properties));
            Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("viewfs://braces"), runtime), uri);
        }
        properties.put("fs.viewfs.mounttable.braces.link./local", "file:///unused-local");
        String remote = "viewfs://braces/remote/{../other}/*.csv";
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery(remote, properties));
        Assertions.assertDoesNotThrow(() -> TvfGuard.check(URI.create("viewfs://braces"),
                javaRequest(properties, HadoopTvfAdmission.Access.HADOOP_READ, remote)));
    }

    @Test
    void exactCleanupKeepsLiteralBracesButRejectsSelectedAndNestedLocalTargets() throws Exception {
        Map<String, String> properties = hdfs("viewfs://cleanup");
        properties.put("fs.viewfs.mounttable.cleanup.link./data", "hdfs://nn/base");
        properties.put("fs.viewfs.mounttable.cleanup.link./data/local", "file:///unused-local");
        properties.put("fs.viewfs.mounttable.cleanup.link./nested", "viewfs://inner/base");
        properties.put("fs.viewfs.mounttable.inner.link./base", "file:///unused-local");
        String literal = "viewfs://cleanup/data/literal{tag}";
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                HadoopTvfAdmission.Access.JAVA_DELETE, literal, properties));
        Configuration runtime = javaRequest(properties, HadoopTvfAdmission.Access.JAVA_DELETE, literal);
        try (RemoteClients clients = new RemoteClients();
                FileSystem outer = FileSystem.get(URI.create("viewfs://cleanup"), runtime)) {
            Assertions.assertTrue(outer.delete(new Path(literal), true));
            Assertions.assertTrue(clients.events.contains("delete:0:/base/literal{tag}"));
            Assertions.assertThrows(IOException.class, () -> outer.delete(new Path("/data"), true));
            Assertions.assertThrows(IOException.class, () -> outer.delete(new Path("/"), true));
        }
        for (String selected : new String[] {"/data/local/file", "/nested/file"}) {
            String uri = "viewfs://cleanup" + selected;
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, uri, properties));
            // The same selected local route can exist only in effective core-site.xml.
            Configuration effective = defaults(properties);
            overlay(effective, HadoopTvfAdmission.protectJava(
                    HadoopTvfAdmission.Access.JAVA_DELETE, uri, hdfs("viewfs://cleanup")));
            Assertions.assertThrows(IOException.class, () -> TvfGuard.check(URI.create("viewfs://cleanup"), effective));
        }
    }

    @Test
    void nativeWriteChecksCreateParentOnInitializationAndRetainedContextReuse() throws Exception {
        Map<String, String> write = HadoopTvfAdmission.protectNativeWrite("/safe/prefix_", hdfs("viewfs://create"));
        Assertions.assertEquals(TvfGuard.NATIVE_WRITE_POLICY, write.get(TvfGuard.POLICY_KEY));
        Assertions.assertNotEquals(HadoopTvfAdmission.protectNativeRead(hdfs("viewfs://create")), write);
        for (int i = 0; i < 16; i++) {
            Assertions.assertEquals(write, HadoopTvfAdmission.protectNativeWrite(
                    "/safe/file" + i + "\0ignored-suffix", hdfs("viewfs://create")));
        }
        Map<String, String> mounts = new HashMap<>();
        mounts.put("fs.viewfs.mounttable.create.link./safe", "hdfs://nn/base");
        mounts.put("fs.viewfs.mounttable.create.link./parent", "file:///unused-local");
        mounts.put("fs.viewfs.mounttable.create.link./parent/final", "hdfs://nn/final");
        mounts.put("fs.viewfs.mounttable.create.link./remote", "hdfs://nn/base");
        mounts.put("fs.viewfs.mounttable.create.link./remote/final", "file:///unused-local");
        mounts.put("fs.viewfs.mounttable.create.link./nested", "viewfs://inner/parent");
        mounts.put("fs.viewfs.mounttable.inner.link./parent", "file:///unused-local");
        mounts.put("fs.viewfs.mounttable.inner.link./parent/final", "hdfs://nn/final");
        Configuration runtime = defaults(mounts);
        overlay(runtime, write);
        nativeRequest(runtime, "viewfs://create", "/parent/final");
        Assertions.assertThrows(IOException.class, () -> FileSystem.newInstance(URI.create("viewfs://create"), runtime));
        nativeRequest(runtime, "viewfs://create", "/safe/file");
        try (MockedStatic<ReflectionUtils> factory = Mockito.mockStatic(
                ReflectionUtils.class, Mockito.CALLS_REAL_METHODS)) {
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(ViewFileSystem.class),
                    Mockito.any(Configuration.class))).thenAnswer(call -> Mockito.mock(ViewFileSystem.class));
            TvfGuard.clearNativeContext();
            FileSystem.newInstance(URI.create("viewfs://create"), runtime);
            TvfGuard.Context writer = TvfGuard.takeNativeContext();
            Assertions.assertNotNull(writer);
            Assertions.assertDoesNotThrow(() -> writer.checkNative("viewfs://create", "/safe/next"));
            Assertions.assertDoesNotThrow(() -> writer.checkNative("viewfs://create", "/remote/final"));
            for (String path : new String[] {"/parent/final", "/nested/final"}) {
                Assertions.assertThrows(IOException.class, () -> writer.checkNative("viewfs://create", path));
            }
            Configuration read = new Configuration(runtime);
            read.set(TvfGuard.POLICY_KEY, TvfGuard.NATIVE_POLICY);
            FileSystem.newInstance(URI.create("viewfs://create"), read);
            TvfGuard.Context reader = TvfGuard.takeNativeContext();
            Assertions.assertNotNull(reader);
            Assertions.assertNotEquals(writer.profile(), reader.profile());
            Assertions.assertThrows(IOException.class, () -> reader.checkNative("viewfs://create", "/remote/final"));
        }
    }

    @Test
    void differentJavaPathsCloseOwnedClientsAndReturnGlobalCacheToBaseline() throws Exception {
        Map<String, String> properties = hdfs("viewfs://owned");
        properties.put("fs.viewfs.mounttable.owned.link./data", "hdfs://nn/base");
        properties.put("fs.viewfs.mounttable.owned.link./alias", "hdfs://nn/alternate");
        int baseline = filesystemCacheSize();
        try (RemoteClients clients = new RemoteClients()) {
            for (int i = 0; i < 6; i++) {
                String path = "viewfs://owned/data/file" + i;
                try (FileSystem outer = FileSystem.get(URI.create("viewfs://owned"),
                        javaRequest(properties, HadoopTvfAdmission.Access.HADOOP_READ, path))) {
                    outer.getFileStatus(new Path(path));
                    outer.getFileStatus(new Path("/alias/file" + i));
                    Assertions.assertEquals(i + 1, clients.clients.size());
                }
                Assertions.assertEquals(baseline, filesystemCacheSize(), path);
            }
            clients.verifyClosedOnce();
        }
    }

    @Test
    void equalJavaPoliciesOwnIndependentClientsAndDeleteBeforeClosingThem() throws Exception {
        Map<String, String> properties = hdfs("viewfs://independent");
        properties.put("fs.viewfs.mounttable.independent.link./data", "hdfs://nn/base");
        Configuration source = javaRequest(properties, HadoopTvfAdmission.Access.HADOOP_READ,
                "viewfs://independent/data/file.csv");
        int baseline = filesystemCacheSize();
        try (RemoteClients clients = new RemoteClients()) {
            try (FileSystem first = FileSystem.get(URI.create("viewfs://independent"), source);
                    FileSystem second = FileSystem.get(URI.create("viewfs://independent"), source)) {
                Assertions.assertNotSame(first, second);
                first.getFileStatus(new Path("/data/file.csv"));
                second.getFileStatus(new Path("/data/file.csv"));
                Assertions.assertEquals(2, clients.clients.size());
                Assertions.assertTrue(first.deleteOnExit(new Path("/data/file.csv")));
                first.close();
                Assertions.assertTrue(clients.events.contains("delete:0:/base/file.csv"));
                Assertions.assertTrue(clients.events.indexOf("delete:0:/base/file.csv")
                        < clients.events.indexOf("close:0"), clients.events.toString());
                Mockito.verify(clients.clients.get(1), Mockito.never()).close();
                Assertions.assertDoesNotThrow(() -> second.getFileStatus(new Path("/data/file.csv")));
                first.close();
                Mockito.verify(clients.clients.get(0), Mockito.times(1)).close();
            }
            clients.verifyClosedOnce();
            Assertions.assertEquals(baseline, filesystemCacheSize());
        }
    }

    @Test
    void nestedJavaViewFsKeepsCaseDistinctAuthoritiesAndClosesEachActualClient() throws Exception {
        Map<String, String> properties = hdfs("viewfs://outer-owned");
        properties.put("fs.viewfs.mounttable.outer-owned.link./warm", "viewfs://A/remote");
        properties.put("fs.viewfs.mounttable.outer-owned.link./other", "viewfs://a/secret");
        properties.put("fs.viewfs.mounttable.A.link./remote", "hdfs://upper/base");
        properties.put("fs.viewfs.mounttable.A.link./secret", "file:///unused-local");
        properties.put("fs.viewfs.mounttable.a.linkFallback", "hdfs://lower/lower");
        int baseline = filesystemCacheSize();
        try (RemoteClients clients = new RemoteClients()) {
            try (FileSystem outer = FileSystem.get(URI.create("viewfs://outer-owned"), javaRequest(
                    properties, HadoopTvfAdmission.Access.HADOOP_READ, "viewfs://outer-owned/warm/file.csv"))) {
                outer.getFileStatus(new Path("/warm/file.csv"));
                outer.getFileStatus(new Path("/other/file.csv"));
                outer.getFileStatus(new Path("/other/next.csv"));
                Assertions.assertTrue(clients.events.contains("stat:0:/base/file.csv"));
                Assertions.assertTrue(clients.events.contains("stat:1:/lower/secret/file.csv"));
                Assertions.assertTrue(clients.events.contains("stat:1:/lower/secret/next.csv"));
                Assertions.assertEquals(URI.create("hdfs://upper"), clients.clients.get(0).getUri());
                Assertions.assertEquals(URI.create("hdfs://lower"), clients.clients.get(1).getUri());
            }
            Assertions.assertEquals(2, clients.clients.size());
            clients.verifyClosedOnce();
            Assertions.assertEquals(baseline, filesystemCacheSize());
        }
    }

    @Test
    void nflyOwnsSameAuthorityPeersAndClosesOthersAfterOneCloseFails() throws Exception {
        Map<String, String> properties = hdfs("viewfs://peers");
        properties.put("fs.viewfs.mounttable.peers.linkNfly.minReplication=2./data",
                "hdfs://nn/first,hdfs://nn/second");
        int baseline = filesystemCacheSize();
        try (RemoteClients clients = new RemoteClients()) {
            FileSystem outer = FileSystem.get(URI.create("viewfs://peers"), javaRequest(
                    properties, HadoopTvfAdmission.Access.HADOOP_READ, "viewfs://peers/data/file.csv"));
            Assertions.assertEquals(2, clients.clients.size());
            Assertions.assertNotSame(clients.clients.get(0), clients.clients.get(1));
            clients.failNextClose = true;
            Assertions.assertThrows(IOException.class, outer::close);
            Assertions.assertDoesNotThrow(outer::close);
            clients.verifyClosedOnce();
            Assertions.assertEquals(baseline, filesystemCacheSize());
        }
    }

    @Test
    void failedNflyInitializationClosesSuccessfulAndFailingPeers() throws Exception {
        Map<String, String> properties = hdfs("viewfs://partial");
        properties.put("fs.viewfs.mounttable.partial.linkNfly.minReplication=2./data",
                "hdfs://good/first,hdfs://fail/second");
        int baseline = filesystemCacheSize();
        try (RemoteClients clients = new RemoteClients()) {
            clients.failInitializeAuthority = "fail";
            Assertions.assertThrows(IOException.class, () -> FileSystem.get(URI.create("viewfs://partial"),
                    javaRequest(properties, HadoopTvfAdmission.Access.HADOOP_READ, "viewfs://partial/data/file.csv")));
            Assertions.assertEquals(2, clients.clients.size());
            clients.verifyClosedOnce();
            Assertions.assertEquals(baseline, filesystemCacheSize());
        }
    }

    @Test
    void discoveryOnlyChecksNormalizedProbesWhileReturnedPathsRemainLiteral() throws Exception {
        Map<String, String> properties = hdfs("viewfs://normalized");
        properties.put("fs.viewfs.mounttable.normalized.link./remote", "file:///unused-local");
        properties.put("fs.viewfs.mounttable.normalized.link./other", "hdfs://nn/base");
        properties.put("fs.viewfs.mounttable.normalized.linkFallback", "hdfs://nn/fallback");
        for (boolean enabled : new boolean[] {false, true}) {
            Config.enable_local_tvf = enabled;
            try (RemoteClients clients = new RemoteClients()) {
                int baseline = filesystemCacheSize();
                for (String pattern : new String[] {"/remote/{../other}/*.csv",
                        "/remote/{{../other}}/*.csv", "/remote/\\.\\./other/*.csv"}) {
                    String uri = "viewfs://normalized" + pattern;
                    Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.checkReadDiscovery(uri, properties));
                    // A supplied/returned file with this spelling is literal and still selects LOCAL.
                    Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                            () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.HADOOP_READ,
                                    uri, properties, false));
                    if ("/remote/{../other}/*.csv".equals(pattern)) {
                        Map<String, String> statement = readProperties(uri);
                        statement.putAll(properties);
                        Assertions.assertDoesNotThrow(() -> new HdfsTableValuedFunction(statement));
                    }
                    Configuration request = javaRequest(properties, HadoopTvfAdmission.Access.HADOOP_READ, uri);
                    try (FileSystem filesystem = FileSystem.get(URI.create("viewfs://normalized"), request)) {
                        org.apache.hadoop.fs.FileStatus[] matches = filesystem.globStatus(new Path(uri));
                        Assertions.assertEquals(1, matches.length, uri + clients.events);
                        Assertions.assertEquals("/other/probe.csv", matches[0].getPath().toUri().getPath(), uri);
                        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(
                                HadoopTvfAdmission.Access.HADOOP_READ,
                                matches[0].getPath().toString(), properties, false));
                    }
                    clients.verifyClosedOnce();
                    Assertions.assertEquals(baseline, filesystemCacheSize());
                }
            }
        }
    }

    private Configuration javaRequest(Map<String, String> properties, HadoopTvfAdmission.Access access,
            String path) throws Exception {
        Configuration configuration = defaults(Collections.emptyMap());
        overlay(configuration, HadoopTvfAdmission.protectJava(access, path, properties));
        // Exercise the default-cached remote-provider case that used to retain every path policy.
        configuration.setBoolean("fs.hdfs.impl.disable.cache", false);
        return configuration;
    }

    private static int filesystemCacheSize() throws Exception {
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        Object cache = cacheField.get(null);
        Field mapField = cache.getClass().getDeclaredField("map");
        mapField.setAccessible(true);
        synchronized (cache) {
            return ((Map<?, ?>) mapField.get(cache)).size();
        }
    }

    /** Real ViewFS dispatch/cache, with all storage operations replaced by recognized remote clients. */
    private static final class RemoteClients implements AutoCloseable {
        private final List<DistributedFileSystem> clients = new ArrayList<>();
        private final List<String> events = new ArrayList<>();
        private final MockedStatic<ReflectionUtils> factory;
        private final Method baseClose;
        private String failInitializeAuthority;
        private boolean failNextClose;

        private RemoteClients() throws Exception {
            // The mock must retain FileSystem.close's real cache removal without invoking DFS RPC cleanup.
            baseClose = FileSystem.class.getDeclaredMethod("closeBase");
            baseClose.setAccessible(true);
            factory = Mockito.mockStatic(ReflectionUtils.class, Mockito.CALLS_REAL_METHODS);
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(DistributedFileSystem.class),
                    Mockito.any(Configuration.class))).thenAnswer(call -> remote(call.getArgument(1)));
            factory.when(() -> ReflectionUtils.newInstance(Mockito.eq(LocalFileSystem.class),
                    Mockito.any(Configuration.class))).thenThrow(new AssertionError("Unexpected local storage access"));
        }

        private DistributedFileSystem remote(Configuration configuration) throws IOException {
            int index = clients.size();
            DistributedFileSystem remote = Mockito.mock(DistributedFileSystem.class);
            clients.add(remote);
            AtomicReference<URI> uri = new AtomicReference<>();
            AtomicBoolean closed = new AtomicBoolean();
            Set<Path> directories = new HashSet<>();
            Mockito.when(remote.getConf()).thenReturn(configuration);
            Mockito.when(remote.getUri()).thenAnswer(call -> uri.get());
            Mockito.when(remote.getWorkingDirectory()).thenReturn(new Path("/"));
            Mockito.when(remote.getHomeDirectory()).thenReturn(new Path("/"));
            Mockito.doAnswer(call -> {
                URI target = call.getArgument(0);
                uri.set(URI.create(target.getScheme() + "://" + target.getRawAuthority()));
                if (target.getAuthority().equals(failInitializeAuthority)) {
                    throw new IOException("Injected remote initialization failure");
                }
                return null;
            }).when(remote).initialize(Mockito.any(URI.class), Mockito.any(Configuration.class));
            Mockito.when(remote.getFileStatus(Mockito.any(Path.class))).thenAnswer(call -> {
                if (closed.get()) {
                    throw new IOException("Remote client is closed");
                }
                Path path = call.getArgument(0);
                events.add("stat:" + index + ":" + path.toUri().getPath());
                return new org.apache.hadoop.fs.FileStatus(1, directories.contains(path), 1, 1, 0, path);
            });
            Mockito.when(remote.listStatus(Mockito.any(Path.class))).thenAnswer(call -> {
                Path path = call.getArgument(0);
                events.add("list:" + index + ":" + path.toUri().getPath());
                directories.add(path);
                return new org.apache.hadoop.fs.FileStatus[] {new org.apache.hadoop.fs.FileStatus(
                        1, false, 1, 1, 0, new Path(path, "probe.csv"))};
            });
            Mockito.when(remote.delete(Mockito.any(Path.class), Mockito.anyBoolean())).thenAnswer(call -> {
                events.add("delete:" + index + ":" + ((Path) call.getArgument(0)).toUri().getPath());
                return true;
            });
            Mockito.doAnswer(call -> {
                events.add("close:" + index);
                closed.set(true);
                baseClose.invoke(remote);
                if (failNextClose) {
                    failNextClose = false;
                    throw new IOException("Injected remote close failure");
                }
                return null;
            }).when(remote).close();
            return remote;
        }

        private void verifyClosedOnce() throws IOException {
            for (DistributedFileSystem client : clients) {
                Mockito.verify(client, Mockito.times(1)).close();
            }
        }

        @Override
        public void close() {
            factory.close();
        }
    }

    private Configuration defaults(Map<String, String> entries) throws Exception {
        StringBuilder xml = new StringBuilder("<configuration>");
        entries.forEach((key, value) -> xml.append("<property><name>").append(key)
                .append("</name><value>").append(value).append("</value></property>"));
        Files.write(temporary.resolve("core-site.xml"),
                xml.append("</configuration>").toString().getBytes(StandardCharsets.UTF_8));
        URLClassLoader loader = new URLClassLoader(new URL[] {temporary.toUri().toURL()},
                FileSystem.class.getClassLoader()) {
            @Override
            public URL getResource(String name) {
                return "core-site.xml".equals(name) ? findResource(name) : super.getResource(name);
            }
        };
        ownedLoaders.add(loader);
        Configuration configuration = new Configuration();
        configuration.setClassLoader(loader);
        entries.forEach((key, value) -> Assertions.assertEquals(
                value.contains("${") ? LocalFileSystem.class.getName() : value, configuration.get(key)));
        return configuration;
    }

    private static void overlay(Configuration configuration, Map<String, String> properties) {
        properties.forEach(configuration::set);
    }

    private static Map<String, String> hdfs(String nameNode) {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", nameNode);
        return properties;
    }

    private static Map<String, String> readProperties(String uri) {
        Map<String, String> properties = hdfs("hdfs://runtime");
        properties.put("uri", uri);
        properties.put("format", "csv");
        properties.put("csv_schema", "id:int");
        return properties;
    }
}
