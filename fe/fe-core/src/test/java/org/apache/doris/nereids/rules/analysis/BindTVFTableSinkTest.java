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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFTableSink;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.TVFTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.HadoopTvfAdmission;
import org.apache.doris.tablefunction.LocalTableValuedFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BindTVFTableSinkTest {
    private boolean originalEnableLocalTvf;

    @BeforeEach
    void setUp() {
        originalEnableLocalTvf = Config.enable_local_tvf;
        Config.enable_local_tvf = false;
    }

    @AfterEach
    void tearDown() {
        Config.enable_local_tvf = originalEnableLocalTvf;
    }

    @Test
    void disabledLocalSinkRejectsInsertAndExplainBeforeProperties() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String properties : Arrays.asList("'format'='csv'",
                    "'file_path'='/tmp/local_gate_', 'backend_id'='1', 'format'='csv'")) {
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> analyzeInsert(prefix + "INSERT INTO local(" + properties + ") SELECT 1"));
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
    }

    @Test
    void enabledLocalSinkKeepsPropertyValidation() {
        Config.enable_local_tvf = true;
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> analyzeInsert("INSERT INTO local('format'='csv') SELECT 1"));
        Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
    }

    @Test
    void disabledLocalDoesNotBlockRemoteSinks() {
        for (String name : Arrays.asList("s3", "hdfs")) {
            Assertions.assertInstanceOf(LogicalTVFTableSink.class,
                    analyzeInsert("INSERT INTO " + name + "('file_path'='remote/data', 'format'='csv') SELECT 1"));
        }
    }

    @Test
    void disabledLocalWriterRejectsRemoteInsertAndExplain() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String name : Arrays.asList("s3", "hdfs")) {
                for (String writer : Arrays.asList("java-writer:local-file")) {
                    for (String props : Arrays.asList("", "'file_path'='/tmp/local_gate_', 'format'='csv',")) {
                        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                                () -> analyzeInsert(prefix + "INSERT INTO " + name + "(" + props
                                        + "'writer_class'='" + writer + "') SELECT 1"));
                        Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
                    }
                }
            }
        }
    }

    @Test
    void directSinkBindingChecksEffectiveLocalWriter() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "s3://bucket/data");
        properties.put("format", "csv");
        properties.put("s3.endpoint", "http://127.0.0.1:9000");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "test-key");
        properties.put("s3.secret_key", "test-secret");
        for (String name : Arrays.asList("s3", "hdfs", "local")) {
            for (String writer : Arrays.asList("java-writer:local-file",
                    "java-writer\0ignored:local-file", "java-writer:local-file\0ignored")) {
                properties.put("writer_class", writer);
                TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), name, properties,
                        Collections.emptyList());
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
        Config.enable_local_tvf = true;
        properties.put("writer_class", "java-writer:local-file");
        new TVFTableSink(new PlanNodeId(0), "s3", properties, Collections.emptyList()).bindDataSink();
        Config.enable_local_tvf = false;
        properties.remove("writer_class");
        new TVFTableSink(new PlanNodeId(0), "s3", properties, Collections.emptyList()).bindDataSink();
        properties.put("writer_class", "java-writer:other-writer");
        Assertions.assertDoesNotThrow(() -> LocalTableValuedFunction.checkSinkEnabled("s3", properties));
        properties.put("writer_class", "java-writer:local-file:other");
        Assertions.assertDoesNotThrow(() -> LocalTableValuedFunction.checkSinkEnabled("s3", properties));
    }

    @Test
    void enabledLocalWriterKeepsRemotePropertyValidation() {
        Config.enable_local_tvf = true;
        for (String name : Arrays.asList("s3", "hdfs")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> analyzeInsert("INSERT INTO " + name
                            + "('writer_class'='java-writer:local-file') SELECT 1"));
            Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
        }
    }

    @Test
    void loadUserCannotExecuteLocalWriterWhileDisabled() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String sql = "INSERT INTO s3('file_path'='/tmp/local_gate_', 'format'='csv',"
                + "'writer_class'='java-writer:local-file') SELECT 1";
        MemoTestUtils.createStatementContext(context, sql);
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(sql);
        Env env = Mockito.spy(Env.getCurrentEnv());
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.doReturn(accessManager).when(env).getAccessManager();
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.LOAD)).thenReturn(true);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(context, executor));
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.ADMIN);
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.LOAD);
            Mockito.verifyNoInteractions(executor);
        }
    }

    @Test
    void disabledNativeHadoopLocalSinksRejectInsertAndExplain() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String storage : Arrays.asList("'fs.defaultFS'='file:///'",
                    "'fs.defaultFS'='hdfs://namenode:8020',"
                            + "'fs.hdfs.impl'='org.apache.hadoop.fs.LocalFileSystem'",
                    "'fs.defaultFS'='hdfs://namenode:8020',"
                            + "'fs.hdfs.impl'='org.apache.hadoop.fs.RawLocalFileSystem'",
                    "'fs.defaultFS'='hdfs://namenode:8020','fs.hdfs.impl'='${fs.file.impl}',"
                            + "'fs.file.impl'='org.apache.hadoop.fs.LocalFileSystem'",
                    "'fs.defaultFS'='hdfs://namenode:8020',"
                            + "'fs.hdfs.impl '='org.apache.hadoop.fs.LocalFileSystem'")) {
                for (String cleanup : Arrays.asList("false", "true")) {
                    AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                            () -> analyzeInsert(prefix + "INSERT INTO hdfs('file_path'='/tmp/local_gate_',"
                                    + "'format'='csv','delete_existing_files'='" + cleanup + "',"
                                    + storage + ") SELECT 1"));
                    Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
                }
            }
        }
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> analyzeInsert("INSERT INTO hdfs('fs.defaultFS'='file:///') SELECT 1"));
        Assertions.assertTrue(exception.getMessage().contains("requires 'file_path'"));
    }

    @Test
    void directNativeHadoopBindingHonorsLocalOptIn() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/tmp/local_gate_");
        properties.put("format", "csv");
        for (String defaultFs : Arrays.asList("file:///", "default")) {
            properties.put("fs.defaultFS", defaultFs);
            TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties,
                    Collections.emptyList());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
        }
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.put("fs.hdfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties, Collections.emptyList());
        Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
        Config.enable_local_tvf = true;
        sink.bindDataSink();
        properties.put("fs.defaultFS", "file:///");
        sink.bindDataSink();
        Config.enable_local_tvf = false;
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.remove("fs.hdfs.impl");
        // Hadoop normally provides a local implementation for file; it is not selected by hdfs.
        properties.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sink.bindDataSink();
        Assertions.assertInstanceOf(LogicalTVFTableSink.class,
                analyzeInsert("INSERT INTO hdfs('file_path'='/tmp/remote_', 'format'='csv',"
                        + "'fs.defaultFS'='hdfs://namenode:8020',"
                        + "'fs.file.impl'='org.apache.hadoop.fs.LocalFileSystem') SELECT 1"));
    }

    @Test
    void normalizedHadoopConfigIsCheckedBeforeNativeBinding() throws Exception {
        String originalConfigDir = Config.hadoop_config_dir;
        String originalConfigProperty = System.getProperty("doris.hadoop.config.dir");
        Path directory = Files.createTempDirectory("tvf-hadoop-admission-");
        Path resource = directory.resolve("local-filesystem.xml");
        try {
            Files.write(resource, ("<configuration><property><name>fs.defaultFS</name>"
                    + "<value>file:///</value></property></configuration>").getBytes(StandardCharsets.UTF_8));
            Config.hadoop_config_dir = directory.toString() + "/";
            Map<String, String> properties = new HashMap<>();
            properties.put("file_path", "/tmp/local_gate_");
            properties.put("format", "csv");
            properties.put("fs.hdfs.support", "true");
            properties.put("hadoop.config.resources", resource.getFileName().toString());
            TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties,
                    Collections.emptyList());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            Config.enable_local_tvf = true;
            sink.bindDataSink();
        } finally {
            Config.hadoop_config_dir = originalConfigDir;
            if (originalConfigProperty == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", originalConfigProperty);
            }
            Files.deleteIfExists(resource);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    void hadoopCleanupChecksSelectedFilesystemWithoutOpeningIt() {
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                "/remote/path", properties));
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                        "file:///tmp/prefix_", properties));
        properties.remove("fs.defaultFS");
        for (String defaultFs : Arrays.asList("local", "file:///")) {
            properties.put("fs.default.name", defaultFs);
            Assertions.assertThrows(AnalysisException.class,
                    () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, "/tmp/prefix_", properties));
        }
        properties.clear();
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, "/tmp/prefix_", properties));
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.put("fs.default.name", "file:///");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, "/tmp/prefix_", properties));
        properties.put("fs.default.name", "hdfs://other-namenode:8020");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                "/remote/path", properties));
        properties.remove("fs.defaultFS");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                "/remote/path", properties));
        properties.clear();
        properties.put("fs.defaultFS", "viewfs://cluster");
        properties.put("fs.viewfs.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                        "viewfs://cluster/tmp", properties));
        properties.clear();
        properties.put("fs.defaultFS", "${fs.selected.uri}");
        properties.put("fs.selected.uri", "file:///");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, "/tmp/prefix_", properties));
        properties.put("fs.selected.uri", "hdfs://namenode:8020");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE, "/remote/path", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.JAVA_DELETE,
                "/remote/path", properties));
        Config.enable_local_tvf = false;
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.put("fs.hdfs.impl", "${fs.selected.impl}");
        properties.put("fs.selected.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        properties.put("fs.selected.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "", properties));
        Config.enable_local_tvf = false;
        properties.put("fs.hdfs.impl", "${fs.selected.impl}");
        properties.remove("fs.selected.impl");
        properties.put("fs.selected.impl ", "org.apache.hadoop.fs.LocalFileSystem");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        properties.put("fs.selected.impl ", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "", properties));
        Config.enable_local_tvf = false;
        // BE consumes canonical defaultFS as its NameNode; the old alias can redefine
        // that key inside Hadoop. Do not infer this indirect provider from the FE's raw map.
        properties.put("fs.default.name", "org.apache.hadoop.fs.LocalFileSystem");
        properties.put("fs.hdfs.impl", "${fs.defaultFS}");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        properties.put("fs.hdfs.impl", "${env:TVF_TEST_UNRESOLVED_IMPL}");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
        Config.enable_local_tvf = true;
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "", properties));
        Config.enable_local_tvf = false;
        properties.clear();
        properties.put("fs.defaultFS", "hdfs://namenode:8020");
        properties.put("fs.hdfs.impl\0ignored", "org.apache.hadoop.fs.LocalFileSystem\0ignored");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "", properties));
    }

    @Test
    void cleanupEntryRejectsHadoopLocalStorageBeforeFilesystemCreation() throws Exception {
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO s3('file_path'='/tmp/local_gate_', 'format'='csv') SELECT 1");
        Method cleanup = InsertIntoTVFCommand.class.getDeclaredMethod(
                "deleteExistingFilesInFE", String.class, Map.class);
        cleanup.setAccessible(true);
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/tmp/local_gate_");
        properties.put("format", "csv");
        properties.put("delete_existing_files", "true");
        properties.put("fs.defaultFS", "file:///");
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            // The nominal S3 name must not hide Hadoop storage selected for FE-side cleanup.
            InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class,
                    () -> cleanup.invoke(command, "s3", properties));
            Assertions.assertInstanceOf(AnalysisException.class, exception.getCause());
            Assertions.assertTrue(exception.getCause().getMessage().contains("LOCAL TVF is disabled"));
            factory.verifyNoInteractions();
        }
    }

    @Test
    void loadUserCannotReachHadoopLocalCleanupOrExecution() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String sql = "INSERT INTO hdfs('file_path'='/tmp/local_gate_', 'format'='csv',"
                + "'delete_existing_files'='true', 'fs.defaultFS'='file:///') SELECT 1";
        MemoTestUtils.createStatementContext(context, sql);
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(sql);
        Env env = Mockito.spy(Env.getCurrentEnv());
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.doReturn(accessManager).when(env).getAccessManager();
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.LOAD)).thenReturn(true);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(context, executor));
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.ADMIN);
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.LOAD);
            Mockito.verifyNoInteractions(executor);
            factory.verifyNoInteractions();
        }
    }

    @Test
    void disabledViewFsLocalMountsRejectInsertAndExplain() {
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            for (String cleanup : Arrays.asList("false", "true")) {
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> analyzeInsert(prefix + "INSERT INTO hdfs('file_path'='/local/sub/prefix_',"
                                + "'format'='csv','delete_existing_files'='" + cleanup + "',"
                                + "'fs.defaultFS'='viewfs://cluster',"
                                + "'fs.viewfs.mounttable.cluster.link./local'='file:///tmp') SELECT 1"));
                Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            }
        }
    }

    @Test
    void viewFsChecksSelectedTargetsAndPreservesRemoteMounts() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/remote/prefix_");
        properties.put("format", "csv");
        properties.put("fs.defaultFS", "viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./remote", "hdfs://namenode:8020/base");
        properties.put("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "hdfs://namenode:8020/fallback");
        new TVFTableSink(new PlanNodeId(0), "hdfs", properties, Collections.emptyList()).bindDataSink();
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/locality/prefix_", properties));
        // A prefix naming a mount point generates a sibling filename; a trailing slash writes inside it.
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/local", properties));
        Assertions.assertInstanceOf(LogicalTVFTableSink.class,
                analyzeInsert("INSERT INTO hdfs('file_path'='/local', 'format'='csv',"
                        + "'delete_existing_files'='true', 'fs.defaultFS'='viewfs://cluster',"
                        + "'fs.viewfs.mounttable.cluster.link./local'='file:///tmp',"
                        + "'fs.viewfs.mounttable.cluster.linkFallback'='hdfs://namenode:8020/base') SELECT 1"));
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "/local/", properties));
        properties.put("file_path", "/local/sub/prefix_");
        TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties, Collections.emptyList());
        Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
        Config.enable_local_tvf = true;
        sink.bindDataSink();
        Config.enable_local_tvf = false;
        properties.remove("fs.viewfs.mounttable.cluster.link./local");
        properties.put("fs.viewfs.mounttable.cluster.link./local/", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.link./local/");
        properties.put("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        properties.put("fs.viewfs.mounttable.cluster.link./local/remote", "hdfs://namenode:8020/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/local/remote/prefix_", properties));
        properties.put("fs.defaultFS", "viewfs://cl%75ster");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.put("fs.defaultFS", "viewfs://cluster_name");
        properties.put("fs.viewfs.ignore.port.in.mount.table.name", "true");
        properties.put("fs.viewfs.mounttable.cluster_name.link./remote", "hdfs://namenode:8020/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/remote/prefix_", properties));
        properties.put("fs.defaultFS", "viewfs://cluster:8020");
        properties.put("fs.viewfs.ignore.port.in.mount.table.name", "true");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.clear();
        properties.put("fs.defaultFS", "viewfs:///");
        properties.put("fs.viewfs.mounttable.default.name.key", "named");
        properties.put("fs.viewfs.mounttable.named.link./local", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.clear();
        properties.put("fs.defaultFS", "viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/unmapped/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.linkFallback");
        for (String key : Arrays.asList("linkMerge./local",
                "linkNfly.minReplication=2,repairOnRead=true./local")) {
            String mountKey = "fs.viewfs.mounttable.cluster." + key;
            properties.put(mountKey, "hdfs://namenode:8020/base,file:///tmp");
            Assertions.assertThrows(AnalysisException.class,
                    () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                            "/local/sub/prefix_", properties));
            properties.put(mountKey, "hdfs://namenode:8020/base,hdfs://other:8020/base");
            Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                    "/local/sub/prefix_", properties));
            properties.remove(mountKey);
        }
        properties.put("fs.viewfs.mounttable.cluster.linkMergeSlash", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.put("fs.viewfs.mounttable.cluster.linkMergeSlash", "hdfs://namenode:8020/base,archive");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/local/sub/prefix_", properties));
        properties.remove("fs.viewfs.mounttable.cluster.linkMergeSlash");
        properties.put("fs.viewfs.mounttable.cluster.link./local", "viewfs://inner/root");
        properties.put("fs.viewfs.mounttable.inner.link./root", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.put("fs.viewfs.mounttable.inner.link./root", "hdfs://namenode:8020/base");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/local/sub/prefix_", properties));
        properties.put("fs.viewfs.mounttable.inner.link./root", "viewfs://cluster/local");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.clear();
        properties.put("fs.defaultFS", "viewfs://A");
        properties.put("fs.viewfs.mounttable.A.link./a", "viewfs://B/base");
        properties.put("fs.viewfs.mounttable.A.link./a/b/c", "hdfs://namenode:8020/remote");
        properties.put("fs.viewfs.mounttable.B.link./base/d", "file:///tmp");
        properties.put("fs.viewfs.mounttable.B.link./base/b/d", "hdfs://namenode:8020/remote");
        properties.put("fs.viewfs.mounttable.B.link./base/z", "hdfs://namenode:8020/remote");
        Assertions.assertDoesNotThrow(() -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                "/a/z/sub/prefix_", properties));
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/a/b/d/sub/prefix_", properties));
        properties.clear();
        properties.put("fs.defaultFS", "hdfs://cluster:8020");
        properties.put("fs.hdfs.impl", "org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme");
        properties.put("fs.viewfs.mounttable.cluster:8020.link./local", "hdfs://namenode:8020/base");
        properties.put("fs.viewfs.mounttable.cluster.link./local", "file:///tmp");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE,
                        "/local/sub/prefix_", properties));
        properties.clear();
        properties.put("fs.defaultFS", "viewfs://inherited");
        Assertions.assertThrows(AnalysisException.class,
                () -> HadoopTvfAdmission.check(HadoopTvfAdmission.Access.NATIVE_WRITE, "/unknown/prefix_", properties));
    }

    @Test
    void explicitRemoteViewFsResourceIsNormalizedBeforeAdmission() throws Exception {
        String originalConfigDir = Config.hadoop_config_dir;
        String originalConfigProperty = System.getProperty("doris.hadoop.config.dir");
        Path directory = Files.createTempDirectory("tvf-remote-viewfs-");
        Path resource = directory.resolve("remote.xml");
        try {
            Files.write(resource, ("<configuration><property>"
                    + "<name>fs.viewfs.mounttable.cluster.link./data</name>"
                    + "<value>hdfs://remote/base</value></property></configuration>")
                    .getBytes(StandardCharsets.UTF_8));
            Config.hadoop_config_dir = directory.toString() + "/";
            String statement = "INSERT INTO hdfs('file_path'='/data/sub/prefix_', 'format'='csv',"
                    + "'fs.defaultFS'='viewfs://cluster', 'hadoop.config.resources'='remote.xml') SELECT 1";
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                for (String prefix : Arrays.asList("", "EXPLAIN ")) {
                    Assertions.assertInstanceOf(LogicalTVFTableSink.class, analyzeInsert(prefix + statement));
                }
                Map<String, String> properties = new HashMap<>();
                properties.put("file_path", "/data/sub/prefix_");
                properties.put("format", "csv");
                properties.put("fs.defaultFS", "viewfs://cluster");
                properties.put("hadoop.config.resources", "remote.xml");
                TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties,
                        Collections.emptyList());
                Assertions.assertDoesNotThrow(sink::bindDataSink);
                factory.verifyNoInteractions();
            }
        } finally {
            Config.hadoop_config_dir = originalConfigDir;
            if (originalConfigProperty == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", originalConfigProperty);
            }
            Files.deleteIfExists(resource);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    void viewFsResourcesAndCleanupRejectBeforeFilesystemCreation() throws Exception {
        String originalConfigDir = Config.hadoop_config_dir;
        String originalConfigProperty = System.getProperty("doris.hadoop.config.dir");
        Path directory = Files.createTempDirectory("tvf-viewfs-admission-");
        Path resource = directory.resolve("viewfs.xml");
        try {
            Files.write(resource, ("<configuration><property><name>fs.defaultFS</name>"
                    + "<value>viewfs://cluster</value></property><property>"
                    + "<name>fs.viewfs.mounttable.cluster.link./local</name>"
                    + "<value>file:///tmp</value></property></configuration>").getBytes(StandardCharsets.UTF_8));
            Config.hadoop_config_dir = directory.toString() + "/";
            Map<String, String> properties = new HashMap<>();
            properties.put("file_path", "/local/sub/prefix_");
            properties.put("format", "csv");
            properties.put("fs.hdfs.support", "true");
            properties.put("hadoop.config.resources", resource.getFileName().toString());
            TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", properties,
                    Collections.emptyList());
            Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
            InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                    "INSERT INTO s3('file_path'='/local/sub/prefix_', 'format'='csv') SELECT 1");
            Method cleanup = InsertIntoTVFCommand.class.getDeclaredMethod(
                    "deleteExistingFilesInFE", String.class, Map.class);
            cleanup.setAccessible(true);
            properties.put("delete_existing_files", "true");
            try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
                InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class,
                        () -> cleanup.invoke(command, "s3", properties));
                Assertions.assertInstanceOf(AnalysisException.class, exception.getCause());
                factory.verifyNoInteractions();
            }
        } finally {
            Config.hadoop_config_dir = originalConfigDir;
            if (originalConfigProperty == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", originalConfigProperty);
            }
            Files.deleteIfExists(resource);
            Files.deleteIfExists(directory);
        }
    }

    @Test
    void loadUserCannotExecuteViewFsLocalWritesOrCleanup() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        String sql = "INSERT INTO hdfs('file_path'='/local/sub/prefix_', 'format'='csv',"
                + "'delete_existing_files'='true','fs.defaultFS'='viewfs://cluster',"
                + "'fs.viewfs.mounttable.cluster.link./local'='file:///tmp') SELECT 1";
        MemoTestUtils.createStatementContext(context, sql);
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(sql);
        Env env = Mockito.spy(Env.getCurrentEnv());
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.doReturn(accessManager).when(env).getAccessManager();
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.LOAD)).thenReturn(true);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(context, executor));
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.ADMIN);
            Mockito.verify(accessManager).checkGlobalPriv(context, PrivPredicate.LOAD);
            Mockito.verifyNoInteractions(executor);
            factory.verifyNoInteractions();
        }
    }

    @Test
    void cleanupEntryRejectsSelectedLocalSpiBeforeFilesystemCreation() throws Exception {
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO s3('file_path'='/tmp/local_gate_/prefix_', 'format'='csv') SELECT 1");
        Method cleanup = InsertIntoTVFCommand.class.getDeclaredMethod(
                "deleteExistingFilesInFE", String.class, Map.class);
        cleanup.setAccessible(true);
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/tmp/local_gate_/prefix_");
        properties.put("format", "csv");
        properties.put("delete_existing_files", "true");
        properties.put("fs.local.support", "true");
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class,
                    () -> cleanup.invoke(command, "s3", properties));
            Assertions.assertInstanceOf(AnalysisException.class, exception.getCause());
            Assertions.assertTrue(exception.getCause().getMessage().contains("LOCAL TVF is disabled"));
            factory.verifyNoInteractions();
        }
    }

    @Test
    void nativeUriPrefixesCannotSelectAnUncheckedLocalMount() throws Exception {
        String properties = "'file_path'='hdfs://unrelated/remote/prefix_', 'format'='csv',"
                + "'fs.defaultFS'='viewfs://cluster',"
                + "'fs.viewfs.mounttable.cluster.link./remote'='hdfs://remote/base',"
                + "'fs.viewfs.mounttable.cluster.link./hdfs:'='file:///tmp'";
        for (String prefix : Arrays.asList("", "EXPLAIN ")) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> analyzeInsert(prefix + "INSERT INTO hdfs(" + properties + ") SELECT 1"));
        }
        Map<String, String> props = new HashMap<>();
        props.put("file_path", "hdfs://unrelated/remote/prefix_");
        props.put("format", "csv");
        props.put("fs.defaultFS", "viewfs://cluster");
        props.put("fs.viewfs.mounttable.cluster.link./remote", "hdfs://remote/base");
        props.put("fs.viewfs.mounttable.cluster.link./hdfs:", "file:///tmp");
        TVFTableSink sink = new TVFTableSink(new PlanNodeId(0), "hdfs", props, Collections.emptyList());
        Assertions.assertThrows(AnalysisException.class, sink::bindDataSink);
    }

    @Test
    void emptyViewFsTargetRejectsBeforeCleanupFilesystemCreation() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file_path", "/data/sub/prefix_");
        properties.put("format", "csv");
        properties.put("fs.defaultFS", "viewfs://cluster");
        properties.put("fs.viewfs.mounttable.cluster.link./data", "");
        properties.put("fs.viewfs.mounttable.cluster.linkFallback", "file:///tmp");
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO hdfs('file_path'='/data/sub/prefix_', 'format'='csv') SELECT 1");
        Method cleanup = InsertIntoTVFCommand.class.getDeclaredMethod(
                "deleteExistingFilesInFE", String.class, Map.class);
        cleanup.setAccessible(true);
        try (MockedStatic<FileSystemFactory> factory = Mockito.mockStatic(FileSystemFactory.class)) {
            InvocationTargetException exception = Assertions.assertThrows(InvocationTargetException.class,
                    () -> cleanup.invoke(command, "hdfs", properties));
            Assertions.assertInstanceOf(AnalysisException.class, exception.getCause());
            factory.verifyNoInteractions();
        }
    }

    private Plan analyzeInsert(String sql) {
        ConnectContext context = MemoTestUtils.createConnectContext();
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        if (parsed instanceof ExplainCommand) {
            parsed = ((ExplainCommand) parsed).getLogicalPlan();
        }
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) parsed;
        return PlanChecker.from(context).analyze(command.getExplainPlan(context)).getPlan();
    }

    @Test
    void duplicateOutputNamesKeepDistinctPositions() {
        Config.enable_local_tvf = true;
        ConnectContext context = MemoTestUtils.createConnectContext();
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO local("
                        + "'file_path'='/tmp/tvf_duplicate_columns_',"
                        + "'backend_id'='1',"
                        + "'format'='csv') "
                        + "SELECT 1 AS x, 2 AS x");

        Plan analyzed = PlanChecker.from(context)
                .analyze(command.getExplainPlan(context))
                .getPlan();
        LogicalTVFTableSink<?> sink = Assertions.assertInstanceOf(LogicalTVFTableSink.class, analyzed);
        LogicalProject<?> sinkProject = Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        List<NamedExpression> projects = sinkProject.getProjects();

        Assertions.assertEquals(2, projects.size());
        Assertions.assertNotEquals(projects.get(0).getExprId(), projects.get(1).getExprId(),
                "TVF sink output positions must not be collapsed by duplicate display names");
    }
}
