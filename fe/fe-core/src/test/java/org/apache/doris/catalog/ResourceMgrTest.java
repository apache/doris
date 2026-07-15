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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResourceMgrTest {
    // s3 resource
    private String s3ResName;
    private String s3ResType;
    private String s3Endpoint;
    private String s3Region;
    private String s3RootPath;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3MaxConnections;
    private String s3ReqTimeoutMs;
    private String s3ConnTimeoutMs;
    private Map<String, String> s3Properties;
    private ExecutorService executor;

    @Before
    public void setUp() {
        s3ResName = "s30";
        s3ResType = "s3";
        s3Endpoint = "aaa";
        s3Region = "bj";
        s3RootPath = "/path/to/root";
        s3AccessKey = "xxx";
        s3SecretKey = "yyy";
        s3MaxConnections = "50";
        s3ReqTimeoutMs = "3000";
        s3ConnTimeoutMs = "1000";
        s3Properties = new HashMap<>();
        s3Properties.put("type", s3ResType);
        s3Properties.put("AWS_ENDPOINT", s3Endpoint);
        s3Properties.put("AWS_REGION", s3Region);
        s3Properties.put("AWS_ROOT_PATH", s3RootPath);
        s3Properties.put("AWS_ACCESS_KEY", s3AccessKey);
        s3Properties.put("AWS_SECRET_KEY", s3SecretKey);
        s3Properties.put("AWS_BUCKET", "test-bucket");
        s3Properties.put("s3_validity_check", "false");
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testAddAlterDropResource() throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                    .thenReturn(true);

            // s3 resource
            ResourceMgr mgr = new ResourceMgr();
            CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, s3ResName, ImmutableMap.copyOf(s3Properties)));
            createResourceCommand.getInfo().validate();
            Assert.assertEquals(0, mgr.getResourceNum());
            mgr.createResource(createResourceCommand);
            Assert.assertEquals(1, mgr.getResourceNum());

            // alter
            s3Region = "sh";
            Map<String, String> copiedS3Properties = Maps.newHashMap(s3Properties);
            copiedS3Properties.put("AWS_REGION", s3Region);
            copiedS3Properties.remove("type");
            // current not support modify s3 property
            // mgr.alterResource(alterResourceStmt);
            // Assert.assertEquals(s3Region, ((S3Resource) mgr.getResource(s3ResName)).getProperty("AWS_REGION"));
        }
    }

    @Test(expected = DdlException.class)
    public void testAddResourceExist() throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                    .thenReturn(true);

            // add
            ResourceMgr mgr = new ResourceMgr();
            CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, s3ResName, ImmutableMap.copyOf(s3Properties)));
            createResourceCommand.getInfo().validate();

            Assert.assertEquals(0, mgr.getResourceNum());
            mgr.createResource(createResourceCommand);
            Assert.assertEquals(1, mgr.getResourceNum());

            // add again
            mgr.createResource(createResourceCommand);
        }
    }

    @Test
    public void testHdfsResourceUsesReadWriteLockForPropertyAccess() throws Exception {
        HdfsResource resource = createHdfsResource("hdfs0", "hdfs://first");

        resource.writeLock();
        Future<Map<String, String>> copiedPropertiesFuture = null;
        try {
            copiedPropertiesFuture = executor.submit(resource::getCopiedProperties);
            assertFutureBlocked(copiedPropertiesFuture);
        } finally {
            resource.writeUnlock();
        }
        Assert.assertEquals("hdfs://first",
                copiedPropertiesFuture.get(5, TimeUnit.SECONDS).get(HdfsResource.HADOOP_FS_NAME));

        resource.writeLock();
        Future<Void> procFuture = null;
        try {
            procFuture = executor.submit(() -> {
                resource.getProcNodeData(new BaseProcResult());
                return null;
            });
            assertFutureBlocked(procFuture);
        } finally {
            resource.writeUnlock();
        }
        procFuture.get(5, TimeUnit.SECONDS);

        resource.readLock();
        Future<Void> modifyFuture = null;
        try {
            modifyFuture = executor.submit(() -> {
                resource.modifyProperties(hdfsProperties("hdfs://second"));
                return null;
            });
            assertFutureBlocked(modifyFuture);
        } finally {
            resource.readUnlock();
        }
        modifyFuture.get(5, TimeUnit.SECONDS);
        Assert.assertEquals("hdfs://second", resource.getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
    }

    @Test
    public void testAlterResourceLogsDetachedSnapshot() throws Exception {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            EditLog.EditLogItem logItem = Mockito.mock(EditLog.EditLogItem.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE), Mockito.any(Resource.class)))
                    .thenReturn(logItem);

            ResourceMgr mgr = new ResourceMgr();
            mgr.createResource(createHdfsResource("hdfs0", "hdfs://initial"), false);
            mgr.alterResource("hdfs0", hdfsProperties("hdfs://first"));

            ArgumentCaptor<Resource> logResourceCaptor = ArgumentCaptor.forClass(Resource.class);
            Mockito.verify(editLog).submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE),
                    logResourceCaptor.capture());
            Resource firstLogResource = logResourceCaptor.getValue();

            mgr.alterResource("hdfs0", hdfsProperties("hdfs://second"));

            Assert.assertNotSame(mgr.getResource("hdfs0"), firstLogResource);
            Assert.assertEquals("hdfs://first",
                    firstLogResource.getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
            Assert.assertEquals("hdfs://second",
                    mgr.getResource("hdfs0").getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        }
    }

    @Test
    public void testCopiedResourceKeepsReferenceSnapshot() throws Exception {
        HdfsResource resource = createHdfsResource("hdfs0", "hdfs://first");
        resource.addReference("tbl", Resource.ReferenceType.POLICY);

        Resource copied = resource.getCopiedResourceSnapshot();
        resource.removeReference("tbl", Resource.ReferenceType.POLICY);

        try {
            copied.dropResource();
            Assert.fail("copied resource should keep the original reference");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("tbl"));
        }
        resource.dropResource();
    }

    private HdfsResource createHdfsResource(String name, String fsName) throws DdlException {
        HdfsResource resource = new HdfsResource(name);
        resource.modifyProperties(hdfsProperties(fsName));
        return resource;
    }

    private Map<String, String> hdfsProperties(String fsName) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(HdfsResource.HADOOP_FS_NAME, fsName);
        return properties;
    }

    private void assertFutureBlocked(Future<?> future) throws Exception {
        try {
            future.get(100, TimeUnit.MILLISECONDS);
            Assert.fail("future should be blocked by the resource lock");
        } catch (TimeoutException e) {
            // Expected. The caller releases the lock and then waits for completion.
        }
    }
}
