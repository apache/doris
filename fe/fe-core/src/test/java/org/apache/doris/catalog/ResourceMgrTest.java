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
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.BaseProcResult;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
        executor = Executors.newFixedThreadPool(2);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
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
            copiedPropertiesFuture = submitAndWaitUntilStarted(resource::getCopiedProperties);
            assertFutureBlocked(copiedPropertiesFuture);
        } finally {
            resource.writeUnlock();
        }
        Assert.assertEquals("hdfs://first",
                copiedPropertiesFuture.get(5, TimeUnit.SECONDS).get(HdfsResource.HADOOP_FS_NAME));

        resource.writeLock();
        Future<Void> procFuture = null;
        try {
            procFuture = submitAndWaitUntilStarted(() -> {
                resource.getProcNodeData(new BaseProcResult());
                return null;
            });
            assertFutureBlocked(procFuture);
        } finally {
            resource.writeUnlock();
        }
        procFuture.get(5, TimeUnit.SECONDS);

        resource.writeLock();
        Future<Resource> snapshotFuture = null;
        try {
            snapshotFuture = submitAndWaitUntilStarted(resource::getCopiedResourceSnapshot);
            assertFutureBlocked(snapshotFuture);
        } finally {
            resource.writeUnlock();
        }
        Assert.assertEquals("hdfs://first",
                snapshotFuture.get(5, TimeUnit.SECONDS).getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));

        resource.readLock();
        Future<Void> modifyFuture = null;
        try {
            modifyFuture = submitAndWaitUntilStarted(() -> {
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
    public void testSynchronousAlterSubmitLogsOrderedDetachedSnapshots() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLog.EditLogItem logItem = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        ResourceMgr mgr = new ResourceMgr();
        HdfsResource resource = createHdfsResource("hdfs0", "hdfs://initial");
        mgr.createResource(resource, false);

        CountDownLatch firstSubmitEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstSubmit = new CountDownLatch(1);
        AtomicInteger submitCount = new AtomicInteger();
        List<Resource> synchronouslySerializedResources = new ArrayList<>();
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE), Mockito.any(Resource.class)))
                .thenAnswer(invocation -> {
                    Assert.assertTrue(Thread.holdsLock(resource.getAlterLock()));
                    synchronouslySerializedResources.add(copyResource(invocation.getArgument(1)));
                    if (submitCount.getAndIncrement() == 0) {
                        firstSubmitEntered.countDown();
                        Assert.assertTrue(releaseFirstSubmit.await(5, TimeUnit.SECONDS));
                    }
                    return logItem;
                });
        Mockito.when(logItem.await()).thenAnswer(invocation -> {
            Assert.assertFalse(Thread.holdsLock(resource.getAlterLock()));
            return 0L;
        });

        Future<Void> firstAlter = submitAndWaitUntilStarted(() -> {
            alterResourceWithEnv(mgr, env, "hdfs0", hdfsProperties("hdfs://first"));
            return null;
        });
        Assert.assertTrue(firstSubmitEntered.await(5, TimeUnit.SECONDS));

        Future<Void> secondAlter = submitAndWaitUntilStarted(() -> {
            alterResourceWithEnv(mgr, env, "hdfs0", hdfsProperties("hdfs://second"));
            return null;
        });
        try {
            assertFutureBlocked(secondAlter);
            Assert.assertEquals("hdfs://first",
                    resource.getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        } finally {
            releaseFirstSubmit.countDown();
        }
        firstAlter.get(5, TimeUnit.SECONDS);
        secondAlter.get(5, TimeUnit.SECONDS);

        ArgumentCaptor<Resource> logResourceCaptor = ArgumentCaptor.forClass(Resource.class);
        Mockito.verify(editLog, Mockito.times(2)).submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE),
                logResourceCaptor.capture());
        Mockito.verify(logItem, Mockito.times(2)).await();
        List<Resource> loggedResources = logResourceCaptor.getAllValues();

        Assert.assertNotSame(resource, loggedResources.get(0));
        Assert.assertNotSame(loggedResources.get(0), loggedResources.get(1));
        Assert.assertEquals("hdfs://first",
                loggedResources.get(0).getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        Assert.assertEquals("hdfs://second",
                loggedResources.get(1).getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        Assert.assertEquals("hdfs://first",
                synchronouslySerializedResources.get(0).getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        Assert.assertEquals("hdfs://second",
                synchronouslySerializedResources.get(1).getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
    }

    @Test
    public void testBatchAlterSerializesQueuedSnapshotsAfterLaterAlter() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLog.EditLogItem firstLogItem = Mockito.mock(EditLog.EditLogItem.class);
        EditLog.EditLogItem secondLogItem = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        ResourceMgr mgr = new ResourceMgr();
        HdfsResource resource = createHdfsResource("hdfs0", "hdfs://initial");
        mgr.createResource(resource, false);

        List<Resource> queuedResources = new ArrayList<>();
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE), Mockito.any(Resource.class)))
                .thenAnswer(invocation -> {
                    Assert.assertTrue(Thread.holdsLock(resource.getAlterLock()));
                    queuedResources.add(invocation.getArgument(1));
                    return queuedResources.size() == 1 ? firstLogItem : secondLogItem;
                });

        CountDownLatch firstAwaitEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstAwait = new CountDownLatch(1);
        Mockito.when(firstLogItem.await()).thenAnswer(invocation -> {
            Assert.assertFalse(Thread.holdsLock(resource.getAlterLock()));
            firstAwaitEntered.countDown();
            Assert.assertTrue(releaseFirstAwait.await(5, TimeUnit.SECONDS));
            return 1L;
        });
        Mockito.when(secondLogItem.await()).thenAnswer(invocation -> {
            Assert.assertFalse(Thread.holdsLock(resource.getAlterLock()));
            return 2L;
        });

        Future<Void> firstAlter = submitAndWaitUntilStarted(() -> {
            alterResourceWithEnv(mgr, env, "hdfs0", hdfsProperties("hdfs://first"));
            return null;
        });
        Assert.assertTrue(firstAwaitEntered.await(5, TimeUnit.SECONDS));

        Future<Void> secondAlter = submitAndWaitUntilStarted(() -> {
            alterResourceWithEnv(mgr, env, "hdfs0", hdfsProperties("hdfs://second"));
            return null;
        });
        try {
            secondAlter.get(1, TimeUnit.SECONDS);
            Assert.assertEquals(2, queuedResources.size());
            Assert.assertNotSame(resource, queuedResources.get(0));
            Assert.assertNotSame(queuedResources.get(0), queuedResources.get(1));

            Resource firstSerialized = copyResource(queuedResources.get(0));
            Resource secondSerialized = copyResource(queuedResources.get(1));
            Assert.assertEquals("hdfs://first",
                    firstSerialized.getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
            Assert.assertEquals("hdfs://second",
                    secondSerialized.getCopiedProperties().get(HdfsResource.HADOOP_FS_NAME));
        } finally {
            releaseFirstAwait.countDown();
        }
        firstAlter.get(5, TimeUnit.SECONDS);

        Mockito.verify(firstLogItem).await();
        Mockito.verify(secondLogItem).await();
    }

    @Test
    public void testS3ValidityCheckDoesNotHoldResourceStateLock() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        EditLog.EditLogItem logItem = Mockito.mock(EditLog.EditLogItem.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(editLog.submitEdit(Mockito.eq(OperationType.OP_ALTER_RESOURCE), Mockito.any(Resource.class)))
                .thenReturn(logItem);

        ResourceMgr mgr = new ResourceMgr();
        S3Resource resource = new S3Resource("s30");
        resource.setProperties(ImmutableMap.copyOf(s3Properties));
        mgr.createResource(resource, false);

        CountDownLatch pingEntered = new CountDownLatch(1);
        CountDownLatch releasePing = new CountDownLatch(1);
        Future<Void> alterFuture = submitAndWaitUntilStarted(() -> {
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                    MockedStatic<S3Resource> mockedS3 = Mockito.mockStatic(S3Resource.class, Mockito.CALLS_REAL_METHODS)) {
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                mockedS3.when(() -> S3Resource.pingS3(
                        Mockito.anyString(), Mockito.anyString(), Mockito.anyMap())).thenAnswer(invocation -> {
                            pingEntered.countDown();
                            Assert.assertTrue(releasePing.await(5, TimeUnit.SECONDS));
                            return null;
                        });
                mgr.alterResource("s30", Maps.newHashMap(ImmutableMap.of("s3_validity_check", "true")));
            }
            return null;
        });
        Assert.assertTrue(pingEntered.await(5, TimeUnit.SECONDS));

        Future<Void> readerFuture = submitAndWaitUntilStarted(() -> {
            resource.readLock();
            try {
                return null;
            } finally {
                resource.readUnlock();
            }
        });
        try {
            readerFuture.get(1, TimeUnit.SECONDS);
        } finally {
            releasePing.countDown();
        }
        alterFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testResourceSnapshotCoordinatesReferenceUpdates() throws Exception {
        HdfsResource resource = createHdfsResource("hdfs0", "hdfs://first");
        resource.addReference("first", Resource.ReferenceType.POLICY);

        Future<Resource> copiedFuture;
        synchronized (resource) {
            copiedFuture = submitAndWaitUntilStarted(resource::getCopiedResourceSnapshot);
            assertFutureBlocked(copiedFuture);
            resource.removeReference("first", Resource.ReferenceType.POLICY);
            resource.addReference("second", Resource.ReferenceType.POLICY);
        }
        Resource copied = copiedFuture.get(5, TimeUnit.SECONDS);

        try {
            copied.dropResource();
            Assert.fail("copied resource should contain a complete reference snapshot");
        } catch (DdlException e) {
            Assert.assertFalse(e.getMessage().contains("first"));
            Assert.assertTrue(e.getMessage().contains("second"));
        }
        resource.removeReference("second", Resource.ReferenceType.POLICY);
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

    private void alterResourceWithEnv(ResourceMgr mgr, Env env, String resourceName,
            Map<String, String> properties) throws DdlException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mgr.alterResource(resourceName, properties);
        }
    }

    private Resource copyResource(Resource resource) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            resource.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return Resource.read(in);
        }
    }

    private <T> Future<T> submitAndWaitUntilStarted(Callable<T> task) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        Future<T> future = executor.submit(() -> {
            started.countDown();
            return task.call();
        });
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        return future;
    }

    private void assertFutureBlocked(Future<?> future) throws Exception {
        try {
            future.get(100, TimeUnit.MILLISECONDS);
            Assert.fail("future should be blocked by the resource lock");
        } catch (TimeoutException e) {
            // The caller releases the lock before waiting for completion.
        }
    }
}
