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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerLoadPendingTaskTest {

    private static TBrokerFileStatus tBrokerFileStatus = new TBrokerFileStatus();

    @BeforeClass
    public static void setUp() {
        tBrokerFileStatus.size = 1;
    }

    @Test
    public void testExecuteTask(@Injectable BrokerLoadJob brokerLoadJob,
                                @Injectable BrokerFileGroup brokerFileGroup,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked Env env) throws UserException {
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = "hdfs://localhost:8900/test_column";
            }
        };

        new MockUp<BrokerUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses) {
                fileStatuses.add(tBrokerFileStatus);
            }
        };

        BrokerLoadPendingTask brokerLoadPendingTask = new BrokerLoadPendingTask(brokerLoadJob, aggKeyToFileGroups, brokerDesc, LoadTask.Priority.NORMAL);
        brokerLoadPendingTask.executeTask();
        BrokerPendingTaskAttachment brokerPendingTaskAttachment = Deencapsulation.getField(brokerLoadPendingTask, "attachment");
        Assert.assertEquals(1, brokerPendingTaskAttachment.getFileNumByTable(aggKey));
        Assert.assertEquals(tBrokerFileStatus, brokerPendingTaskAttachment.getFileStatusByTable(aggKey).get(0).get(0));
    }

    private Map<String, String> baseS3Props() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.region", "us-east-1");
        props.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        return props;
    }

    private Map<FileGroupAggKey, List<BrokerFileGroup>> makeAggKeyMap(BrokerFileGroup fileGroup) {
        Map<FileGroupAggKey, List<BrokerFileGroup>> map = Maps.newHashMap();
        List<BrokerFileGroup> groups = Lists.newArrayList();
        groups.add(fileGroup);
        map.put(new FileGroupAggKey(1L, null), groups);
        return map;
    }

    @Test
    public void testAnonymousFallbackOn403NoCredentials(
            @Injectable BrokerLoadJob brokerLoadJob,
            @Injectable BrokerFileGroup brokerFileGroup,
            @Mocked Env env) throws UserException {
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = Lists.newArrayList("s3://public-bucket/data/file.parquet");
            }
        };

        AtomicInteger parseFileCallCount = new AtomicInteger(0);
        new MockUp<BrokerUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc desc, List<TBrokerFileStatus> fileStatuses)
                    throws UserException {
                int count = parseFileCallCount.incrementAndGet();
                if (count == 1) {
                    throw new UserException(
                            "list s3 files failed, err: Status Code: 403, AWS Error Code: AccessDenied");
                }
                // Second call (anonymous retry) succeeds
                fileStatuses.add(tBrokerFileStatus);
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("S3", baseS3Props());
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyMap = makeAggKeyMap(brokerFileGroup);
        BrokerLoadPendingTask task = new BrokerLoadPendingTask(
                brokerLoadJob, aggKeyMap, brokerDesc, LoadTask.Priority.NORMAL);

        Deencapsulation.invoke(task, "getAllFileStatus");

        Assert.assertEquals(2, parseFileCallCount.get());
        // Verify task's brokerDesc was updated with ANONYMOUS
        BrokerDesc updatedDesc = Deencapsulation.getField(task, "brokerDesc");
        Assert.assertEquals("ANONYMOUS", updatedDesc.getProperties().get("s3.credentials_provider_type"));
        // Verify parent job's brokerDesc was also updated
        BrokerDesc jobDesc = Deencapsulation.getField(brokerLoadJob, "brokerDesc");
        Assert.assertEquals("ANONYMOUS", jobDesc.getProperties().get("s3.credentials_provider_type"));
    }

    @Test
    public void testNoFallbackWhenExplicitCredentials(
            @Injectable BrokerLoadJob brokerLoadJob,
            @Injectable BrokerFileGroup brokerFileGroup,
            @Mocked Env env) {
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = Lists.newArrayList("s3://bucket/data/file.parquet");
            }
        };

        new MockUp<BrokerUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc desc, List<TBrokerFileStatus> fileStatuses)
                    throws UserException {
                throw new UserException(
                        "list s3 files failed, err: Status Code: 403, AWS Error Code: AccessDenied");
            }
        };

        Map<String, String> props = baseS3Props();
        props.put("s3.access_key", "AKIAIOSFODNN7EXAMPLE");
        props.put("s3.secret_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        BrokerDesc brokerDesc = new BrokerDesc("S3", props);

        BrokerLoadPendingTask task = new BrokerLoadPendingTask(
                brokerLoadJob, makeAggKeyMap(brokerFileGroup), brokerDesc, LoadTask.Priority.NORMAL);

        try {
            Deencapsulation.invoke(task, "getAllFileStatus");
            Assert.fail("Should have thrown UserException");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Status Code: 403"));
        }
    }

    @Test
    public void testOriginalErrorThrownWhenBothAttemptsFail(
            @Injectable BrokerLoadJob brokerLoadJob,
            @Injectable BrokerFileGroup brokerFileGroup,
            @Mocked Env env) {
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = Lists.newArrayList("s3://bucket/data/file.parquet");
            }
        };

        AtomicInteger parseFileCallCount = new AtomicInteger(0);
        new MockUp<BrokerUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc desc, List<TBrokerFileStatus> fileStatuses)
                    throws UserException {
                int count = parseFileCallCount.incrementAndGet();
                if (count == 1) {
                    throw new UserException(
                            "list s3 files failed, err: Status Code: 403, Original error");
                }
                throw new UserException(
                        "list s3 files failed, err: Status Code: 403, Anonymous also denied");
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("S3", baseS3Props());
        BrokerLoadPendingTask task = new BrokerLoadPendingTask(
                brokerLoadJob, makeAggKeyMap(brokerFileGroup), brokerDesc, LoadTask.Priority.NORMAL);

        try {
            Deencapsulation.invoke(task, "getAllFileStatus");
            Assert.fail("Should have thrown UserException");
        } catch (Exception e) {
            // Should throw the ORIGINAL error, not the retry error
            Assert.assertTrue(e.getMessage().contains("Original error"));
            Assert.assertFalse(e.getMessage().contains("Anonymous also denied"));
        }
        Assert.assertEquals(2, parseFileCallCount.get());
    }

    @Test
    public void testNoFallbackOnNon403Error(
            @Injectable BrokerLoadJob brokerLoadJob,
            @Injectable BrokerFileGroup brokerFileGroup,
            @Mocked Env env) {
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = Lists.newArrayList("s3://bucket/data/file.parquet");
            }
        };

        AtomicInteger parseFileCallCount = new AtomicInteger(0);
        new MockUp<BrokerUtil>() {
            @Mock
            public void parseFile(String path, BrokerDesc desc, List<TBrokerFileStatus> fileStatuses)
                    throws UserException {
                parseFileCallCount.incrementAndGet();
                throw new UserException("list s3 files failed, err: Status Code: 404, Not Found");
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("S3", baseS3Props());
        BrokerLoadPendingTask task = new BrokerLoadPendingTask(
                brokerLoadJob, makeAggKeyMap(brokerFileGroup), brokerDesc, LoadTask.Priority.NORMAL);

        try {
            Deencapsulation.invoke(task, "getAllFileStatus");
            Assert.fail("Should have thrown UserException");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Status Code: 404"));
        }
        // parseFile should have been called only once (no retry for non-403)
        Assert.assertEquals(1, parseFileCallCount.get());
    }
}
