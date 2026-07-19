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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerPendingTaskAttachment;
import org.apache.doris.load.loadv2.LoadLoadingTask;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloudBrokerLoadJobTest {

    @After
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testCreateTaskScopesS3ExpressMarkerToBrokerLoad() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("s3.provider", "AWS");
        properties.put("s3.endpoint", "https://endpoint-is-ignored.example.com");
        properties.put("s3.region", "us-west-2");
        BrokerDesc original = new BrokerDesc("S3", StorageBackend.StorageType.S3, properties);

        CloudBrokerLoadJob job = new CloudBrokerLoadJob();
        Deencapsulation.setField(job, "brokerDesc", original);
        Map<String, String> sessionVariables = Deencapsulation.getField(job, "sessionVariables");
        sessionVariables.put(CloudBrokerLoadJob.CLOUD_CLUSTER_ID, "cluster-id");

        BrokerFileGroup fileGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(fileGroup.getFilePaths()).thenReturn(
                Collections.singletonList("s3://analytics--usw2-az1--x-s3/data.parquet"));
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        BrokerFileGroupAggInfo fileGroupAggInfo = new BrokerFileGroupAggInfo();
        Deencapsulation.setField(fileGroupAggInfo, "aggKeyToFileGroups",
                Collections.singletonMap(aggKey, Collections.singletonList(fileGroup)));
        Deencapsulation.setField(job, "fileGroupAggInfo", fileGroupAggInfo);

        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfoService.getClusterNameByClusterId("cluster-id")).thenReturn("cluster-name");
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            BrokerDesc expressTaskBrokerDesc = createTaskAndCaptureBrokerDesc(job, fileGroup, aggKey);
            Assert.assertNotSame(original, expressTaskBrokerDesc);
            Assert.assertFalse(original.getBackendConfigProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
            Assert.assertEquals("true", expressTaskBrokerDesc.getBackendConfigProperties()
                    .get(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));

            Mockito.when(fileGroup.getFilePaths()).thenReturn(
                    Collections.singletonList("s3://ordinary-bucket/data.parquet"));
            BrokerDesc ordinaryTaskBrokerDesc = createTaskAndCaptureBrokerDesc(job, fileGroup, aggKey);
            Assert.assertSame(original, ordinaryTaskBrokerDesc);
            Assert.assertFalse(ordinaryTaskBrokerDesc.getBackendConfigProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));

            Mockito.when(fileGroup.getFilePaths()).thenReturn(
                    Collections.singletonList("s3://analytics--usw2-az1--x-s3/data.parquet"));
            Deencapsulation.setField(job, "jobType", EtlJobType.COPY);
            BrokerDesc copyTaskBrokerDesc = createTaskAndCaptureBrokerDesc(job, fileGroup, aggKey);
            Assert.assertSame(original, copyTaskBrokerDesc);
            Assert.assertFalse(copyTaskBrokerDesc.getBackendConfigProperties()
                    .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
        }
    }

    private BrokerDesc createTaskAndCaptureBrokerDesc(CloudBrokerLoadJob job, BrokerFileGroup fileGroup,
            FileGroupAggKey aggKey) throws Exception {
        AtomicReference<BrokerDesc> brokerDesc = new AtomicReference<>();
        try (MockedConstruction<CloudLoadLoadingTask> mockedTask = Mockito.mockConstruction(
                CloudLoadLoadingTask.class,
                (mock, context) -> brokerDesc.set((BrokerDesc) context.arguments().get(3)))) {
            LoadLoadingTask task = job.createTask(Mockito.mock(Database.class), Mockito.mock(OlapTable.class),
                    Collections.singletonList(fileGroup), false, 0, aggKey,
                    Mockito.mock(BrokerPendingTaskAttachment.class));
            Assert.assertSame(mockedTask.constructed().get(0), task);
        }
        return brokerDesc.get();
    }
}
