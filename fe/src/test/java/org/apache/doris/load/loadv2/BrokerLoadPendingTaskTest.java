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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class BrokerLoadPendingTaskTest {

    @Test
    public void testExecuteTask(@Injectable BrokerLoadJob brokerLoadJob,
                                @Injectable BrokerFileGroup brokerFileGroup,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked Catalog catalog,
                                @Injectable TBrokerFileStatus tBrokerFileStatus) throws UserException {
        Map<Long, List<BrokerFileGroup>> tableToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        tableToFileGroups.put(1L, brokerFileGroups);
        new Expectations() {
            {
                catalog.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = "hdfs://localhost:8900/test_column";
            }
        };
        new MockUp<BrokerUtil>() {
            @Mock
            public void parseBrokerFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses) {
                fileStatuses.add(tBrokerFileStatus);
            }
        };

        BrokerLoadPendingTask brokerLoadPendingTask = new BrokerLoadPendingTask(brokerLoadJob, tableToFileGroups, brokerDesc);
        brokerLoadPendingTask.executeTask();
        BrokerPendingTaskAttachment brokerPendingTaskAttachment = Deencapsulation.getField(brokerLoadPendingTask, "attachment");
        Assert.assertEquals(1, brokerPendingTaskAttachment.getFileNumByTable(1L));
        Assert.assertEquals(tBrokerFileStatus, brokerPendingTaskAttachment.getFileStatusByTable(1L).get(0).get(0));
    }
}
