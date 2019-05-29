/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.BrokerScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;

public class LoadingTaskPlannerTest {

    @Test
    public void testPlan(@Injectable OlapTable table,
                         @Injectable BrokerDesc brokerDesc,
                         @Injectable BrokerFileGroup brokerFileGroup,
                         @Injectable TBrokerFileStatus tBrokerFileStatus,
                         @Injectable Column column) throws UserException {
        List<BrokerFileGroup> brokerFileGroupList = Lists.newArrayList();
        brokerFileGroupList.add(brokerFileGroup);
        List<TBrokerFileStatus> tBrokerFileStatuses = Lists.newArrayList();
        tBrokerFileStatuses.add(tBrokerFileStatus);
        List<List<TBrokerFileStatus>> tBrokerFileStatusesLists = Lists.newArrayList();
        tBrokerFileStatusesLists.add(tBrokerFileStatuses);
        List<Column> baseSchema = Lists.newArrayList();
        baseSchema.add(column);
        List<String> partitionNames = Lists.newArrayList();
        partitionNames.add("p1");
        partitionNames.add("p2");
        new Expectations() {
            {
                table.getBaseSchema();
                result = baseSchema;
                brokerFileGroup.getPartitionNames();
                result = partitionNames;
            }
        };

        LoadingTaskPlanner loadingTaskPlanner = new LoadingTaskPlanner(1L, 1L, table,
                                                                       brokerDesc, brokerFileGroupList, true);
        loadingTaskPlanner.plan(tBrokerFileStatusesLists, 1);
        List<PlanFragment> fragments = Deencapsulation.getField(loadingTaskPlanner, "fragments");
        Assert.assertEquals(1, fragments.size());
        Assert.assertEquals(true, fragments.get(0).getPlanRoot() instanceof BrokerScanNode);
        BrokerScanNode brokerScanNode = (BrokerScanNode) fragments.get(0).getPlanRoot();


    }
}
