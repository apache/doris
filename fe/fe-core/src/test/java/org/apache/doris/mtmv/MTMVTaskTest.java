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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MTMVTaskTest {
    private long poneId = 1L;
    private String poneName = "p1";
    private long ptwoId = 2L;
    private String ptwoName = "p2";
    private List<Long> allPartitionIds = Lists.newArrayList(poneId, ptwoId);
    private MTMVRelation relation = new MTMVRelation(Sets.newHashSet(), Sets.newHashSet());

    @Mocked
    private MTMV mtmv;
    @Mocked
    private MTMVUtil mtmvUtil;
    @Mocked
    private MTMVPartitionUtil mtmvPartitionUtil;
    @Mocked
    private MTMVPartitionInfo mtmvPartitionInfo;
    @Mocked
    private MTMVRefreshInfo mtmvRefreshInfo;

    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        new Expectations() {
            {
                mtmvUtil.getMTMV(anyLong, anyLong);
                minTimes = 0;
                result = mtmv;

                mtmv.getPartitionIds();
                minTimes = 0;
                result = allPartitionIds;

                mtmv.getMvPartitionInfo();
                minTimes = 0;
                result = mtmvPartitionInfo;

                mtmvPartitionInfo.getPartitionType();
                minTimes = 0;
                result = MTMVPartitionType.FOLLOW_BASE_TABLE;

                mtmvPartitionUtil.getPartitionsIdsByNames(mtmv, Lists.newArrayList(poneName));
                minTimes = 0;
                result = poneId;

                mtmvPartitionUtil.isMTMVSync(mtmv, (Set<BaseTableInfo>) any, (Set<String>) any,
                        (Map<Long, Set<Long>>) any);
                minTimes = 0;
                result = true;

                mtmv.getRefreshInfo();
                minTimes = 0;
                result = mtmvRefreshInfo;

                mtmvRefreshInfo.getRefreshMethod();
                minTimes = 0;
                result = RefreshMethod.COMPLETE;
            }
        };
    }

    @Test
    public void testCalculateNeedRefreshPartitionsManualComplete() throws AnalysisException {
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL, null, true);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertEquals(allPartitionIds, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsManualPartitions() throws AnalysisException {
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName), false);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertEquals(Lists.newArrayList(poneId), result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystem() throws AnalysisException {
        new Expectations() {
            {
                mtmvRefreshInfo.getRefreshMethod();
                minTimes = 0;
                result = RefreshMethod.AUTO;
            }
        };
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertTrue(CollectionUtils.isEmpty(result));
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemComplete() throws AnalysisException {
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertEquals(allPartitionIds, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemNotSyncComplete() throws AnalysisException {
        new Expectations() {
            {
                mtmvPartitionUtil.isMTMVSync(mtmv, (Set<BaseTableInfo>) any, (Set<String>) any,
                        (Map<Long, Set<Long>>) any);
                minTimes = 0;
                result = false;
            }
        };
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertEquals(allPartitionIds, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemNotSyncAuto() throws AnalysisException {
        new Expectations() {
            {
                mtmvPartitionUtil
                        .isMTMVSync(mtmv, (Set<BaseTableInfo>) any, (Set<String>) any, (Map<Long, Set<Long>>) any);
                minTimes = 0;
                result = false;

                mtmvRefreshInfo.getRefreshMethod();
                minTimes = 0;
                result = RefreshMethod.AUTO;

                mtmvPartitionUtil
                        .getMTMVNeedRefreshPartitions(mtmv, (Set<BaseTableInfo>) any, (Map<Long, Set<Long>>) any);
                minTimes = 0;
                result = Lists.newArrayList(ptwoId);
            }
        };
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<Long> result = task.calculateNeedRefreshPartitions(Maps.newHashMap());
        Assert.assertEquals(Lists.newArrayList(ptwoId), result);
    }
}
