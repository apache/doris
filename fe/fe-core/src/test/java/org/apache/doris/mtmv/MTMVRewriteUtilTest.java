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
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

public class MTMVRewriteUtilTest {
    @Mocked
    private MTMV mtmv;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private SessionVariable sessionVariable;
    @Mocked
    private Partition p1;
    @Mocked
    private MTMVRelation relation;
    @Mocked
    private MTMVStatus status;
    @Mocked
    private MTMVPartitionUtil mtmvPartitionUtil;
    @Mocked
    private MTMVUtil mtmvUtil;
    private long currentTimeMills = 3L;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {

        new Expectations() {
            {
                mtmv.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList(p1);

                p1.getVisibleVersionTime();
                minTimes = 0;
                result = 1L;

                mtmv.getGracePeriod();
                minTimes = 0;
                result = 0L;

                mtmv.getRelation();
                minTimes = 0;
                result = relation;

                mtmv.getStatus();
                minTimes = 0;
                result = status;

                mtmv.getGracePeriod();
                minTimes = 0;
                result = 0L;

                status.getState();
                minTimes = 0;
                result = MTMVState.NORMAL;

                status.getRefreshState();
                minTimes = 0;
                result = MTMVRefreshState.SUCCESS;

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isEnableMaterializedViewRewrite();
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = true;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<String>) any);
                minTimes = 0;
                result = true;

                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = false;
            }
        };
    }

    @Test
    public void testGetMTMVCanRewritePartitionsForceConsistent() throws AnalysisException {
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 2L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<String>) any);
                minTimes = 0;
                result = false;
            }
        };

        // currentTimeMills is 3, grace period is 2, and partition getVisibleVersionTime is 1
        // if forceConsistent this should get 0 partitions which mtmv can use.
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, true);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNormal() {
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsInGracePeriod() throws AnalysisException {
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 2L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<String>) any);
                minTimes = 0;
                result = false;
            }
        };

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotInGracePeriod() throws AnalysisException {
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 1L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<String>) any);
                minTimes = 0;
                result = false;
            }
        };

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableMaterializedViewRewrite() {
        new Expectations() {
            {
                sessionVariable.isEnableMaterializedViewRewrite();
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotSync() throws AnalysisException {
        new Expectations() {
            {
                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<String>) any);
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsEnableContainExternalTable() {
        new Expectations() {
            {
                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = true;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableContainExternalTable() {
        new Expectations() {
            {
                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsStateAbnormal() {
        new Expectations() {
            {
                status.getState();
                minTimes = 0;
                result = MTMVState.SCHEMA_CHANGE;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateAbnormal() {
        new Expectations() {
            {
                status.getRefreshState();
                minTimes = 0;
                result = MTMVRefreshState.FAIL;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateInit() {
        new Expectations() {
            {
                status.getRefreshState();
                minTimes = 0;
                result = MTMVRefreshState.INIT;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }
}
