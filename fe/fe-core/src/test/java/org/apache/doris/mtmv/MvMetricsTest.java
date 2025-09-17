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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.rules.exploration.mv.AsyncMaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.SyncMaterializationContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class MvMetricsTest extends TestWithFeService {
    @Mocked
    private AsyncMaterializationContext asyncMvContext1;
    @Mocked
    private SyncMaterializationContext syncMvContext1;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("CREATE TABLE test.`t1` (\n"
                + " `k1` bigint(20) NULL,\n"
                + " `k2` bigint(20) NULL,\n"
                + " `k3` bigint(20) not NULL,\n"
                + " `k4` bigint(20) not NULL,\n"
                + " `k5` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");");
        createMv("create materialized view mv1 as select k1 as a1,sum(k2) as a2 from test.t1 group by k1");

        createMvByNereids("CREATE MATERIALIZED VIEW test.mtmv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1') "
                + "AS select * from test.t1");
    }

    @Test
    public void testRecordRewriteMetricsAsyncPartial() {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        new Expectations() {
            {
                asyncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(asyncMvMetrics);

                asyncMvContext1.isPartitionNeedUnion();
                minTimes = 0;
                result = true;
            }
        };
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(asyncMvContext1), Maps.newHashMap(),
                Lists.newArrayList(asyncMvContext1),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertTrue(asyncMvMetrics.getLastRewriteTime() > 0);
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewritePartialSuccess().getValue());
    }

    @Test
    public void testRecordRewriteMetricsAsyncFull() {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        new Expectations() {
            {
                asyncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(asyncMvMetrics);

                asyncMvContext1.isPartitionNeedUnion();
                minTimes = 0;
                result = false;
            }
        };
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(asyncMvContext1), Maps.newHashMap(),
                Lists.newArrayList(asyncMvContext1),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertTrue(asyncMvMetrics.getLastRewriteTime() > 0);
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewriteFullSuccess().getValue());
    }

    @Test
    public void testRecordRewriteMetricsSync() {
        SyncMvMetrics syncMvMetrics = new SyncMvMetrics();
        new Expectations() {
            {
                syncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(syncMvMetrics);
            }
        };
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(syncMvContext1), Maps.newHashMap(),
                Lists.newArrayList(syncMvContext1),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertEquals(1, (long) syncMvMetrics.getRewriteSuccess().getValue());
    }

    @Test
    public void testRecordRewriteMetricsWithHint() {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        ArrayList<String> mv1 = Lists.newArrayList("mv1");
        new Expectations() {
            {
                asyncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(asyncMvMetrics);

                asyncMvContext1.generateMaterializationIdentifier();
                minTimes = 0;
                result = mv1;

                asyncMvContext1.isPartitionNeedUnion();
                minTimes = 0;
                result = false;
            }
        };
        HashSet<List<String>> useHint = Sets.newHashSet();
        useHint.add(mv1);
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(asyncMvContext1), Maps.newHashMap(),
                Lists.newArrayList(asyncMvContext1),
                useHint, Sets.newHashSet());
        Assert.assertTrue(asyncMvMetrics.getLastRewriteTime() > 0);
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewriteFullSuccess().getValue());
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewriteSuccessWithHint().getValue());
    }

    @Test
    public void testRecordRewriteMetricsFailureShapeMismatch() {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        new Expectations() {
            {
                asyncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(asyncMvMetrics);

                asyncMvContext1.isSuccess();
                minTimes = 0;
                result = false;
            }
        };
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(), Maps.newHashMap(),
                Lists.newArrayList(asyncMvContext1),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertEquals(0, asyncMvMetrics.getLastRewriteTime());
        Assert.assertEquals(0, (long) asyncMvMetrics.getRewriteFullSuccess().getValue());
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewriteFailureShapeMismatch().getValue());
    }

    @Test
    public void testRecordRewriteMetricsFailureCboRejected() {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        new Expectations() {
            {
                asyncMvContext1.getMaterializationMetrics();
                minTimes = 0;
                result = Optional.of(asyncMvMetrics);

                asyncMvContext1.isSuccess();
                minTimes = 0;
                result = true;
            }
        };
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(), Maps.newHashMap(),
                Lists.newArrayList(asyncMvContext1),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertEquals(0, asyncMvMetrics.getLastRewriteTime());
        Assert.assertEquals(0, (long) asyncMvMetrics.getRewriteFullSuccess().getValue());
        Assert.assertEquals(1, (long) asyncMvMetrics.getRewriteFailureCboRejected().getValue());
    }

    @Test
    public void testRecordRewriteMetricsFailureWithHint() throws AnalysisException {
        HashSet<List<String>> noUseHint = Sets.newHashSet();
        noUseHint.add(Lists.newArrayList("internal", "test", "t1", "mv1"));
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(), Maps.newHashMap(),
                Lists.newArrayList(),
                Sets.newHashSet(), noUseHint);
        OlapTable olapTable = (OlapTable) Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException("internal")
                .getDbOrAnalysisException("test").getTableOrAnalysisException("t1");
        MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexIdToMeta()
                .get(olapTable.getIndexIdByName("mv1"));
        Assert.assertEquals(1, (long) materializedIndexMeta.getSyncMvMetrics().getRewriteFailureWithHint().getValue());
    }

    @Test
    public void testRecordRewriteMetricsFailureWithHintAsync() throws AnalysisException {
        HashSet<List<String>> noUseHint = Sets.newHashSet();
        noUseHint.add(Lists.newArrayList("internal", "test", "mtmv1"));
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(), Maps.newHashMap(),
                Lists.newArrayList(),
                Sets.newHashSet(), noUseHint);
        MTMV mtmv = (MTMV) Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException("internal")
                .getDbOrAnalysisException("test").getTableOrAnalysisException("mtmv1");
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRewriteFailureWithHint().getValue());
    }

    @Test
    public void testRecordRewriteMetricsFailureStaleData() throws AnalysisException {
        AsyncMvMetrics asyncMvMetrics = new AsyncMvMetrics();
        HashMap<BaseTableInfo, Collection<Partition>> mvCanRewritePartitionsMap = Maps.newHashMap();
        MTMV mtmv = (MTMV) Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException("internal")
                .getDbOrAnalysisException("test").getTableOrAnalysisException("mtmv1");
        mvCanRewritePartitionsMap.put(new BaseTableInfo(mtmv), Collections.EMPTY_LIST);
        MvMetrics.recordRewriteMetrics(Sets.newHashSet(), mvCanRewritePartitionsMap,
                Lists.newArrayList(),
                Sets.newHashSet(), Sets.newHashSet());
        Assert.assertEquals(0, asyncMvMetrics.getLastRewriteTime());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRewriteFailureStaleData().getValue());
    }
}
