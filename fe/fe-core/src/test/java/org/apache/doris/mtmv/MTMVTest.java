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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MTMVTest {
    @Test
    public void testToInfoString() {
        String expect
                = "MTMV{refreshInfo=BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 2 SECOND STARTS ss, "
                + "querySql='select * from xxx;', "
                + "status=MTMVStatus{state=INIT, schemaChangeDetail='null', refreshState=INIT}, "
                + "envInfo=EnvInfo{ctlId='1', dbId='2'}, "
                + "jobInfo=MTMVJobInfo{jobName='job1', "
                + "historyTasks=[MTMVTask{dbId=0, mtmvId=0, taskContext=null, "
                + "needRefreshPartitions=null, completedPartitions=null, refreshMode=null} "
                + "AbstractTask{jobId=null, taskId=1, status=null, createTimeMs=null, startTimeMs=null, "
                + "finishTimeMs=null, taskType=null, errMsg='null'}]}, mvProperties={}, "
                + "relation=MTMVRelation{baseTables=[], baseTablesOneLevel=[], baseViews=[]}, "
                + "mvPartitionInfo=MTMVPartitionInfo{partitionType=null, relatedTable=null, "
                + "relatedCol='null', partitionCol='null'}, "
                + "refreshSnapshot=MTMVRefreshSnapshot{partitionSnapshots={}}, id=1, name='null', "
                + "qualifiedDbName='db1', comment='comment1'}";
        MTMV mtmv = new MTMV();
        mtmv.setId(1L);
        mtmv.setComment("comment1");
        mtmv.setQualifiedDbName("db1");
        mtmv.setRefreshInfo(buildMTMVRefreshInfo(mtmv));
        mtmv.setQuerySql("select * from xxx;");
        mtmv.setStatus(new MTMVStatus());
        mtmv.setEnvInfo(new EnvInfo(1L, 2L));
        mtmv.setJobInfo(buildMTMVJobInfo(mtmv));
        mtmv.setMvProperties(new HashMap<>());
        mtmv.setRelation(new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet()));
        mtmv.setMvPartitionInfo(new MTMVPartitionInfo());
        mtmv.setRefreshSnapshot(new MTMVRefreshSnapshot());
        Assert.assertEquals(expect, mtmv.toInfoString());
    }

    private MTMVRefreshInfo buildMTMVRefreshInfo(MTMV mtmv) {
        MTMVRefreshTriggerInfo info = new MTMVRefreshTriggerInfo(RefreshTrigger.SCHEDULE,
                new MTMVRefreshSchedule("ss", 2,
                        IntervalUnit.SECOND));
        MTMVRefreshInfo mtmvRefreshInfo = new MTMVRefreshInfo(BuildMode.IMMEDIATE, RefreshMethod.COMPLETE, info);
        return mtmvRefreshInfo;
    }

    private MTMVJobInfo buildMTMVJobInfo(MTMV mtmv) {
        MTMVJobInfo mtmvJobInfo = new MTMVJobInfo("job1");
        mtmvJobInfo.addHistoryTask(buildMTMVTask(mtmv));
        return mtmvJobInfo;
    }

    private MTMVTask buildMTMVTask(MTMV mtmv) {
        MTMVTask task = new MTMVTask(mtmv, null, null);
        task.setTaskId(1L);
        return task;
    }

    @Test
    public void testCalculateDoublyPartitionMappings() throws AnalysisException {
        Map<String, Set<String>> mvToBase = Maps.newHashMap();
        Map<String, String> baseToMv = Maps.newHashMap();
        Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = mockRelatedPartitionDescs();
        Map<String, PartitionItem> mvPartitionItems = mockMvPartitionItems();
        for (Entry<String, PartitionItem> entry : mvPartitionItems.entrySet()) {
            Set<String> basePartitionNames = relatedPartitionDescs.getOrDefault(entry.getValue().toPartitionKeyDesc(),
                    Sets.newHashSet());
            String mvPartitionName = entry.getKey();
            mvToBase.put(mvPartitionName, basePartitionNames);
            for (String basePartitionName : basePartitionNames) {
                baseToMv.put(basePartitionName, mvPartitionName);
            }
        }
        Assert.assertEquals(mvToBase.get("mvp1"), Sets.newHashSet("baseP1_1", "baseP1_2"));
        Assert.assertEquals(baseToMv.get("baseP1_1"), "mvp1");
        Assert.assertEquals(baseToMv.get("baseP1_2"), "mvp1");
    }

    private Map<PartitionKeyDesc, Set<String>> mockRelatedPartitionDescs() throws AnalysisException {
        Map<PartitionKeyDesc, Set<String>> res = Maps.newHashMap();
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        PartitionKey rangeP1Lower = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")),
                Lists.newArrayList(k1));
        PartitionKey rangeP1Upper = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                Lists.newArrayList(k1));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        PartitionItem item1 = new RangePartitionItem(rangeP1);
        res.put(item1.toPartitionKeyDesc(), Sets.newHashSet("baseP1_1", "baseP1_2"));
        return res;
    }

    private Map<String, PartitionItem> mockMvPartitionItems() throws AnalysisException {
        Map<String, PartitionItem> res = Maps.newHashMap();
        Column k1 = new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", "key1");
        PartitionKey rangeP1Lower = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")),
                Lists.newArrayList(k1));
        PartitionKey rangeP1Upper = PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("10")),
                Lists.newArrayList(k1));
        Range<PartitionKey> rangeP1 = Range.closedOpen(rangeP1Lower, rangeP1Upper);
        PartitionItem item1 = new RangePartitionItem(rangeP1);
        res.put("mvp1", item1);
        return res;
    }
}
