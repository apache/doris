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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
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
                = "MTMV{refreshInfo=BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 2 SECOND STARTS \"ss\", "
                + "querySql='select * from xxx;', "
                + "status=MTMVStatus{state=INIT, schemaChangeDetail='null', refreshState=INIT}, "
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

    @Test
    public void testGetExcludedTriggerTables() {
        Map<String, String> mvProperties = Maps.newHashMap();
        MTMV mtmv = new MTMV();
        mtmv.setMvProperties(mvProperties);

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "t1");
        Set<TableName> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assert.assertEquals(1, excludedTriggerTables.size());
        Assert.assertTrue(excludedTriggerTables.contains(new TableName(null, null, "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "db1.t1");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assert.assertEquals(1, excludedTriggerTables.size());
        Assert.assertTrue(excludedTriggerTables.contains(new TableName(null, "db1", "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "ctl1.db1.t1");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assert.assertEquals(1, excludedTriggerTables.size());
        Assert.assertTrue(excludedTriggerTables.contains(new TableName("ctl1", "db1", "t1")));

        mvProperties.put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, "ctl1.db1.t1,db2.t2,t3");
        excludedTriggerTables = mtmv.getExcludedTriggerTables();
        Assert.assertEquals(3, excludedTriggerTables.size());
        Assert.assertTrue(excludedTriggerTables.contains(new TableName("ctl1", "db1", "t1")));
        Assert.assertTrue(excludedTriggerTables.contains(new TableName(null, "db2", "t2")));
        Assert.assertTrue(excludedTriggerTables.contains(new TableName(null, null, "t3")));
    }

    @Test
    public void testAlterStatus() {
        MTMV mtmv = new MTMV();
        MTMVStatus status = new MTMVStatus();
        mtmv.setStatus(status);
        // test init
        Assert.assertEquals(MTMVState.INIT, status.getState());
        Assert.assertEquals(MTMVRefreshState.INIT, status.getRefreshState());
        // test schema change
        status.setRefreshState(MTMVRefreshState.SUCCESS);
        mtmv.alterStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "base table"));
        Assert.assertEquals(MTMVState.SCHEMA_CHANGE, status.getState());
        Assert.assertEquals(MTMVRefreshState.SUCCESS, status.getRefreshState());

        MTMVStatus alterStatus = new MTMVStatus();
        alterStatus.setState(MTMVState.SCHEMA_CHANGE);
        alterStatus.setSchemaChangeDetail("base table");
        mtmv.alterStatus(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "base table"));
        Assert.assertEquals(MTMVState.SCHEMA_CHANGE, status.getState());
        Assert.assertEquals(MTMVRefreshState.SUCCESS, status.getRefreshState());
    }
}
