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

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DataProcessorDesc;
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.Load;
import org.apache.doris.load.Source;
import org.apache.doris.task.MasterTaskExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SparkLoadJobTest {

    @Test
    public void testFromLoadStmt(@Injectable LoadStmt loadStmt,
                                 @Injectable DataDescription dataDescription,
                                 @Injectable LabelName labelName,
                                 @Injectable Database database,
                                 @Injectable OlapTable olapTable,
                                 @Mocked Catalog catalog,
                                 @Injectable String originStmt) {

        String label = "label";
        long dbId = 1;
        String tableName = "table";
        String databaseName = "database";
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        DataProcessorDesc dataProcessorDesc = new EtlClusterDesc("spark.cluster0", Maps.newHashMap());
        Map<String, String> properties = Maps.newHashMap();
        String hiveTable = "hivedb.table0";
        properties.put("bitmap_data", hiveTable);

        new Expectations() {
            {
                loadStmt.getLabel();
                result = labelName;
                labelName.getDbName();
                result = databaseName;
                labelName.getLabelName();
                result = label;
                catalog.getDb(databaseName);
                result = database;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                dataDescription.getTableName();
                result = tableName;
                database.getTable(tableName);
                result = olapTable;
                dataDescription.getPartitionNames();
                result = null;
                database.getId();
                result = dbId;
                loadStmt.getDataProcessorDesc();
                result = dataProcessorDesc;
                loadStmt.getProperties();
                result = properties;
            }
        };

        new MockUp<Load>() {
            @Mock
            public void checkAndCreateSource(Database db, DataDescription dataDescription,
                    Map<Long, Map<Long, List<Source>>> tableToPartitionSources, EtlJobType jobType) {

            }
        };

        try {
            BulkLoadJob bulkLoadJob = BulkLoadJob.fromLoadStmt(loadStmt, originStmt);
            Assert.assertEquals(Long.valueOf(dbId), Deencapsulation.getField(bulkLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(bulkLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(bulkLoadJob, "state"));
            Assert.assertEquals(EtlJobType.SPARK, Deencapsulation.getField(bulkLoadJob, "jobType"));
            Assert.assertEquals(hiveTable, ((SparkLoadJob) bulkLoadJob).getHiveTableName());
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }
}
