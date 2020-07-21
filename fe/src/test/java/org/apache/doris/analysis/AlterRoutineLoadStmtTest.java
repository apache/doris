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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import mockit.Expectations;
import mockit.Mocked;

/*
 * Author: Chenmingyu
 * Date: Jul 20, 2020
 */

public class AlterRoutineLoadStmtTest {

    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, "200000");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put("property.client.id", "101");
            dataSourceProperties.put("property.group.id", "mygroup");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10000, 20000, 30000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
            } catch (UserException e) {
                Assert.fail();
            }

            Assert.assertEquals(2, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
            Assert.assertTrue(stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
            Assert.assertEquals(2, stmt.getCustomKafkaProperties().size());
            Assert.assertTrue(stmt.getCustomKafkaProperties().containsKey("group.id"));
            Assert.assertTrue(stmt.getCustomKafkaProperties().containsKey("client.id"));
            Assert.assertEquals(3, stmt.getKafkaPartitionOffsets().size());
        }
    }

    @Test(expected = AnalysisException.class)
    public void testNoPproperties() throws AnalysisException, UserException {
        Map<String, String> jobProperties = Maps.newHashMap();
        String typeName = "kafka";
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                jobProperties, typeName, dataSourceProperties);
        stmt.analyze(analyzer);
    }

    @Test
    public void testUnsupportedProperties() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("format is invalid property"));
            } catch (UserException e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_TOPIC_PROPERTY, "new_topic");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Do not support modify kafka custom property: kafka_topic"));
            } catch (UserException e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Must also specify partition offset"));
            } catch (UserException e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000, 2000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
            } catch (UserException e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "1000, 2000, 3000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Must also specify kafka partition"));
            } catch (UserException e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY, "200000");
            String typeName = "kafka";
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put("property.client.id", "101");
            dataSourceProperties.put("property.group.id", "mygroup");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY, "1,2,3");
            dataSourceProperties.put(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY, "10000, 20000, 30000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, typeName, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("max_batch_size should between 100MB and 1GB"));
            } catch (UserException e) {
                Assert.fail();
            }
        }
    }

}
