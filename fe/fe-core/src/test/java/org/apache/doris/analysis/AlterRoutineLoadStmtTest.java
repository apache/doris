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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterRoutineLoadStmtTest {

    private Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;

    @Before
    public void setUp() throws MetaNotFoundException {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        FeConstants.runningUnitTest = true;
        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        new Expectations() {
            {
                Env.getCurrentEnv().getRoutineLoadManager()
                        .getJob(anyString, anyString);
                minTimes = 0;
                result = new KafkaRoutineLoadJob();
            }
        };
        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY, "200000");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put("property.client.id", "101");
            dataSourceProperties.put("property.group.id", "mygroup");
            dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "1,2,3");
            dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000, 30000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);
            stmt.analyze(analyzer);
            Assert.assertEquals(2, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY));
            Assert.assertTrue(stmt.getAnalyzedJobProperties().containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY));
            Assert.assertTrue(stmt.hasDataSourceProperty());
            KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) stmt.getDataSourceProperties();
            Assert.assertEquals(2, kafkaDataSourceProperties.getCustomKafkaProperties().size());
            Assert.assertTrue(kafkaDataSourceProperties.getCustomKafkaProperties().containsKey("group.id"));
            Assert.assertTrue(kafkaDataSourceProperties.getCustomKafkaProperties().containsKey("client.id"));
            Assert.assertEquals(3, kafkaDataSourceProperties.getKafkaPartitionOffsets().size());
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test(expected = AnalysisException.class)
    public void testNoProperties() throws UserException {
        AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                Maps.newHashMap(), new HashMap<>());
        stmt.analyze(analyzer);
    }

    @Test
    public void testUnsupportedProperties() throws MetaNotFoundException {
        new Expectations() {
            {
                Env.getCurrentEnv().getRoutineLoadManager()
                        .getJob(anyString, anyString);
                minTimes = 0;
                result = new KafkaRoutineLoadJob();
            }
        };
        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, new HashMap<>());
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("format is invalid property"));
            } catch (UserException e) {
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE

        // alter topic is now supported
        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), "new_topic");
            //fixme: should be failed
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);

            try {
                stmt.analyze(analyzer);
            } catch (AnalysisException e) {
                Assert.fail();
            } catch (UserException e) {
                e.printStackTrace();
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE

        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "1,2,3");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Must set offset or default offset with partition property"));
            } catch (UserException e) {
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE

        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "1,2,3");
            dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "1000, 2000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
            } catch (UserException e) {
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE

        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "1000, 2000, 3000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("Partitions number should be equals to offsets number"));
            } catch (UserException e) {
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE

        { // CHECKSTYLE IGNORE THIS LINE
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY, "100");
            jobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY, "200000");
            Map<String, String> dataSourceProperties = Maps.newHashMap();
            dataSourceProperties.put("property.client.id", "101");
            dataSourceProperties.put("property.group.id", "mygroup");
            dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "1,2,3");
            dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000, 30000");
            AlterRoutineLoadStmt stmt = new AlterRoutineLoadStmt(new LabelName("db1", "label1"),
                    jobProperties, dataSourceProperties);
            try {
                stmt.analyze(analyzer);
                Assert.fail();
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("max_batch_size should between 100MB and 10GB"));
            } catch (UserException e) {
                Assert.fail();
            }
        } // CHECKSTYLE IGNORE THIS LINE
    }

}
