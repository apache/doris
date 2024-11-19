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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.LoadDataSourceType;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateRoutineLoadStmtTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreateRoutineLoadStmtTest.class);
    @Mocked
    Database database;

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    @Mocked
    private ConnectContext ctx;

    @Mocked
    OlapTable table;

    @Before
    public void setUp() {
        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return env;
            }
        };
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = database;

                database.getTableNullable(anyString);
                minTimes = 0;
                result = table;

                table.hasDeleteSign();
                minTimes = 0;
                result = false;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

    }

    @Test
    public void testAnalyzeWithDuplicateProperty(@Injectable Analyzer analyzer) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "http://127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        Separator columnSeparator = new Separator(",");

        // duplicate load property
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties,
                                                                                LoadTask.MergeType.APPEND, "");

        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        try {
            createRoutineLoadStmt.analyze(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testAnalyze(@Injectable Analyzer analyzer,
                            @Injectable SessionVariable sessionVariable) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String tableNameString = "table1";
        String topicName = "topic1";
        String serverAddress = "127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        String timeZone = "8:00";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        Separator columnSeparator = new Separator(",");

        // duplicate load property
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        properties.put(LoadStmt.TIMEZONE, timeZone);
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                                                                                loadPropertyList, properties,
                                                                                typeName, customProperties,
                                                                                LoadTask.MergeType.APPEND, "");
        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        new Expectations() {
            {
                ctx.getSessionVariable();
                result = sessionVariable;
                sessionVariable.getSendBatchParallelism();
                result = 1;

                sessionVariable.getTimeZone();
                result = "Asia/Hong_Kong";
            }
        };

        createRoutineLoadStmt.analyze(analyzer);

        Assert.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
        Assert.assertEquals(columnSeparator, createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator());
        Assert.assertEquals(partitionNames.getPartitionNames(), createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
        Assert.assertEquals(2, createRoutineLoadStmt.getDesiredConcurrentNum());
        Assert.assertEquals(0, createRoutineLoadStmt.getMaxErrorNum());
        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) createRoutineLoadStmt.getDataSourceProperties();
        Assert.assertEquals(serverAddress, kafkaDataSourceProperties.getBrokerList());
        Assert.assertEquals(topicName, kafkaDataSourceProperties.getTopic());
        Assert.assertEquals("+08:00", createRoutineLoadStmt.getTimezone());
    }

    @Test
    public void testMultiTableAnalyze(@Injectable Analyzer analyzer,
                                      @Injectable SessionVariable sessionVariable) throws UserException {
        String jobName = "job1";
        String dbName = "db1";
        LabelName labelName = new LabelName(dbName, jobName);
        String topicName = "topic1";
        String serverAddress = "127.0.0.1:8080";
        String kafkaPartitionString = "1,2,3";
        String timeZone = "8:00";
        List<String> partitionNameString = Lists.newArrayList();
        partitionNameString.add("p1");
        PartitionNames partitionNames = new PartitionNames(false, partitionNameString);
        Separator columnSeparator = new Separator(",");

        // duplicate load property
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        properties.put(LoadStmt.TIMEZONE, timeZone);
        String typeName = LoadDataSourceType.KAFKA.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), topicName);
        customProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), serverAddress);
        customProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), kafkaPartitionString);
        customProperties.put(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getName(), "\t");
        customProperties.put(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getName(), "2");
        customProperties.put(KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getName(), "TEXT");

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, null,
                loadPropertyList, properties,
                typeName, customProperties,
                LoadTask.MergeType.APPEND, "");
        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer1) {
                return;
            }
        };

        new Expectations() {
            {
                ctx.getSessionVariable();
                result = sessionVariable;
                sessionVariable.getSendBatchParallelism();
                result = 1;

                sessionVariable.getTimeZone();
                result = "Asia/Hong_Kong";
            }
        };

        createRoutineLoadStmt.analyze(analyzer);

        Assert.assertNotNull(createRoutineLoadStmt.getRoutineLoadDesc());
        Assert.assertEquals(columnSeparator, createRoutineLoadStmt.getRoutineLoadDesc().getColumnSeparator());
        Assert.assertEquals(partitionNames.getPartitionNames(), createRoutineLoadStmt.getRoutineLoadDesc().getPartitionNames().getPartitionNames());
        Assert.assertEquals(2, createRoutineLoadStmt.getDesiredConcurrentNum());
        Assert.assertEquals(0, createRoutineLoadStmt.getMaxErrorNum());
        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) createRoutineLoadStmt.getDataSourceProperties();
        Assert.assertEquals(serverAddress, kafkaDataSourceProperties.getBrokerList());
        Assert.assertEquals(topicName, kafkaDataSourceProperties.getTopic());
        Assert.assertEquals("+08:00", createRoutineLoadStmt.getTimezone());
        Assert.assertEquals("\t", kafkaDataSourceProperties.getTableNameProperties().get(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_DELIMITER.getName()));
        Assert.assertEquals("2", kafkaDataSourceProperties.getTableNameProperties().get(KafkaConfiguration.KAFKA_TEXT_TABLE_NAME_FIELD_INDEX.getName()));
        Assert.assertEquals("TEXT", kafkaDataSourceProperties.getTableNameProperties().get(KafkaConfiguration.KAFKA_TABLE_NAME_FORMAT.getName()));
    }

}
