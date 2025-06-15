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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class AlterRoutineLoadCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    public void runBefore() throws IOException {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws MetaNotFoundException, IOException {
        runBefore();
//        new Expectations() {
//            {
//                Env.getCurrentEnv().getRoutineLoadManager().getJob("db1", anyString);
//                minTimes = 0;
//                result = new KafkaRoutineLoadJob();
//            }
//        };
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "100");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY, "200000");
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("property.client.id", "101");
        dataSourceProperties.put("property.group.id", "mygroup");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "1,2,3");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000, 30000");

        LabelNameInfo labelNameInfo = new LabelNameInfo("db1", "label1");
        Database dataBase = new Database(0, "db1");
        Env.getCurrentInternalCatalog().unprotectCreateDb(dataBase);

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        Assertions.assertEquals(2, command.getAnalyzedJobProperties().size());
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY));
        Assertions.assertTrue(command.hasDataSourceProperty());

        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) command.getDataSourceProperties();
        Assertions.assertEquals(2, kafkaDataSourceProperties.getCustomKafkaProperties().size());
        Assertions.assertTrue(kafkaDataSourceProperties.getCustomKafkaProperties().containsKey("group.id"));
        Assertions.assertTrue(kafkaDataSourceProperties.getCustomKafkaProperties().containsKey("client.id"));
        Assertions.assertEquals(3, kafkaDataSourceProperties.getKafkaPartitionOffsets().size());
    }
}
