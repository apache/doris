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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class AlterRoutineLoadCommandTest {
    @Mocked
    private Env env;

    @Mocked
    private ConnectContext connectContext;

    private void runBefore() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidate() {
        runBefore();
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        jobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY, "100");
        jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.01");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY, "10");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY, "300000");
        jobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY, "1048576000");
        jobProperties.put(CreateRoutineLoadInfo.STRICT_MODE, "false");
        jobProperties.put(CreateRoutineLoadInfo.TIMEZONE, "Asia/Shanghai");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        LabelNameInfo labelNameInfo = new LabelNameInfo("testDb", "label1");

        AlterRoutineLoadCommand command = new AlterRoutineLoadCommand(labelNameInfo, jobProperties, dataSourceProperties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        Assertions.assertEquals(8, command.getAnalyzedJobProperties().size());
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.STRICT_MODE));
        Assertions.assertTrue(command.getAnalyzedJobProperties().containsKey(CreateRoutineLoadInfo.TIMEZONE));
    }
}
