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

package org.apache.doris.job.base;

import org.apache.doris.job.common.IntervalUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JobExecutionConfigurationTest {

    @Test
    public void testGetTriggerDelayTimesOneTime() {
        JobExecutionConfiguration configuration = new JobExecutionConfiguration();
        configuration.setExecuteType(JobExecuteType.ONE_TIME);

        TimerDefinition timerDefinition = new TimerDefinition();
        timerDefinition.setStartTimeMs(1000L); // Start time set to 1 second in the future
        configuration.setTimerDefinition(timerDefinition);

        List<Long> delayTimes = configuration.getTriggerDelayTimes(
                0L, 0L, 5000L);

        Assertions.assertEquals(1, delayTimes.size());
        Assertions.assertEquals(1, delayTimes.get(0).longValue());
    }

    @Test
    public void testGetTriggerDelayTimesRecurring() {
        JobExecutionConfiguration configuration = new JobExecutionConfiguration();
        configuration.setExecuteType(JobExecuteType.RECURRING);

        TimerDefinition timerDefinition = new TimerDefinition();
        timerDefinition.setStartTimeMs(100000L); // Start time set to 1 second in the future
        timerDefinition.setInterval(10L); // Interval set to 10 milliseconds
        timerDefinition.setIntervalUnit(IntervalUnit.MINUTE);
        configuration.setTimerDefinition(timerDefinition);

        List<Long> delayTimes = configuration.getTriggerDelayTimes(
                0L, 0L, 1100000L);

        Assertions.assertEquals(2, delayTimes.size());
        Assertions.assertArrayEquals(new Long[]{100L, 700L}, delayTimes.toArray());
        delayTimes = configuration.getTriggerDelayTimes(
                200000L, 0L, 1100000L);
        Assertions.assertEquals(2, delayTimes.size());
        Assertions.assertArrayEquals(new Long[]{0L, 500L}, delayTimes.toArray());
        delayTimes = configuration.getTriggerDelayTimes(
                1001000L, 0L, 1000000L);
        Assertions.assertEquals(1, delayTimes.size());
        timerDefinition.setStartTimeMs(2000L);
        timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
        Assertions.assertArrayEquals(new Long[]{2L, 12L}, configuration.getTriggerDelayTimes(100000L, 100000L, 120000L).toArray());

        timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
        long second = 1000L;
        timerDefinition.setStartTimeMs(second);
        timerDefinition.setInterval(1L);
        Assertions.assertEquals(3, configuration.getTriggerDelayTimes(second * 5 + 10L, second * 3, second * 7).size());
        Assertions.assertEquals(3, configuration.getTriggerDelayTimes(second * 5, second * 5, second * 7).size());
        timerDefinition.setStartTimeMs(1672531200000L);
        timerDefinition.setIntervalUnit(IntervalUnit.MINUTE);
        timerDefinition.setInterval(1L);
        Assertions.assertArrayEquals(new Long[]{0L}, configuration.getTriggerDelayTimes(1672531800000L, 1672531200000L, 1672531800000L).toArray());

        List<Long> expectDelayTimes = configuration.getTriggerDelayTimes(1672531200000L, 1672531200000L, 1672531850000L);

        Assertions.assertArrayEquals(new Long[]{0L, 60L, 120L, 180L, 240L, 300L, 360L, 420L, 480L, 540L, 600L}, expectDelayTimes.toArray());
    }

    @Test
    public void testImmediate() {
        JobExecutionConfiguration configuration = new JobExecutionConfiguration();
        configuration.setExecuteType(JobExecuteType.ONE_TIME);
        configuration.setImmediate(true);
        TimerDefinition timerDefinition = new TimerDefinition();
        timerDefinition.setStartTimeMs(0L);
        configuration.setTimerDefinition(timerDefinition);
        configuration.checkParams();
    }

}
