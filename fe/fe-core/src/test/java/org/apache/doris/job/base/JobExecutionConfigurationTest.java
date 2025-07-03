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

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.common.IntervalUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JobExecutionConfigurationTest {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionConfigurationTest.class);

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
        timerDefinition.setStartTimeMs(700000L); // Start time set to 700 second in the future
        timerDefinition.setInterval(10L); // Interval set to 10 minute
        timerDefinition.setIntervalUnit(IntervalUnit.MINUTE);
        configuration.setTimerDefinition(timerDefinition);

        List<Long> delayTimes = configuration.getTriggerDelayTimes(
                0L, 0L, 1100000L);
        // test should filter result which smaller than start time
        Assertions.assertEquals(1, delayTimes.size());
        Assertions.assertArrayEquals(new Long[]{700L}, delayTimes.toArray());

        timerDefinition.setStartTimeMs(100000L); // Start time set to 100 second in the future
        delayTimes = configuration.getTriggerDelayTimes(
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
        Assertions.assertEquals(2, configuration.getTriggerDelayTimes(second * 5 + 10L, second * 3, second * 7).size());
        Assertions.assertEquals(2, configuration.getTriggerDelayTimes(second * 5, second * 5, second * 7).size());
        timerDefinition.setStartTimeMs(1672531200000L);
        timerDefinition.setIntervalUnit(IntervalUnit.MINUTE);
        timerDefinition.setInterval(1L);

        List<Long> expectDelayTimes = configuration.getTriggerDelayTimes(1672531200000L, 1672531200000L, 1672531850000L);

        Assertions.assertArrayEquals(new Long[]{0L, 60L, 120L, 180L, 240L, 300L, 360L, 420L, 480L, 540L, 600L}, expectDelayTimes.toArray());
        timerDefinition.setIntervalUnit(IntervalUnit.MINUTE);
        timerDefinition.setInterval(1L);
        timerDefinition.setStartTimeMs(1577808000000L);
        // Log detailed time information
        LOG.info("Current time is: "
                + TimeUtils.longToTimeStringWithms(1736459699000L));
        LOG.info("Start time window is: "
                + TimeUtils.longToTimeStringWithms(1736459698000L));
        LOG.info("Latest batch scheduler timer task time is: "
                + TimeUtils.longToTimeStringWithms(1736460299000L));

        // Get and log trigger delay times
        delayTimes = configuration.getTriggerDelayTimes(1736459699000L, 1736459698000L,
                1736460299000L);
        Assertions.assertEquals(10, delayTimes.size());
        LOG.info("Trigger delay times size: " + delayTimes.size());
        delayTimes.forEach(a -> LOG.info(TimeUtils.longToTimeStringWithms(a * 1000 + 1736459699000L)));

        LOG.info("----");

        // Log detailed time information
        LOG.info("Current time is: "
                + TimeUtils.longToTimeStringWithms(1736460901000L));
        LOG.info("Start time window is: "
                + TimeUtils.longToTimeStringWithms(1736460900000L));
        LOG.info("Latest batch scheduler timer task time is: "
                + TimeUtils.longToTimeStringWithms(1736461501000L));

        // Get and log trigger delay times
        delayTimes = configuration.getTriggerDelayTimes(1736460901000L, 1736460900000L,
                1736461501000L);
        Assertions.assertEquals(11, delayTimes.size());
        LOG.info("Trigger delay times size: " + delayTimes.size());
        delayTimes.forEach(a -> LOG.info(TimeUtils.longToTimeStringWithms(a * 1000 + 1736460901000L)));

        LOG.info("----");

        // Log detailed time information
        LOG.info("Current time is: " + TimeUtils.longToTimeStringWithms(1736461502000L));
        LOG.info("Start time window is: " + TimeUtils.longToTimeStringWithms(1736461501000L));
        LOG.info("Latest batch scheduler timer task time is: "
                + TimeUtils.longToTimeStringWithms(1736462102000L));

        // Get and log trigger delay times
        delayTimes = configuration.getTriggerDelayTimes(1736461502000L, 1736461501000L,
                1736462102000L);
        Assertions.assertEquals(10, delayTimes.size());
        LOG.info("Trigger delay times size: " + delayTimes.size());
        delayTimes.forEach(a -> LOG.info(TimeUtils.longToTimeStringWithms(a * 1000 + 1736461502000L)));
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
