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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;


public class MTMVRelatedPartitionDescSyncLimitGeneratorTest {
    @Mocked
    private DateTimeAcquire dateTimeAcquire;

    @Test
    public void testGenerateMTMVPartitionSyncConfigByProperties() throws AnalysisException {
        MTMVRelatedPartitionDescSyncLimitGenerator generator = new MTMVRelatedPartitionDescSyncLimitGenerator();
        Map<String, String> mvProperties = Maps.newHashMap();
        MTMVPartitionSyncConfig config = generator
                .generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assert.assertEquals(-1, config.getSyncLimit());
        Assert.assertFalse(config.getDateFormat().isPresent());
        Assert.assertEquals(MTMVPartitionSyncTimeUnit.DAY, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT, "1");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assert.assertEquals(1, config.getSyncLimit());
        Assert.assertFalse(config.getDateFormat().isPresent());
        Assert.assertEquals(MTMVPartitionSyncTimeUnit.DAY, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT, "month");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assert.assertEquals(1, config.getSyncLimit());
        Assert.assertFalse(config.getDateFormat().isPresent());
        Assert.assertEquals(MTMVPartitionSyncTimeUnit.MONTH, config.getTimeUnit());

        mvProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT, "%Y%m%d");
        config = generator.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        Assert.assertEquals(1, config.getSyncLimit());
        Assert.assertEquals("%Y%m%d", config.getDateFormat().get());
        Assert.assertEquals(MTMVPartitionSyncTimeUnit.MONTH, config.getTimeUnit());
    }

    @Test
    public void testGetNowTruncSubSec() throws AnalysisException {
        MTMVRelatedPartitionDescSyncLimitGenerator generator = new MTMVRelatedPartitionDescSyncLimitGenerator();
        DateTimeLiteral dateTimeLiteral = new DateTimeLiteral("2020-02-03 20:10:10");
        new Expectations() {
            {
                dateTimeAcquire.now();
                minTimes = 0;
                result = dateTimeLiteral;
            }
        };
        long nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.DAY, 1);
        // 2020-02-03
        Assert.assertEquals(1580659200L, nowTruncSubSec);
        nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.MONTH, 1);
        // 2020-02-01
        Assert.assertEquals(1580486400L, nowTruncSubSec);
        nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.YEAR, 1);
        // 2020-01-01
        Assert.assertEquals(1577808000L, nowTruncSubSec);
        nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.MONTH, 3);
        // 2019-12-01
        Assert.assertEquals(1575129600L, nowTruncSubSec);
        nowTruncSubSec = generator.getNowTruncSubSec(MTMVPartitionSyncTimeUnit.DAY, 4);
        // 2020-01-31
        Assert.assertEquals(1580400000L, nowTruncSubSec);
    }
}
