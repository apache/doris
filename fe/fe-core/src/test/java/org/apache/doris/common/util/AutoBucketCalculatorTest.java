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

package org.apache.doris.common.util;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketContext;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketResult;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AutoBucketCalculatorTest {

    @Mocked
    OlapTable table;

    @Test
    public void testCalculateAutoBucketsNotAutoBucket() {
        new Expectations() {
            {
                table.isAutoBucket();
                result = false;

                table.getName();
                result = "test_table";
                minTimes = 0;

                table.getId();
                result = 1L;
                minTimes = 0;
            }
        };

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("not auto bucket table", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsExecuteFirstTime() {
        new Expectations() {
            {
                table.isAutoBucket();
                result = true;

                table.getName();
                result = "test_table";
                minTimes = 0;

                table.getId();
                result = 1L;
                minTimes = 0;
            }
        };

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", true, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("executeFirstTime", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsWithBoundsCheck() {
        new Expectations() {
            {
                table.isAutoBucket();
                result = false;

                table.getName();
                result = "test_table";
                minTimes = 0;

                table.getId();
                result = 1L;
                minTimes = 0;
            }
        };

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        int buckets = AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context);

        Assertions.assertEquals(10, buckets);
    }
}
