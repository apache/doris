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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ThriftHmsClient#convertColumnStatistics}: the byte-faithful extraction of ndv / numNulls /
 * avgColLen from a hive {@code ColumnStatisticsObj}, ported from legacy {@code HMSExternalTable.setStatData}.
 *
 * <p>WHY: the variant handling must match legacy exactly — a string column captures {@code avgColLen} (used
 * for its data-size estimate), a numeric column leaves it {@code -1} (the consumer uses the fixed slot width),
 * and a variant the legacy reader does NOT handle (boolean / binary / timestamp) must yield ndv=0 / numNulls=0
 * even though the metastore recorded a null count — otherwise a boolean column would report a spurious ndv.</p>
 */
public class ThriftHmsClientColumnStatsTest {

    @Test
    public void stringStatsCaptureAvgColLen() {
        StringColumnStatsData s = new StringColumnStatsData();
        s.setNumDVs(10);
        s.setNumNulls(2);
        s.setAvgColLen(5.0);
        s.setMaxColLen(20);
        HmsColumnStatistics stat = ThriftHmsClient.convertColumnStatistics(objWith("c", "string", s, true));
        Assertions.assertEquals(10, stat.getNdv());
        Assertions.assertEquals(2, stat.getNumNulls());
        Assertions.assertEquals(5.0, stat.getAvgColLenBytes(), 0.0);
    }

    @Test
    public void longStatsLeaveAvgColLenUnset() {
        LongColumnStatsData l = new LongColumnStatsData();
        l.setNumDVs(7);
        l.setNumNulls(1);
        HmsColumnStatistics stat = ThriftHmsClient.convertColumnStatistics(objWith("c", "bigint", l, false));
        Assertions.assertEquals(7, stat.getNdv());
        Assertions.assertEquals(1, stat.getNumNulls());
        // -1 => non-string; the consumer uses the Doris slot width.
        Assertions.assertEquals(-1, stat.getAvgColLenBytes(), 0.0);
    }

    @Test
    public void unhandledVariantYieldsZeroNdvAndNulls() {
        // Boolean is a variant legacy setStatData does not read, so ndv/numNulls stay 0 even though the
        // metastore recorded numNulls=3 — pinning legacy parity (no spurious ndv for boolean/binary/timestamp).
        BooleanColumnStatsData b = new BooleanColumnStatsData();
        b.setNumTrues(5);
        b.setNumFalses(3);
        b.setNumNulls(3);
        ColumnStatisticsData data = new ColumnStatisticsData();
        data.setBooleanStats(b);
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("c");
        obj.setColType("boolean");
        obj.setStatsData(data);
        HmsColumnStatistics stat = ThriftHmsClient.convertColumnStatistics(obj);
        Assertions.assertEquals(0, stat.getNdv());
        Assertions.assertEquals(0, stat.getNumNulls());
        Assertions.assertEquals(-1, stat.getAvgColLenBytes(), 0.0);
    }

    @Test
    public void missingStatsDataYieldsZeros() {
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName("c");
        HmsColumnStatistics stat = ThriftHmsClient.convertColumnStatistics(obj);
        Assertions.assertEquals(0, stat.getNdv());
        Assertions.assertEquals(0, stat.getNumNulls());
        Assertions.assertEquals(-1, stat.getAvgColLenBytes(), 0.0);
        Assertions.assertEquals("c", stat.getColumnName());
    }

    private static ColumnStatisticsObj objWith(String name, String type, Object variant, boolean isString) {
        ColumnStatisticsData data = new ColumnStatisticsData();
        if (isString) {
            data.setStringStats((StringColumnStatsData) variant);
        } else {
            data.setLongStats((LongColumnStatsData) variant);
        }
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName(name);
        obj.setColType(type);
        obj.setStatsData(data);
        return obj;
    }
}
