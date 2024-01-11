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

package org.apache.doris.statistics;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ColStatsDataTest {
    @Test
    public void testConstructNotNull() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add("100");
        values.add("200");
        values.add("300");
        values.add("min");
        values.add("max");
        values.add("400");
        values.add("500");
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertEquals("id", data.statsId.id);
        Assertions.assertEquals(10000, data.statsId.catalogId);
        Assertions.assertEquals(20000, data.statsId.dbId);
        Assertions.assertEquals(30000, data.statsId.tblId);
        Assertions.assertEquals(0, data.statsId.idxId);
        Assertions.assertEquals("col", data.statsId.colId);
        Assertions.assertEquals(null, data.statsId.partId);
        Assertions.assertEquals(100, data.count);
        Assertions.assertEquals(200, data.ndv);
        Assertions.assertEquals(300, data.nullCount);
        Assertions.assertEquals("min", data.minLit);
        Assertions.assertEquals("max", data.maxLit);
        Assertions.assertEquals(400, data.dataSizeInBytes);
        Assertions.assertEquals("500", data.updateTime);
    }

    @Test
    public void testConstructNull() {
        List<String> values = Lists.newArrayList();
        values.add("id");
        values.add("10000");
        values.add("20000");
        values.add("30000");
        values.add("0");
        values.add("col");
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        values.add(null);
        ResultRow row = new ResultRow(values);
        ColStatsData data = new ColStatsData(row);
        Assertions.assertEquals("id", data.statsId.id);
        Assertions.assertEquals(10000, data.statsId.catalogId);
        Assertions.assertEquals(20000, data.statsId.dbId);
        Assertions.assertEquals(30000, data.statsId.tblId);
        Assertions.assertEquals(0, data.statsId.idxId);
        Assertions.assertEquals("col", data.statsId.colId);
        Assertions.assertEquals(null, data.statsId.partId);
        Assertions.assertEquals(0, data.count);
        Assertions.assertEquals(0, data.ndv);
        Assertions.assertEquals(0, data.nullCount);
        Assertions.assertEquals(null, data.minLit);
        Assertions.assertEquals(null, data.maxLit);
        Assertions.assertEquals(0, data.dataSizeInBytes);
        Assertions.assertEquals(null, data.updateTime);
    }
}
