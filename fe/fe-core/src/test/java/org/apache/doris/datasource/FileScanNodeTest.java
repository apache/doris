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

package org.apache.doris.datasource;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPartitionKeyValue;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FileScanNodeTest {

    @Test
    public void testFillTablePartitionContextOnlySetsPartitionName() {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hms_ctl.db.tbl");

        TFileRangeDesc range = new TFileRangeDesc();
        FileScanNode.fillTablePartitionContext(range, table, "dt=20260319");

        Assert.assertEquals("dt=20260319", range.getPartitionName());
    }

    @Test
    public void testFillTablePartitionContextSkipsEmptyPartitionName() {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hms_ctl.db.tbl");

        TFileRangeDesc range = new TFileRangeDesc();
        FileScanNode.fillTablePartitionContext(range, table, "");

        Assert.assertFalse(range.isSetPartitionName());
    }

    @Test
    public void testBuildPartitionName() {
        Assert.assertEquals("country=cn/dt=20260319",
                FileScanNode.buildPartitionName(Arrays.asList("country", "dt"),
                        Arrays.asList("cn", "20260319")));
        Assert.assertEquals("", FileScanNode.buildPartitionName(Collections.singletonList("dt"), null));
        Assert.assertEquals("", FileScanNode.buildPartitionName(Collections.singletonList("dt"),
                Arrays.asList("20260319", "extra")));
        Assert.assertEquals("", FileScanNode.buildPartitionName(Collections.emptyList(),
                Collections.singletonList("20260319")));
    }

    @Test
    public void testFillTablePartitionContextWithPartitionKeyValues() {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hms_ctl.db.tbl");

        TFileRangeDesc range = new TFileRangeDesc();
        List<TPartitionKeyValue> partitionKeyValues = FileScanNode.buildPartitionKeyValues(
                Arrays.asList("country", "dt"), Arrays.asList("cn", "20260319"));

        FileScanNode.fillTablePartitionContext(range, table, partitionKeyValues);

        Assert.assertEquals("country=cn/dt=20260319", range.getPartitionName());
        Assert.assertEquals(2, range.getPartitionValuesSize());
        Assert.assertEquals("country", range.getPartitionValues().get(0).getKey());
        Assert.assertEquals("cn", range.getPartitionValues().get(0).getValue());
        Assert.assertEquals("dt", range.getPartitionValues().get(1).getKey());
        Assert.assertEquals("20260319", range.getPartitionValues().get(1).getValue());
    }

    @Test
    public void testBuildPartitionKeyValuesFromMapUsesStableKeyOrder() {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", "20260319");
        partitionValues.put("country", "cn");
        partitionValues.put("city", null);

        List<TPartitionKeyValue> partitionKeyValues = FileScanNode.buildPartitionKeyValues(partitionValues);

        Assert.assertEquals("city=/country=cn/dt=20260319",
                FileScanNode.buildPartitionName(partitionKeyValues));
        Assert.assertEquals("city", partitionKeyValues.get(0).getKey());
        Assert.assertEquals("", partitionKeyValues.get(0).getValue());
        Assert.assertTrue(partitionKeyValues.get(0).isIsNull());
        Assert.assertEquals("country", partitionKeyValues.get(1).getKey());
        Assert.assertEquals("dt", partitionKeyValues.get(2).getKey());
    }
}
