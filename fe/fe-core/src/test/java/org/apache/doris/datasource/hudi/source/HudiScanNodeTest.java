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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.TableFormatType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class HudiScanNodeTest {

    @Test
    public void testCopyHudiSplitIsolatesMutableState() {
        HudiSplit source = new HudiSplit(
                LocationPath.of("hdfs://host/table/file.parquet"),
                1, 2, 3, new String[] {"host-1"}, new ArrayList<>(Collections.singletonList("p1")));
        source.setModificationTime(4);
        source.setTableFormatType(TableFormatType.HUDI);
        source.setAlternativeHosts(new ArrayList<>(Collections.singletonList("host-2")));
        source.setSelfSplitWeight(5L);
        source.setTargetSplitSize(6L);
        source.setHudiDeltaLogs(new ArrayList<>(Collections.singletonList("log-1")));
        source.setHudiColumnNames(new ArrayList<>(Collections.singletonList("column-1")));
        source.setHudiColumnTypes(new ArrayList<>(Collections.singletonList("type-1")));
        source.setNestedFields(new ArrayList<>(Collections.singletonList("nested-1")));
        source.setHudiPartitionValues(new java.util.HashMap<>(ImmutableMap.of("key", "value")));

        HudiSplit copy = HudiScanNode.copyHudiSplit(source);
        copy.getHosts()[0] = "changed-host";
        copy.getPartitionValues().set(0, "changed-partition");
        copy.getAlternativeHosts().set(0, "changed-alternative-host");
        copy.getHudiDeltaLogs().set(0, "changed-log");
        copy.getHudiColumnNames().set(0, "changed-column");
        copy.getHudiColumnTypes().set(0, "changed-type");
        copy.getNestedFields().set(0, "changed-nested");
        copy.getHudiPartitionValues().put("key", "changed-value");
        copy.setTargetSplitSize(7L);

        assertNotSame(source.getHosts(), copy.getHosts());
        assertEquals(Arrays.asList("host-1"), Arrays.asList(source.getHosts()));
        assertEquals(Collections.singletonList("p1"), source.getPartitionValues());
        assertEquals(Collections.singletonList("host-2"), source.getAlternativeHosts());
        assertEquals(Collections.singletonList("log-1"), source.getHudiDeltaLogs());
        assertEquals(Collections.singletonList("column-1"), source.getHudiColumnNames());
        assertEquals(Collections.singletonList("type-1"), source.getHudiColumnTypes());
        assertEquals(Collections.singletonList("nested-1"), source.getNestedFields());
        assertEquals(ImmutableMap.of("key", "value"), source.getHudiPartitionValues());
        assertEquals(6L, source.getTargetSplitSize());
    }
}
