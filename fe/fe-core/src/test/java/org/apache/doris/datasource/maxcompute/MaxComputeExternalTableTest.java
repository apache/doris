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

package org.apache.doris.datasource.maxcompute;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MaxComputeExternalTableTest {
    @Test
    public void testParsePartitionValues() {
        List<String> partitionColumns = Arrays.asList("p1", "p2");

        Assert.assertEquals(Arrays.asList("a", "b"),
                MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p1=a/p2=b"));
        Assert.assertEquals(Arrays.asList("a", "b"),
                MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p2=b/p1=a"));
    }

    @Test
    public void testParsePartitionValuesRejectsInvalidSpec() {
        List<String> partitionColumns = Arrays.asList("p1", "p2");

        Assert.assertThrows(RuntimeException.class,
                () -> MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p1=a"));
        Assert.assertThrows(RuntimeException.class,
                () -> MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p1=a/raw"));
        Assert.assertThrows(RuntimeException.class,
                () -> MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p1=a/p1=b"));
        Assert.assertThrows(RuntimeException.class,
                () -> MaxComputeExternalTable.parsePartitionValues(partitionColumns, "p1=a/p3=b"));
    }
}
