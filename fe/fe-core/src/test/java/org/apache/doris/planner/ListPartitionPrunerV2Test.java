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

package org.apache.doris.planner;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListPartitionPrunerV2Test {
    @Test
    public void testPartitionValuesMap() throws AnalysisException {
        List<PartitionValue> partitionValues = new ArrayList<>();
        partitionValues.add(new PartitionValue("1.123000"));
        ArrayList<Type> types = new ArrayList<>();
        types.add(ScalarType.DOUBLE);

        // for hive table
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(partitionValues, types, true);
        ListPartitionItem listPartitionItem = new ListPartitionItem(Lists.newArrayList(key));
        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionValues.size());
        idToPartitionItem.put(1L, listPartitionItem);

        // for olap table
        PartitionKey key2 = PartitionKey.createListPartitionKeyWithTypes(partitionValues, types, false);
        ListPartitionItem listPartitionItem2 = new ListPartitionItem(Lists.newArrayList(key2));
        idToPartitionItem.put(2L, listPartitionItem2);

        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        Assert.assertEquals("1.123000", partitionValuesMap.get(1L).get(0));
        Assert.assertEquals("1.123", partitionValuesMap.get(2L).get(0));
    }
}
