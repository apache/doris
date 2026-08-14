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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class MTMVRelatedPartitionDescOnePartitionColGeneratorTest {

    private static final Column DATE_COL = new Column("c1", ScalarType.createType(PrimitiveType.DATE),
            true, null, "", "");

    @Mocked
    private MTMVPartitionInfo partitionInfo;

    @Test
    public void testQueryUsedPartitionsFilter() throws Exception {
        new Expectations() {
            {
                partitionInfo.getPartitionType();
                result = MTMVPartitionType.FOLLOW_BASE_TABLE;

                partitionInfo.getRelatedColPos();
                result = 0;
            }
        };
        RelatedPartitionDescResult result = new RelatedPartitionDescResult();
        Map<String, PartitionItem> partitionItems = Maps.newHashMap();
        partitionItems.put("p1", buildRange("2021-01-01", "2021-01-02"));
        partitionItems.put("p2", buildRange("2021-01-02", "2021-01-03"));
        result.setItems(partitionItems);

        new MTMVRelatedPartitionDescOnePartitionColGenerator().apply(
                partitionInfo, Maps.newHashMap(), result, Sets.newHashSet("p2"));

        Set<String> mappedPartitions = Sets.newHashSet();
        result.getDescs().values().forEach(mappedPartitions::addAll);
        Assert.assertEquals(Sets.newHashSet("p2"), mappedPartitions);
    }

    private static RangePartitionItem buildRange(String lower, String upper) throws AnalysisException {
        PartitionKey lowerKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lower)), Lists.newArrayList(DATE_COL));
        PartitionKey upperKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upper)), Lists.newArrayList(DATE_COL));
        return new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
    }
}
