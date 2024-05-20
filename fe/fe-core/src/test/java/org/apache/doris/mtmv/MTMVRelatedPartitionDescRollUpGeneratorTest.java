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

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MTMVRelatedPartitionDescRollUpGeneratorTest {
    @Mocked
    private MTMVPartitionUtil mtmvPartitionUtil;
    @Mocked
    private MTMVPartitionInfo mtmvPartitionInfo;

    @Test
    public void testRollUpRange() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, null), new StringLiteral("month")));
        new Expectations() {
            {
                mtmvPartitionUtil.getPartitionColumnType((MTMVRelatedTableIf) any, (String) any);
                minTimes = 0;
                result = Type.DATE;

                mtmvPartitionInfo.getRelatedTable();
                minTimes = 0;
                result = null;

                mtmvPartitionInfo.getExpr();
                minTimes = 0;
                result = null;

                mtmvPartitionInfo.getPartitionType();
                minTimes = 0;
                result = MTMVPartitionType.EXPR;

                mtmvPartitionInfo.getExpr();
                minTimes = 0;
                result = expr;
            }
        };
        MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
        Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
        PartitionKeyDesc desc20200101 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-01-01")),
                Lists.newArrayList(new PartitionValue("2020-01-02")));
        PartitionKeyDesc desc20200102 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-01-02")),
                Lists.newArrayList(new PartitionValue("2020-01-03")));
        PartitionKeyDesc desc20200201 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-02-01")),
                Lists.newArrayList(new PartitionValue("2020-02-02")));
        relatedPartitionDescs.put(desc20200101, Sets.newHashSet("name1"));
        relatedPartitionDescs.put(desc20200102, Sets.newHashSet("name2"));
        relatedPartitionDescs.put(desc20200201, Sets.newHashSet("name3"));
        Map<PartitionKeyDesc, Set<String>> res = generator.rollUpRange(relatedPartitionDescs,
                mtmvPartitionInfo);

        PartitionKeyDesc expectDesc202001 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-01-01")),
                Lists.newArrayList(new PartitionValue("2020-02-01")));
        PartitionKeyDesc expectDesc202002 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-02-01")),
                Lists.newArrayList(new PartitionValue("2020-03-01")));
        Assert.assertEquals(2, res.size());
        Assert.assertEquals(Sets.newHashSet("name1", "name2"), res.get(expectDesc202001));
        Assert.assertEquals(Sets.newHashSet("name3"), res.get(expectDesc202002));
    }

    @Test
    public void testRollUpList() throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, null), new StringLiteral("month")));
        new Expectations() {
            {
                mtmvPartitionUtil.getPartitionColumnType((MTMVRelatedTableIf) any, (String) any);
                minTimes = 0;
                result = Type.DATE;

                mtmvPartitionInfo.getRelatedTable();
                minTimes = 0;
                result = null;

                mtmvPartitionInfo.getExpr();
                minTimes = 0;
                result = null;

                mtmvPartitionInfo.getPartitionType();
                minTimes = 0;
                result = MTMVPartitionType.EXPR;

                mtmvPartitionInfo.getExpr();
                minTimes = 0;
                result = expr;
            }
        };
        MTMVRelatedPartitionDescRollUpGenerator generator = new MTMVRelatedPartitionDescRollUpGenerator();
        Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs = Maps.newHashMap();
        relatedPartitionDescs.put(generateInDesc("2020-01-01"), Sets.newHashSet("name1"));
        relatedPartitionDescs.put(generateInDesc("2020-01-02"), Sets.newHashSet("name2"));
        relatedPartitionDescs.put(generateInDesc("2020-02-01"), Sets.newHashSet("name3"));
        Map<PartitionKeyDesc, Set<String>> res = generator.rollUpList(relatedPartitionDescs,
                mtmvPartitionInfo, Maps.newHashMap());

        PartitionKeyDesc expectDesc202001 = generateInDesc("2020-01-01", "2020-01-02");
        PartitionKeyDesc expectDesc202002 = generateInDesc("2020-02-01");
        Assert.assertEquals(2, res.size());
        Assert.assertEquals(Sets.newHashSet("name1", "name2"), res.get(expectDesc202001));
        Assert.assertEquals(Sets.newHashSet("name3"), res.get(expectDesc202002));
    }

    private PartitionKeyDesc generateInDesc(String... values) {
        List<List<PartitionValue>> partitionValues = Lists.newArrayList();
        for (String value : values) {
            List<PartitionValue> partitionValue = Lists.newArrayList(new PartitionValue(value));
            partitionValues.add(partitionValue);
        }
        return PartitionKeyDesc.createIn(partitionValues);
    }
}
