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
import org.apache.doris.analysis.LiteralExprUtils;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MTMVPartitionExprDateTruncTest {

    @Test
    public void testGetRollUpIdentityUsesTypedPartitionLiteralValue() throws AnalysisException {
        MTMVPartitionExprDateTrunc service = createService("month");
        java.util.List<java.util.List<PartitionValue>> values = Lists.newArrayList();
        values.add(Lists.newArrayList(partitionValue("2024-01-16", "not-a-datetime")));
        PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createIn(values);

        String identity = service.getRollUpIdentity(partitionKeyDesc, Maps.newHashMap());

        Assert.assertEquals("2024-01-01 00:00:00", identity);
    }

    @Test
    public void testGenerateRollUpPartitionKeyDescUsesTypedPartitionLiteralValue() throws AnalysisException {
        MTMVPartitionExprDateTrunc service = createService("month");
        MTMVPartitionInfo mtmvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        MTMVRelatedTableIf pctTable = Mockito.mock(MTMVRelatedTableIf.class);
        Mockito.when(mtmvPartitionInfo.getPartitionColByPctTable(pctTable)).thenReturn("dt");

        PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(
                Lists.newArrayList(partitionValue("2024-01-16", "bad-lower")),
                Lists.newArrayList(partitionValue("2024-01-17", "bad-upper")));

        try (MockedStatic<MTMVPartitionUtil> mockedUtil = Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mockedUtil.when(() -> MTMVPartitionUtil.getPartitionColumnType(pctTable, "dt")).thenReturn(Type.DATE);

            PartitionKeyDesc result = service.generateRollUpPartitionKeyDesc(partitionKeyDesc,
                    mtmvPartitionInfo, pctTable);

            Assert.assertEquals("2024-01-01", result.getLowerValues().get(0).getStringValue());
            Assert.assertEquals("2024-02-01", result.getUpperValues().get(0).getStringValue());
        }
    }

    private MTMVPartitionExprDateTrunc createService(String timeUnit) throws AnalysisException {
        FunctionCallExpr expr = new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, null), new StringLiteral(timeUnit)), true);
        return new MTMVPartitionExprDateTrunc(expr);
    }

    private PartitionValue partitionValue(String typedValue, String rawStringValue) throws AnalysisException {
        return new PartitionValue(LiteralExprUtils.createLiteral(typedValue, Type.DATE), false, rawStringValue);
    }
}
