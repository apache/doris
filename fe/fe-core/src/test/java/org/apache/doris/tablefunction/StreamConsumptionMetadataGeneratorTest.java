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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.stream.TableStreamManager;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class StreamConsumptionMetadataGeneratorTest {

    @Test
    public void testNoConjunctsUseUnfilteredScanPath() throws Exception {
        Env env = Mockito.mock(Env.class);
        TableStreamManager manager = Mockito.mock(TableStreamManager.class);
        Mockito.when(env.getTableStreamManager()).thenReturn(manager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest()
                    .setSchemaTableName(TSchemaTableName.TABLE_STREAM_CONSUMPTION)
                    .setSchemaTableParams(new TSchemaTableRequestParams());
            TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(request);

            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Mockito.verify(manager).fillStreamConsumptionValuesMetadataResult(Mockito.anyList());
            Mockito.verify(manager, Mockito.never()).fillStreamConsumptionValuesMetadataResult(
                    Mockito.anyList(), Mockito.any(TableStreamManager.StreamConsumptionSelector.class));
        }
    }

    @Test
    public void testStreamConjunctIsNotReevaluatedForUnit() throws Exception {
        TableStreamManager.StreamConsumptionSelector selector = captureSelector(List.of(
                new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(null, "DB_NAME"), new StringLiteral("db1"))));

        Assertions.assertFalse(selector.hasUnitFilter());
        Assertions.assertFalse(selector.test("other_db", "s1", 1, null));
        Assertions.assertTrue(selector.test("other_db", "s1", 1, "p1"));
    }

    @Test
    public void testCrossLevelOrIsEvaluatedWithUnit() throws Exception {
        Expr dbPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "DB_NAME"), new StringLiteral("db1"));
        Expr unitPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, "UNIT"), new StringLiteral("p1"));
        TableStreamManager.StreamConsumptionSelector selector = captureSelector(List.of(
                new CompoundPredicate(CompoundPredicate.Operator.OR, dbPredicate, unitPredicate)));

        Assertions.assertTrue(selector.hasUnitFilter());
        Assertions.assertTrue(selector.test("other_db", "s1", 1, null));
        Assertions.assertTrue(selector.test("db1", "s1", 1, "p2"));
        Assertions.assertTrue(selector.test("other_db", "s1", 1, "p1"));
        Assertions.assertFalse(selector.test("other_db", "s1", 1, "p2"));
    }

    private TableStreamManager.StreamConsumptionSelector captureSelector(List<Expr> conjuncts) throws Exception {
        Env env = Mockito.mock(Env.class);
        TableStreamManager manager = Mockito.mock(TableStreamManager.class);
        Mockito.when(env.getTableStreamManager()).thenReturn(manager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TSchemaTableRequestParams params = new TSchemaTableRequestParams();
            params.setFrontendConjuncts(GsonUtils.GSON.toJson(conjuncts));
            TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(
                    new TFetchSchemaTableDataRequest()
                            .setSchemaTableName(TSchemaTableName.TABLE_STREAM_CONSUMPTION)
                            .setSchemaTableParams(params));

            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            ArgumentCaptor<TableStreamManager.StreamConsumptionSelector> selectorCaptor =
                    ArgumentCaptor.forClass(TableStreamManager.StreamConsumptionSelector.class);
            Mockito.verify(manager).fillStreamConsumptionValuesMetadataResult(
                    Mockito.anyList(), selectorCaptor.capture());
            return selectorCaptor.getValue();
        }
    }
}
