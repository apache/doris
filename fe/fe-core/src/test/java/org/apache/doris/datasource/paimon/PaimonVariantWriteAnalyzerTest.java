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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class PaimonVariantWriteAnalyzerTest {

    @Test
    public void testDisabledVariantV2IsRejectedDuringAnalysis() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(target, VariantType.COMPUTE_V2_INSTANCE, false));
        Assert.assertTrue(exception.getMessage().contains("enable_variant_v2=true"));
    }

    @Test
    public void testLegacyVariantInputIsRejectedDuringAnalysis() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(target, VariantType.INSTANCE, true));
        Assert.assertTrue(exception.getMessage().contains("Variant V1"));
        Assert.assertTrue(exception.getMessage().contains("payload"));
    }

    @Test
    public void testComputeV2InputIsAccepted() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        PaimonVariantWriteAnalyzer.validate(
                target,
                Collections.singletonList(target.getColumn("payload")),
                outputs("payload", VariantType.COMPUTE_V2_INSTANCE),
                true);
    }

    @Test
    public void testNestedLegacyVariantInputIsRejected() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.ARRAY(DataTypes.VARIANT())));

        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> validate(target, ArrayType.of(VariantType.INSTANCE), true));
        Assert.assertTrue(exception.getMessage().contains("payload[]"));
    }

    @Test
    public void testNonVariantTableDoesNotRequireVariantV2() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.STRING()));

        validate(target, org.apache.doris.nereids.types.StringType.INSTANCE, false);
    }

    private static void validate(
            PaimonWriteTarget target, DataType sourceType, boolean enableVariantV2)
            throws AnalysisException {
        Column column = target.getSchema().get(0);
        PaimonVariantWriteAnalyzer.validate(
                target,
                Collections.singletonList(column),
                outputs(column.getName(), sourceType),
                enableVariantV2);
    }

    private static Map<String, NamedExpression> outputs(String name, DataType dataType) {
        NamedExpression expression = Mockito.mock(NamedExpression.class);
        Mockito.when(expression.getDataType()).thenReturn(dataType);
        Map<String, NamedExpression> outputs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        outputs.put(name, expression);
        return outputs;
    }

    private static PaimonWriteTarget createTarget(DataField... fields) throws Exception {
        PaimonExternalTable dorisTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getPaimonTableForWrite()).thenReturn(table);
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(fields));
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        return PaimonWriteTarget.create(dorisTable);
    }
}
