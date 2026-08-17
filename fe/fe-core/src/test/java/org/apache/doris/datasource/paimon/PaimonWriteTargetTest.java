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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class PaimonWriteTargetTest {

    @Test
    public void testVariantColumnIsAvailableToWriteBinding() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));

        DataType type = DataType.fromCatalogType(target.getColumnTypes().get("payload"));
        Assert.assertTrue(type instanceof VariantType);
        Assert.assertTrue(((VariantType) type).isComputeV2());
    }

    @Test
    public void testNestedVariantUsesComputeV2() throws Exception {
        PaimonWriteTarget target = createTarget(
                DataTypes.FIELD(0, "items", DataTypes.ARRAY(DataTypes.VARIANT())),
                DataTypes.FIELD(1, "record", DataTypes.ROW(
                        DataTypes.FIELD(2, "payload", DataTypes.VARIANT()))));

        ArrayType arrayType = (ArrayType) DataType.fromCatalogType(
                target.getColumnTypes().get("items"));
        StructType structType = (StructType) DataType.fromCatalogType(
                target.getColumnTypes().get("record"));
        Assert.assertTrue(((VariantType) arrayType.getItemType()).isComputeV2());
        Assert.assertTrue(((VariantType) structType.getFields().get(0).getDataType()).isComputeV2());
    }

    @Test
    public void testCaseInsensitiveColumnCollisionIsRejected() {
        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class,
                () -> createTarget(
                        DataTypes.FIELD(0, "payload", DataTypes.INT()),
                        DataTypes.FIELD(1, "PAYLOAD", DataTypes.BIGINT())));

        Assert.assertTrue(exception.getMessage().contains(
                "columns which differ only by case: payload and PAYLOAD"));
    }

    private static PaimonWriteTarget createTarget(
            org.apache.paimon.types.DataField... fields) throws Exception {
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
