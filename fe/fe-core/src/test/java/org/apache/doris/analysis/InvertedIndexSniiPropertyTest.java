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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class InvertedIndexSniiPropertyTest {
    @Test
    public void testSniiAcceptsVariantParentColumn() throws AnalysisException {
        InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.VARIANT,
                new HashMap<>(), TInvertedIndexFileStorageFormat.SNII);
    }

    @Test
    public void testSniiLegacyIndexColumnTypes() throws AnalysisException {
        IndexDef def = new IndexDef("snii_index", false, Lists.newArrayList("col1"),
                IndexDef.IndexType.INVERTED, new HashMap<>(), "");
        def.checkColumn(new Column("col1", Type.STRING, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", Type.FLOAT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", ArrayType.create(Type.INT), true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);
        def.checkColumn(new Column("col1", Type.VARIANT, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII);

        Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                new Column("col1", Type.JSONB, true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII));
        Assertions.assertThrows(AnalysisException.class, () -> def.checkColumn(
                new Column("col1", ArrayType.create(ArrayType.create(Type.INT)), true), KeysType.DUP_KEYS, false,
                TInvertedIndexFileStorageFormat.SNII));
    }
}
