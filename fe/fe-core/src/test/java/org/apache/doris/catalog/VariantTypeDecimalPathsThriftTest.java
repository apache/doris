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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TVariantDecimalPath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class VariantTypeDecimalPathsThriftTest {

    private static TScalarType scalarTypeOf(VariantType type) {
        TTypeDesc desc = new TTypeDesc();
        desc.setTypes(new ArrayList<>());
        type.toThrift(desc);
        return desc.getTypes().get(desc.getTypes().size() - 1).getScalarType();
    }

    @Test
    public void decimalFieldsRideTheVariantScalarType() {
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("money.amount", ScalarType.createDecimalV3Type(20, 6), "",
                PatternType.MATCH_NAME));
        fields.add(new VariantField("ratio*", ScalarType.createDecimalV3Type(30, 18), ""));
        fields.add(new VariantField("prices", new ArrayType(ScalarType.createDecimalV3Type(26, 9)), ""));
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        fields.add(new VariantField("score", Type.DOUBLE, ""));
        fields.add(new VariantField("tags", new ArrayType(ScalarType.createStringType()), ""));

        TScalarType scalarType = scalarTypeOf(new VariantType(fields));
        Assertions.assertTrue(scalarType.isVariantIsV2());
        Assertions.assertTrue(scalarType.isSetVariantDecimalPaths());
        List<TVariantDecimalPath> paths = scalarType.getVariantDecimalPaths();
        Assertions.assertEquals(3, paths.size());

        Assertions.assertEquals("money.amount", paths.get(0).getPattern());
        Assertions.assertFalse(paths.get(0).isIsGlob());
        Assertions.assertEquals(20, paths.get(0).getPrecision());
        Assertions.assertEquals(6, paths.get(0).getScale());

        Assertions.assertEquals("ratio*", paths.get(1).getPattern());
        Assertions.assertTrue(paths.get(1).isIsGlob());
        Assertions.assertEquals(30, paths.get(1).getPrecision());
        Assertions.assertEquals(18, paths.get(1).getScale());

        Assertions.assertEquals("prices", paths.get(2).getPattern());
        Assertions.assertTrue(paths.get(2).isIsGlob());
        Assertions.assertEquals(26, paths.get(2).getPrecision());
        Assertions.assertEquals(9, paths.get(2).getScale());
    }

    @Test
    public void typesWithoutDecimalFieldsLeaveTheFieldUnset() {
        Assertions.assertFalse(scalarTypeOf(new VariantType()).isSetVariantDecimalPaths());

        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        fields.add(new VariantField("score", Type.DOUBLE, ""));
        TScalarType scalarType = scalarTypeOf(new VariantType(fields));
        Assertions.assertFalse(scalarType.isSetVariantDecimalPaths());
        Assertions.assertTrue(scalarType.isVariantIsV2());
    }
}
