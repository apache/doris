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
import org.apache.doris.thrift.TVariantTemplatePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class VariantTypeTemplatePathsThriftTest {

    private static TScalarType scalarTypeOf(VariantType type) {
        TTypeDesc desc = new TTypeDesc();
        desc.setTypes(new ArrayList<>());
        type.toThrift(desc);
        return desc.getTypes().get(desc.getTypes().size() - 1).getScalarType();
    }

    private static void assertPath(TVariantTemplatePath path, String pattern, boolean isGlob, boolean isDecimal) {
        Assertions.assertEquals(pattern, path.getPattern());
        Assertions.assertEquals(isGlob, path.isIsGlob());
        Assertions.assertEquals(isDecimal, path.isIsDecimal());
    }

    @Test
    public void theWholeTemplateRidesInDeclarationOrderWhenADecimalFieldExists() {
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("amount*", ScalarType.createStringType(), ""));
        fields.add(new VariantField("money.amount", ScalarType.createDecimalV3Type(20, 6), "",
                PatternType.MATCH_NAME));
        fields.add(new VariantField("ratio*", ScalarType.createDecimalV3Type(30, 18), ""));
        fields.add(new VariantField("prices", new ArrayType(ScalarType.createDecimalV3Type(26, 9)), ""));
        fields.add(new VariantField("score", Type.DOUBLE, ""));
        fields.add(new VariantField("tags", new ArrayType(ScalarType.createStringType()), ""));

        TScalarType scalarType = scalarTypeOf(new VariantType(fields));
        Assertions.assertTrue(scalarType.isVariantIsV2());
        Assertions.assertTrue(scalarType.isSetVariantTemplatePaths());
        List<TVariantTemplatePath> paths = scalarType.getVariantTemplatePaths();
        Assertions.assertEquals(6, paths.size());
        // the non-DECIMAL fields ride too, in place, so the BE's first match is the shredder's first match
        assertPath(paths.get(0), "amount*", true, false);
        assertPath(paths.get(1), "money.amount", false, true);
        assertPath(paths.get(2), "ratio*", true, true);
        assertPath(paths.get(3), "prices", true, true);
        assertPath(paths.get(4), "score", true, false);
        assertPath(paths.get(5), "tags", true, false);
    }

    @Test
    public void thePatternTypeIsPartOfAFieldsIdentity() {
        // is_glob rides the wire from it, so two templates differing only here type a path differently
        VariantField literal = new VariantField("a*", ScalarType.createStringType(), "", PatternType.MATCH_NAME);
        VariantField glob = new VariantField("a*", ScalarType.createStringType(), "");
        Assertions.assertNotEquals(literal, glob);
        Assertions.assertNotEquals(literal.hashCode(), glob.hashCode());
        Assertions.assertEquals(literal, new VariantField("a*", ScalarType.createStringType(), "",
                PatternType.MATCH_NAME));
    }

    @Test
    public void typesWithoutDecimalFieldsLeaveTheFieldUnset() {
        Assertions.assertFalse(scalarTypeOf(new VariantType()).isSetVariantTemplatePaths());

        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        fields.add(new VariantField("score", Type.DOUBLE, ""));
        TScalarType scalarType = scalarTypeOf(new VariantType(fields));
        Assertions.assertFalse(scalarType.isSetVariantTemplatePaths());
        Assertions.assertTrue(scalarType.isVariantIsV2());
    }
}
