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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.VariantType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class MVColumnItemTest {

    @Test
    void bareVariantCopiesAllPersistedPropertiesWithoutChangingItsExpressionType() throws Exception {
        for (boolean escaped : new boolean[] {false, true}) {
            VariantType sourceType = sourceVariantType(escaped);
            Column source = new Column("v", sourceType);
            VariantType expressionType = new VariantType();
            SlotDescriptor descriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
            descriptor.setColumn(source);
            descriptor.setType(expressionType);
            // A whole-column descriptor keeps its default null subpath, as produced by translation.
            SlotRef expression = new SlotRef(descriptor);
            MVColumnItem item = new MVColumnItem("v", expression);
            for (boolean escapeDerived : new boolean[] {false, true}) {
                Column result = item.toMVColumn(Map.of(), escapeDerived);
                assertSameStorageType(sourceType, result.getType());
                Assertions.assertSame(expressionType, expression.getType());
                Assertions.assertSame(expression, result.getDefineExpr());
            }
        }
    }

    @Test
    void derivedVariantSelectsAnOutputFormatWithoutChangingItsNeutralDefineExpression() throws Exception {
        VariantType expressionType = new VariantType();
        FunctionCallExpr expression = new FunctionCallExpr("coalesce",
                List.of(new SlotRef(expressionType, true), new SlotRef(expressionType, true)), true);
        expression.setType(expressionType);
        MVColumnItem item = new MVColumnItem("v", expression);

        Column escaped = item.toMVColumn(Map.of(), true);
        assertSameStorageType(new VariantType(0, true), escaped.getType());
        Column legacy = item.toMVColumn(Map.of(), false);
        assertSameStorageType(expressionType, legacy.getType());
        assertSameStorageType(expressionType, item.toMVColumn(Map.of()).getType());
        Assertions.assertSame(expressionType, expression.getType());
        Assertions.assertSame(expressionType, item.getType());
        Assertions.assertSame(expression, escaped.getDefineExpr());
    }

    @Test
    void extractedVariantSlotDoesNotInheritTheRootsTemplateOrNestedLayout() throws Exception {
        Column root = new Column("v", sourceVariantType(false));
        VariantType expressionType = new VariantType();
        SlotDescriptor descriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
        descriptor.setColumn(root);
        descriptor.setType(expressionType);
        descriptor.setSubColLables(List.of("obj"));
        SlotRef expression = new SlotRef(descriptor);
        MVColumnItem item = new MVColumnItem("v", expression);

        assertSameStorageType(new VariantType(0, true), item.toMVColumn(Map.of(), true).getType());
        assertSameStorageType(expressionType, item.toMVColumn(Map.of(), false).getType());
        Assertions.assertSame(expressionType, expression.getType());
    }

    private static VariantType sourceVariantType(boolean escaped) {
        return new VariantType(new ArrayList<>(List.of(
                new VariantField("obj.a", Type.STRING, "source template", PatternType.MATCH_NAME))),
                escaped ? 7 : 0, escaped, escaped ? 101 : 0, escaped ? 8 : 0,
                false, 0L, 64, !escaped, escaped);
    }

    private static void assertSameStorageType(Type expected, Type actual) {
        Assertions.assertEquals(ColumnToThrift.toThrift(new Column("v", expected)),
                ColumnToThrift.toThrift(new Column("v", actual)));
    }
}
