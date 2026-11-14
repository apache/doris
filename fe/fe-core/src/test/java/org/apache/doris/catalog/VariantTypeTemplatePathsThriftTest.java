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

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TryCastExpr;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
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

    private static void assertPath(TVariantTemplatePath path, String pattern, boolean isGlob, TPrimitiveType type) {
        Assertions.assertEquals(pattern, path.getPattern());
        Assertions.assertEquals(isGlob, path.isIsGlob());
        Assertions.assertEquals(type, path.getType());
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
        // every field rides, in place and with its own type (an ARRAY field with its item type), so the BE's
        // first match is the shredder's first match
        assertPath(paths.get(0), "amount*", true, TPrimitiveType.STRING);
        assertPath(paths.get(1), "money.amount", false, TPrimitiveType.DECIMAL128I);
        assertPath(paths.get(2), "ratio*", true, TPrimitiveType.DECIMAL128I);
        assertPath(paths.get(3), "prices", true, TPrimitiveType.DECIMAL128I);
        assertPath(paths.get(4), "score", true, TPrimitiveType.DOUBLE);
        assertPath(paths.get(5), "tags", true, TPrimitiveType.STRING);
    }

    @Test
    public void decimalFieldSendsTheWidthItsPrecisionSelects() {
        // ScalarType.getSuitableDecimalType: precision <= 9 / 18 / 38 / 76 picks DECIMAL32 / 64 / 128 / 256
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("d32", ScalarType.createDecimalV3Type(5, 2), ""));
        fields.add(new VariantField("d64", ScalarType.createDecimalV3Type(10, 2), ""));
        fields.add(new VariantField("d128", ScalarType.createDecimalV3Type(38, 10), ""));
        fields.add(new VariantField("d256", ScalarType.createDecimalV3Type(76, 10), ""));
        fields.add(new VariantField("d256s", new ArrayType(ScalarType.createDecimalV3Type(39, 0)), ""));

        List<TVariantTemplatePath> paths = scalarTypeOf(new VariantType(fields)).getVariantTemplatePaths();
        Assertions.assertEquals(5, paths.size());
        assertPath(paths.get(0), "d32", true, TPrimitiveType.DECIMAL32);
        assertPath(paths.get(1), "d64", true, TPrimitiveType.DECIMAL64);
        assertPath(paths.get(2), "d128", true, TPrimitiveType.DECIMAL128I);
        assertPath(paths.get(3), "d256", true, TPrimitiveType.DECIMAL256);
        assertPath(paths.get(4), "d256s", true, TPrimitiveType.DECIMAL256);
    }

    @Test
    public void largeIntFieldSendsTheTemplateToo() {
        // no DECIMAL anywhere: the LARGEINT field alone makes the template ride
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        fields.add(new VariantField("id", Type.LARGEINT, "", PatternType.MATCH_NAME));
        fields.add(new VariantField("n", Type.BIGINT, ""));

        TScalarType scalarType = scalarTypeOf(new VariantType(fields));
        Assertions.assertTrue(scalarType.isSetVariantTemplatePaths());
        List<TVariantTemplatePath> paths = scalarType.getVariantTemplatePaths();
        Assertions.assertEquals(3, paths.size());
        assertPath(paths.get(0), "name", true, TPrimitiveType.STRING);
        assertPath(paths.get(1), "id", false, TPrimitiveType.LARGEINT);
        assertPath(paths.get(2), "n", true, TPrimitiveType.BIGINT);

        // an ARRAY<LARGEINT> field counts through its item type, which is what rides
        ArrayList<VariantField> arrayFields = new ArrayList<>();
        arrayFields.add(new VariantField("ids", new ArrayType(Type.LARGEINT), ""));
        arrayFields.add(new VariantField("flag", Type.BOOLEAN, ""));
        List<TVariantTemplatePath> arrayPaths
                = scalarTypeOf(new VariantType(arrayFields)).getVariantTemplatePaths();
        Assertions.assertEquals(2, arrayPaths.size());
        assertPath(arrayPaths.get(0), "ids", true, TPrimitiveType.LARGEINT);
        assertPath(arrayPaths.get(1), "flag", true, TPrimitiveType.BOOLEAN);
    }

    @Test
    public void thePatternTypeRidesTheWireAndStaysOutOfAFieldsIdentity() {
        // Two fields that differ only in how the pattern matches are equal, as they were before the template
        // rode to the BE: the common type of a set operation over two such templates keeps the template.
        VariantField literal = new VariantField("a*", ScalarType.createDecimalV3Type(10, 2), "",
                PatternType.MATCH_NAME);
        VariantField glob = new VariantField("a*", ScalarType.createDecimalV3Type(10, 2), "");
        Assertions.assertEquals(literal, glob);
        Assertions.assertEquals(literal.hashCode(), glob.hashCode());
        Assertions.assertNotEquals(literal, new VariantField("b*", ScalarType.createDecimalV3Type(10, 2), ""));
        Assertions.assertNotEquals(literal, new VariantField("a*", ScalarType.createDecimalV3Type(10, 3), ""));

        // is_glob still tells the BE which is which
        ArrayList<VariantField> literalFields = new ArrayList<>();
        literalFields.add(literal);
        ArrayList<VariantField> globFields = new ArrayList<>();
        globFields.add(glob);
        assertPath(scalarTypeOf(new VariantType(literalFields)).getVariantTemplatePaths().get(0),
                "a*", false, TPrimitiveType.DECIMAL64);
        assertPath(scalarTypeOf(new VariantType(globFields)).getVariantTemplatePaths().get(0),
                "a*", true, TPrimitiveType.DECIMAL64);
    }

    @Test
    public void castTargetCarriesNoTemplate() {
        // A cast does not read by a template: parse_to_variant and a load do. The template rides on a VARIANT
        // type wherever the type goes, and the BE hands it to the type's JSON reader, which a cast of a JSON
        // string to an ARRAY of that VARIANT runs. So it is taken off the target of a CAST and of a TRY_CAST:
        // such a cast reads what it read before the template rode, and two casts that differ only in the
        // template, one cast to the FE and to the BE's condition cache, return the same.
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("a", ScalarType.createDecimalV3Type(10, 2), ""));
        VariantType templated = new VariantType(fields);
        Assertions.assertEquals(1, scalarTypeOf(templated).getVariantTemplatePaths().size());
        Type array = ArrayType.create(templated, true);
        StringLiteral text = new StringLiteral("[{\"a\":1.50}]");
        Expr[] casts = {
                new CastExpr(array, text, true),
                new TryCastExpr(array, text, true, true),
                new CastExpr(templated, text, true)};
        for (Expr cast : casts) {
            TExprNode node = ExprToThriftVisitor.treeToThrift(cast).getNodes().get(0);
            Assertions.assertTrue(node.getNodeType() == TExprNodeType.CAST_EXPR
                    || node.getNodeType() == TExprNodeType.TRY_CAST_EXPR, node.toString());
            int variants = 0;
            for (TTypeNode typeNode : node.getType().getTypes()) {
                if (typeNode.isSetScalarType()
                        && typeNode.getScalarType().getType() == TPrimitiveType.VARIANT) {
                    variants++;
                    Assertions.assertFalse(typeNode.getScalarType().isSetVariantTemplatePaths(), node.toString());
                }
            }
            Assertions.assertEquals(1, variants, node.toString());
        }
        // the type itself still sends it: the type of a slot and of a parse call is not touched
        Assertions.assertEquals(1, scalarTypeOf(templated).getVariantTemplatePaths().size());
    }

    @Test
    public void timestampNsFieldNeverRefusesTheType() {
        // PrimitiveType#toThrift refuses TIMESTAMP_NS below TIMESTAMP_NS_MIN_BE_EXEC_VERSION, to keep a value of
        // it from a BE that does not know the type. The template only names the type. A template that sends
        // nothing maps no field at all, and one that rides names the field for a BE that knows it.
        int execVersion = Config.be_exec_version;
        Config.be_exec_version = Config.TIMESTAMP_NS_MIN_BE_EXEC_VERSION - 1;
        try {
            Assertions.assertThrows(IllegalStateException.class, PrimitiveType.TIMESTAMP_NS::toThrift);

            ArrayList<VariantField> plain = new ArrayList<>();
            plain.add(new VariantField("t", Type.TIMESTAMP_NS, ""));
            VariantType sendsNothing = new VariantType(plain);
            Assertions.assertFalse(sendsNothing.readsNumbersFromText());
            Assertions.assertFalse(scalarTypeOf(sendsNothing).isSetVariantTemplatePaths());

            ArrayList<VariantField> withDecimal = new ArrayList<>();
            withDecimal.add(new VariantField("t", Type.TIMESTAMP_NS, ""));
            withDecimal.add(new VariantField("amount", ScalarType.createDecimalV3Type(10, 2), ""));
            VariantType rides = new VariantType(withDecimal);
            Assertions.assertTrue(rides.readsNumbersFromText());
            List<TVariantTemplatePath> paths = scalarTypeOf(rides).getVariantTemplatePaths();
            Assertions.assertEquals(2, paths.size());
            assertPath(paths.get(0), "t", true, TPrimitiveType.TIMESTAMP_NS);
            assertPath(paths.get(1), "amount", true, TPrimitiveType.DECIMAL64);
        } finally {
            Config.be_exec_version = execVersion;
        }
    }

    @Test
    public void typesWithoutDecimalOrLargeIntFieldsLeaveTheFieldUnset() {
        Assertions.assertFalse(scalarTypeOf(new VariantType()).isSetVariantTemplatePaths());

        // STRING, INT, BIGINT, DOUBLE and their arrays read a JSON number the way the parser always did
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        fields.add(new VariantField("count", Type.INT, ""));
        fields.add(new VariantField("total", Type.BIGINT, ""));
        fields.add(new VariantField("score", Type.DOUBLE, ""));
        fields.add(new VariantField("counts", new ArrayType(Type.BIGINT), ""));
        VariantType type = new VariantType(fields);
        Assertions.assertFalse(type.readsNumbersFromText());
        Assertions.assertTrue(type.collectTemplatePaths().isEmpty());
        TScalarType scalarType = scalarTypeOf(type);
        Assertions.assertFalse(scalarType.isSetVariantTemplatePaths());
        Assertions.assertTrue(scalarType.isVariantIsV2());
    }
}
