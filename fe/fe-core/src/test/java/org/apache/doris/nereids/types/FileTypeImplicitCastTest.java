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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class FileTypeImplicitCastTest {
    private static final String PUBLIC_STRUCT = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT,"
            + "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>";
    private static final String FILE_VALUE = "CAST(named_struct('uri', 'urn:cast', 'offset', cast(0 as "
                        + "bigint), 'size', cast(0 as bigint), 'content_type', '', 'checksum', '', "
                        + "'inline', null) AS FILE)";
    private final NereidsParser parser = new NereidsParser();
    private ConnectContext previousContext;
    private CascadesContext context;
    private ExpressionAnalyzer analyzer;

    @BeforeEach
    public void setUp() {
        previousContext = ConnectContext.get();
        context = MemoTestUtils.createCascadesContext("SELECT 1");
        // Exercise the INSERT VALUES analyzer, including its immediate FE constant folding.
        analyzer = InsertUtils.buildExprAnalyzer(context.getRewritePlan(), context);
    }

    @AfterEach
    public void tearDown() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testValuesRejectResolvedAndUnboundReverseConversions() {
        for (boolean strict : Arrays.asList(false, true)) {
            context.getConnectContext().getSessionVariable().enableStrictCast = strict;
            for (String shape : Arrays.asList("%s", "ARRAY<%s>")) {
                String sourceType = String.format(shape, "FILE");
                String source = shape.equals("%s") ? FILE_VALUE : "CAST(ARRAY(CAST(" + FILE_VALUE + " AS " + PUBLIC_STRUCT + ")) AS "
                        + sourceType + ")";
                for (String leaf : Arrays.asList("JSON", "VARIANT", PUBLIC_STRUCT)) {
                    DataType target = parser.parseDataType(String.format(shape, leaf));
                    Expression resolved = parser.parseExpression(source);
                    Assertions.assertThrows(AnalysisException.class,
                            () -> TypeCoercionUtils.castUnbound(resolved, target), source + " -> " + target);

                    Expression unbound = parser.parseExpression("element_at(CAST(named_struct('v', CAST("
                            + source + " AS " + String.format(shape, PUBLIC_STRUCT)
                            + ")) AS STRUCT<v:" + sourceType + ">), 'v')");
                    Assertions.assertThrows(UnboundException.class, unbound::getDataType);
                    Expression assignment = TypeCoercionUtils.castUnbound(unbound, target);
                    Assertions.assertFalse(((Cast) assignment).isExplicitType());
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> analyzer.analyze(assignment));
                    Assertions.assertTrue(error.getMessage().contains("Cannot implicitly convert"));

                    Expression explicit = new Cast(unbound, target, true);
                    Assertions.assertEquals(target, analyzer.analyze(
                            TypeCoercionUtils.castUnbound(explicit, target)).getDataType());
                }
            }
        }
    }

    @Test
    public void testUnboundNullFileIsCheckedBeforeFolding() {
        Expression unbound = parser.parseExpression("coalesce(CAST(NULL AS FILE), CAST(NULL AS FILE))");
        Expression assignment = TypeCoercionUtils.castUnbound(unbound, StringType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(assignment));
        Assertions.assertThrows(AnalysisException.class,
                () -> analyzer.analyze(new Cast(unbound, StringType.INSTANCE, true)));
    }

    @Test
    public void testCheckCastHonorsImplicitMarker() {
        ExpressionRuleExecutor checks = new ExpressionRuleExecutor(Collections.singletonList(
                ExpressionRewrite.bottomUp(new CheckCast())));
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(context);
        Expression file = new SlotReference("f", FileType.INSTANCE);
        Assertions.assertThrows(AnalysisException.class,
                () -> checks.rewrite(new Cast(file, StringType.INSTANCE), rewriteContext));
        Assertions.assertThrows(AnalysisException.class,
                () -> checks.rewrite(new Cast(file, StringType.INSTANCE, true), rewriteContext));
    }

    @Test
    public void testForwardFileRequiresExplicitCastAndOrdinaryAssignmentsRemainAllowed() {
        for (String source : Arrays.asList("named_struct('uri', 'urn:cast')",
                "json_parse('{\"uri\":\"urn:cast\"}')")) {
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(TypeCoercionUtils.castUnbound(
                    parser.parseExpression(source), FileType.INSTANCE)));
        }
        // ARRAY(JSON_PARSE(...)) is rejected by the ordinary ARRAY constructor before casting.
        // A typed ARRAY<JSON> source still supports recursive assignment to ARRAY<FILE>.
        for (Expression source : Arrays.asList(parser.parseExpression("array(named_struct('uri', 'urn:cast'))"),
                new SlotReference("json_array", ArrayType.of(JsonType.INSTANCE)))) {
            Assertions.assertThrows(AnalysisException.class, () -> analyzer.analyze(TypeCoercionUtils.castUnbound(
                    source, ArrayType.of(FileType.INSTANCE))));
        }
        Assertions.assertEquals(IntegerType.INSTANCE, analyzer.analyze(TypeCoercionUtils.castUnbound(
                parser.parseExpression("concat('1', '2')"), IntegerType.INSTANCE)).getDataType());
        Assertions.assertEquals(StringType.INSTANCE, analyzer.analyze(TypeCoercionUtils.castUnbound(
                parser.parseExpression("abs(-7)"), StringType.INSTANCE)).getDataType());

        Assertions.assertDoesNotThrow(() -> TypeCoercionUtils.castUnbound(
                new SlotReference("v", new VariantType(1)), new VariantType(2)));
        Assertions.assertFalse(TypeCoercionUtils.implicitCast(VariantType.INSTANCE, JsonType.INSTANCE).isPresent());
        Assertions.assertThrows(AnalysisException.class, () -> TypeCoercionUtils.castUnbound(
                new SlotReference("a", ArrayType.of(IntegerType.INSTANCE)), IntegerType.INSTANCE));
    }
}
