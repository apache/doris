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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TVariantTemplatePath;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The sink types parse_to_variant with the target column's VARIANT template for every INSERT shape, the fast
 * VALUES path included, and leaves a call outside a sink alone.
 */
public class TypeVariantTemplateParseTest extends TestWithFeService {
    private static final String DATABASE = "variant_template_parse_db";
    private static final String TABLE = "variant_template_parse_t";
    private static final String SOURCE = "variant_template_parse_src";
    private static final String DOCUMENT = "{\"money\":{\"amount\":188118222.011121933},\"name\":\"n\"}";

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        createTable("create table " + TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table " + SOURCE + " (\n"
                + "    id int,\n"
                + "    j string\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
    }

    @Test
    public void testFastValuesPathSendsTheTemplateToTheBackend() throws Exception {
        // the default session plans INSERT ... VALUES with FastInsertIntoValuesPlanner, which runs no Rewriter
        Assertions.assertTrue(connectContext.getSessionVariable().isEnableFastAnalyzeInsertIntoValues());
        List<TScalarType> singleRow = parseCallTypesOnTheWire(PlanChecker.from(connectContext).getInsertExecutor(
                "insert into " + TABLE + " values (1, parse_to_variant('" + DOCUMENT + "'))"));
        Assertions.assertEquals(1, singleRow.size());
        assertCarriesTheTemplate(singleRow.get(0));

        List<TScalarType> twoRows = parseCallTypesOnTheWire(PlanChecker.from(connectContext).getInsertExecutor(
                "insert into " + TABLE + " values (1, parse_to_variant('" + DOCUMENT + "')),"
                        + " (2, parse_to_variant('" + DOCUMENT + "'))"));
        Assertions.assertEquals(2, twoRows.size());
        for (TScalarType type : twoRows) {
            assertCarriesTheTemplate(type);
        }
    }

    @Test
    public void testInsertSelectShapesTypeTheCall() throws Exception {
        VariantType template = templateOfTargetColumn();
        String source = "select id, parse_to_variant(j) as v from " + SOURCE;
        List<String> shapes = new ArrayList<>();
        shapes.add("insert into " + TABLE + " " + source);
        shapes.add("insert into " + TABLE + " " + source + " where id > 0");
        shapes.add("insert into " + TABLE + " " + source + " union all " + source);
        shapes.add("insert into " + TABLE + " select * from (" + source + " limit 10) s");
        shapes.add("insert into " + TABLE + " " + source + " order by id");
        shapes.add("insert into " + TABLE + " with c as (" + source + ") select id, v from c");
        // a cast the statement wrote to another template is looked through: the sink's template wins
        shapes.add("insert into " + TABLE + " select id, cast(parse_to_variant(j) as variant<'name':string>)"
                + " from " + SOURCE);
        for (String sql : shapes) {
            List<ParseToVariant> calls = parseCalls(rewrittenInsertPlan(sql));
            Assertions.assertFalse(calls.isEmpty(), sql);
            for (ParseToVariant call : calls) {
                Assertions.assertEquals(template, call.getReturnType(), sql);
            }
        }
    }

    @Test
    public void testCteReferencedTwiceStaysUntyped() throws Exception {
        // the consumer's producer is not below the sink and the CTE is not inlined: a documented limitation
        String sql = "insert into " + TABLE + " with c as (select id, parse_to_variant(j) as v from " + SOURCE
                + ") select id, v from c union all select id, v from c";
        List<ParseToVariant> calls = parseCalls(rewrittenInsertPlan(sql));
        Assertions.assertFalse(calls.isEmpty());
        for (ParseToVariant call : calls) {
            Assertions.assertEquals(VariantType.INSTANCE, call.getReturnType());
        }
    }

    @Test
    public void testSelectKeepsTheCastAndTheUntypedCall() {
        // no sink: the query returns what the DOM parser read, the cast over the call is only a relabel
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select cast(parse_to_variant('" + DOCUMENT + "') as variant<'money.amount':decimal(20,6)>)")
                .rewrite()
                .getPlan();
        List<ParseToVariant> calls = parseCalls(plan);
        Assertions.assertEquals(1, calls.size());
        Assertions.assertEquals(VariantType.INSTANCE, calls.get(0).getReturnType());
        List<Cast> casts = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                casts.addAll(expression.<Cast>collectToList(Cast.class::isInstance));
            }
        });
        Assertions.assertEquals(1, casts.size());
        Assertions.assertInstanceOf(ParseToVariant.class, casts.get(0).child());
    }

    private Plan rewrittenInsertPlan(String sql) {
        createStatementCtx(sql);
        InsertIntoTableCommand command = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        return PlanChecker.from(connectContext)
                .analyze(command.getExplainPlan(connectContext))
                .rewrite()
                .getPlan();
    }

    private VariantType templateOfTargetColumn() throws Exception {
        Database database = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DATABASE);
        OlapTable table = (OlapTable) database.getTableOrMetaException(TABLE);
        return (VariantType) DataType.fromCatalogType(table.getColumn("v").getType());
    }

    private static List<ParseToVariant> parseCalls(Plan plan) {
        List<ParseToVariant> calls = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                calls.addAll(expression.<ParseToVariant>collectToList(ParseToVariant.class::isInstance));
            }
        });
        return calls;
    }

    /** The result type of every parse_to_variant node the BE would receive in a union's constant rows. */
    private static List<TScalarType> parseCallTypesOnTheWire(AbstractInsertExecutor executor) {
        List<TScalarType> types = new ArrayList<>();
        for (PlanFragment fragment : executor.getCoordinator().getFragments()) {
            for (TPlanNode node : fragment.toThrift().getPlan().getNodes()) {
                if (!node.isSetUnionNode()) {
                    continue;
                }
                for (List<TExpr> row : node.getUnionNode().getConstExprLists()) {
                    for (TExpr expr : row) {
                        for (TExprNode exprNode : expr.getNodes()) {
                            if (exprNode.isSetFn()
                                    && "parse_to_variant".equals(exprNode.getFn().getName().getFunctionName())) {
                                types.add(exprNode.getType().getTypes().get(0).getScalarType());
                            }
                        }
                    }
                }
            }
        }
        return types;
    }

    private static void assertCarriesTheTemplate(TScalarType type) {
        Assertions.assertTrue(type.isSetVariantTemplatePaths());
        List<TVariantTemplatePath> paths = type.getVariantTemplatePaths();
        Assertions.assertEquals(3, paths.size());
        Assertions.assertEquals("money.amount", paths.get(0).getPattern());
        Assertions.assertTrue(paths.get(0).isIsDecimal());
        Assertions.assertEquals("ratio*", paths.get(1).getPattern());
        Assertions.assertTrue(paths.get(1).isIsDecimal());
        Assertions.assertEquals("name", paths.get(2).getPattern());
        Assertions.assertFalse(paths.get(2).isIsDecimal());
    }
}
