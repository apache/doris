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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TryParseToVariant;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TVariantTemplatePath;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The sink types parse_to_variant with the target column's VARIANT template for every INSERT shape, the fast
 * VALUES path included, and leaves a call outside a sink alone.
 */
public class TypeVariantTemplateParseTest extends TestWithFeService {
    private static final String DATABASE = "variant_template_parse_db";
    private static final String TABLE = "variant_template_parse_t";
    private static final String SOURCE = "variant_template_parse_src";
    private static final String UNIQUE_TABLE = "variant_template_parse_mow";
    private static final String DOCUMENT = "{\"money\":{\"amount\":188118222.011121933},\"name\":\"n\"}";

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        connectContext.setDatabase(DATABASE);
        // the source table holds no rows here: PRUNE_EMPTY_PARTITION would turn its scan into an empty relation and
        // drop the projection that carries the parse call before any assertion could see it
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("create table " + TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table " + UNIQUE_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>\n"
                + ")\n"
                + "unique key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\", \"enable_unique_key_merge_on_write\"=\"true\")");
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
                // the translator sends the signature's type, not the field
                Assertions.assertEquals(template, call.getDataType(), sql);
                // the typed call is told apart from the plain one in a printed plan
                Assertions.assertTrue(call.toString().contains("template<'money.amount':"), call.toString());
                Assertions.assertTrue(call.shapeInfo().contains("template<'money.amount':"), call.shapeInfo());
            }
        }
    }

    @Test
    public void testCallsUnderAnExpressionThatReturnsAnArgumentAreTyped() throws Exception {
        VariantType template = templateOfTargetColumn();
        String from = " from " + SOURCE;
        List<String> shapes = new ArrayList<>();
        shapes.add("insert into " + TABLE + " select id, if(id > 0, parse_to_variant(j), null)" + from);
        shapes.add("insert into " + TABLE + " select id, case when id > 0 then parse_to_variant(j)"
                + " when id < 0 then try_parse_to_variant(j) else parse_to_variant(j) end" + from);
        shapes.add("insert into " + TABLE + " select id, coalesce(try_parse_to_variant(j), parse_to_variant(j))"
                + from);
        shapes.add("insert into " + TABLE + " select id, nvl(try_parse_to_variant(j), parse_to_variant(j))" + from);
        // MERGE INTO selects every value it assigns with a short-circuit IF of its own
        shapes.add("merge into " + UNIQUE_TABLE + " t using " + SOURCE + " s on t.id = s.id"
                + " when matched then update set v = parse_to_variant(s.j)"
                + " when not matched then insert (id, v) values (s.id, try_parse_to_variant(s.j))");
        for (String sql : shapes) {
            List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
            Assertions.assertFalse(types.isEmpty(), sql);
            for (VariantType type : types) {
                Assertions.assertEquals(template, type, sql);
            }
        }
    }

    @Test
    public void testATypedArgumentKeepsTheTypeItsExpressionWasBoundWith() throws Exception {
        // the IF was bound as a plain VARIANT: right after the rule its typed call sits under a relabel back to it
        String sql = "insert into " + TABLE + " select id, if(id > 0, parse_to_variant(j), null) from " + SOURCE;
        createStatementCtx(sql);
        Explainable command = (Explainable) new NereidsParser().parseSingle(sql);
        Plan plan = PlanChecker.from(connectContext)
                .analyze(command.getExplainPlan(connectContext))
                .applyCustom(new TypeVariantTemplateParse())
                .getPlan();
        List<If> ifs = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                ifs.addAll(expression.<If>collectToList(If.class::isInstance));
            }
        });
        Assertions.assertEquals(1, ifs.size());
        Expression value = ifs.get(0).child(1);
        Assertions.assertInstanceOf(Cast.class, value, value.toString());
        Assertions.assertEquals(VariantType.INSTANCE, value.getDataType());
        Assertions.assertInstanceOf(ParseToVariant.class, value.child(0), value.toString());
        Assertions.assertEquals(templateOfTargetColumn(), ((ParseToVariant) value.child(0)).getReturnType());
    }

    @Test
    public void testAReaderBetweenTheCallAndTheSinkSeesTheTypedCall() throws Exception {
        // the call is typed where it is computed, so the filter over its slot reads what the column will store
        String sql = "insert into " + TABLE + " select id, v from (select id, parse_to_variant(j) as v from "
                + SOURCE + ") s where cast(v['money']['amount'] as string) = '1.5'";
        createStatementCtx(sql);
        Explainable command = (Explainable) new NereidsParser().parseSingle(sql);
        Plan plan = PlanChecker.from(connectContext)
                .analyze(command.getExplainPlan(connectContext))
                .applyCustom(new TypeVariantTemplateParse())
                .getPlan();
        List<Alias> calls = new ArrayList<>();
        List<LogicalFilter<?>> filters = new ArrayList<>();
        plan.foreach(node -> {
            if (node instanceof LogicalFilter) {
                filters.add((LogicalFilter<?>) node);
            }
            for (Expression expression : ((Plan) node).getExpressions()) {
                if (expression instanceof Alias && expression.child(0) instanceof ParseToVariant) {
                    calls.add((Alias) expression);
                }
            }
        });
        Assertions.assertEquals(1, calls.size());
        Assertions.assertEquals(templateOfTargetColumn(), ((ParseToVariant) calls.get(0).child()).getReturnType());
        Assertions.assertEquals(1, filters.size());
        boolean filterReadsTheCall = false;
        for (Expression conjunct : filters.get(0).getConjuncts()) {
            for (Slot slot : conjunct.getInputSlots()) {
                filterReadsTheCall |= slot.getExprId().equals(calls.get(0).getExprId());
            }
        }
        Assertions.assertTrue(filterReadsTheCall, filters.get(0).toString());
    }

    @Test
    public void testACallIsToldApartByHowItsTemplateMatchesAndATypeIsNot() {
        DataType amount = DecimalV3Type.createDecimalV3Type(20, 6);
        VariantType literal = new VariantType(ImmutableList.of(new VariantField("a", amount, "", "MATCH_NAME")));
        VariantType glob = new VariantType(ImmutableList.of(new VariantField("a", amount, "")));
        // One type wherever types are compared, as before the template rode to the BE: the common type of a
        // set operation over the two, which a CTAS takes its column from, keeps the template.
        Assertions.assertEquals(literal, glob);
        Assertions.assertEquals(literal.hashCode(), glob.hashCode());
        Assertions.assertEquals(Optional.of(literal),
                TypeCoercionUtils.findWiderTypeForTwo(literal, glob, false, false));
        // Not one call: the BE matches a MATCH_NAME pattern literally and a glob with fnmatch.
        Assertions.assertFalse(literal.equalsWithPatternTypes(glob));
        Assertions.assertTrue(literal.equalsWithPatternTypes(
                new VariantType(ImmutableList.of(new VariantField("a", amount, "", "MATCH_NAME")))));
        Expression text = new StringLiteral("{}");
        Assertions.assertNotEquals(new ParseToVariant(text, literal), new ParseToVariant(text, glob));
        Assertions.assertEquals(new ParseToVariant(text, literal), new ParseToVariant(text, literal));
        Assertions.assertNotEquals(new TryParseToVariant(text, literal), new TryParseToVariant(text, glob));
        Assertions.assertEquals(new TryParseToVariant(text, glob), new TryParseToVariant(text, glob));
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
        Assertions.assertFalse(calls.get(0).toString().contains("template<"), calls.get(0).toString());
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

    private Plan rewrittenPlan(String sql) throws Exception {
        createStatementCtx(sql);
        Explainable command = (Explainable) new NereidsParser().parseSingle(sql);
        return PlanChecker.from(connectContext)
                .analyze(command.getExplainPlan(connectContext))
                .rewrite()
                .getPlan();
    }

    /** The result type of every parse_to_variant and try_parse_to_variant call in {@code plan}. */
    private static List<VariantType> callReturnTypes(Plan plan) {
        List<VariantType> types = new ArrayList<>();
        plan.foreach(node -> {
            for (Expression expression : ((Plan) node).getExpressions()) {
                for (Expression call : expression.<Expression>collectToList(
                        e -> e instanceof ParseToVariant || e instanceof TryParseToVariant)) {
                    types.add(call instanceof ParseToVariant
                            ? ((ParseToVariant) call).getReturnType() : ((TryParseToVariant) call).getReturnType());
                }
            }
        });
        return types;
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
        Assertions.assertEquals(TPrimitiveType.DECIMAL128I, paths.get(0).getType());
        Assertions.assertEquals("ratio*", paths.get(1).getPattern());
        Assertions.assertEquals(TPrimitiveType.DECIMAL128I, paths.get(1).getType());
        Assertions.assertEquals("name", paths.get(2).getPattern());
        Assertions.assertEquals(TPrimitiveType.STRING, paths.get(2).getType());
    }
}
