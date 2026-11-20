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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.StringType;
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
 * VALUES path included, when that column is the one reader of the call's value. It leaves a call with another
 * reader, whether that reader names the call's slot or holds a copy of the call, and a call outside a sink,
 * alone.
 */
public class TypeVariantTemplateParseTest extends TestWithFeService {
    private static final String DATABASE = "variant_template_parse_db";
    private static final String TABLE = "variant_template_parse_t";
    private static final String SOURCE = "variant_template_parse_src";
    private static final String UNIQUE_TABLE = "variant_template_parse_mow";
    private static final String PLAIN_TEMPLATE_TABLE = "variant_template_parse_plain";
    private static final String WIDE_TABLE = "variant_template_parse_wide";
    private static final String TWO_TEMPLATES_TABLE = "variant_template_parse_two";
    private static final String PATTERN_TYPES_TABLE = "variant_template_parse_patterns";
    private static final String GENERATED_TABLE = "variant_template_parse_generated";
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
        createTable("create table " + PLAIN_TEMPLATE_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'name':string,'count':bigint>\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table " + WIDE_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>,\n"
                + "    raw variant,\n"
                + "    txt string\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table " + TWO_TEMPLATES_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>,\n"
                + "    w variant<'money.amount':decimal(10,2)>\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        // two templates that differ only in how the pattern matches: the BE reads "ab" by the glob only
        createTable("create table " + PATTERN_TYPES_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'a*':decimal(10,2)>,\n"
                + "    w variant<MATCH_NAME 'a*':decimal(10,2)>\n"
                + ")\n"
                + "duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties(\"replication_num\"=\"1\")");
        createTable("create table " + GENERATED_TABLE + " (\n"
                + "    id int,\n"
                + "    v variant<'money.amount':decimal(20,6),'ratio*':decimal(30,18),'name':string>,\n"
                + "    g decimal(20,6) generated always as (cast(v['money']['amount'] as decimal(20,6)))\n"
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
        shapes.add("insert into " + TABLE + " select id, nullable(parse_to_variant(j))" + from);
        shapes.add("insert into " + TABLE + " select id, non_nullable(try_parse_to_variant(j))" + from);
        shapes.add("update " + UNIQUE_TABLE + " set v = parse_to_variant('" + DOCUMENT + "') where id = 1");
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
    public void testACallOnlyItsColumnReadsIsTypedThroughTheNodesThatHandItOn() throws Exception {
        VariantType template = templateOfTargetColumn();
        String parsed = "(select id, parse_to_variant(j) as v from " + SOURCE + ")";
        List<String> shapes = new ArrayList<>();
        // a filter, a sort key and a join condition that do not name the value
        shapes.add("insert into " + TABLE + " select id, v from " + parsed + " s where id > 0 order by id");
        shapes.add("insert into " + TABLE + " select a.id, a.v from " + parsed + " a join " + SOURCE
                + " b on a.id = b.id");
        // a window over other columns
        shapes.add("insert into " + TABLE + " select id, v from (select id, v, row_number() over"
                + " (partition by id order by id) as rn from " + parsed + " s) w where rn = 1");
        // the value taken from one slot in two places, by the one column: both branches of a MERGE INTO
        shapes.add("merge into " + UNIQUE_TABLE + " t using (select id, parse_to_variant(j) as p from " + SOURCE
                + ") s on t.id = s.id when matched then update set v = s.p"
                + " when not matched then insert (id, v) values (s.id, s.p)");
        // the other columns of the table take their defaults
        shapes.add("insert into " + WIDE_TABLE + " (id, v) select id, v from " + parsed + " s");
        // a top-n over another column
        shapes.add("insert into " + TABLE + " select id, v from " + parsed + " s order by id limit 5");
        for (String sql : shapes) {
            List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
            Assertions.assertFalse(types.isEmpty(), sql);
            for (VariantType type : types) {
                Assertions.assertTrue(template.equalsWithPatternTypes(type), sql);
            }
        }
    }

    @Test
    public void testACallSomethingElseReadsStaysAsItIs() throws Exception {
        // The template applies to the value its column stores and to nothing else. When anything else in the
        // statement reads the call's value, the call is left untyped: that reader sees what it always saw.
        String parsed = "(select id, parse_to_variant(j) as v from " + SOURCE + ")";
        String limited = "(select id, parse_to_variant(j) as v from " + SOURCE + " limit 10)";
        List<String> shapes = new ArrayList<>();
        // a filter, a sort key, a join condition
        shapes.add("insert into " + TABLE + " select id, v from " + parsed
                + " s where cast(v['money']['amount'] as string) = '1.5'");
        shapes.add("insert into " + TABLE + " select id, v from " + parsed + " s order by cast(v['name'] as string)");
        shapes.add("insert into " + TABLE + " select a.id, a.v from " + parsed + " a join " + parsed
                + " b on cast(a.v['name'] as string) = cast(b.v['name'] as string)");
        // a filter that holds a subquery as well stays above the subquery's join, and still names the slot
        shapes.add("insert into " + TABLE + " select id, v from " + parsed
                + " s where cast(v['money']['amount'] as string) = '1.5' and id in (select id from " + SOURCE + ")");
        // the condition of the IF that returns it
        shapes.add("insert into " + TABLE + " select id, if(v is null, null, v) from " + limited + " s");
        // other columns of the same sink: a plain VARIANT, a STRING
        shapes.add("insert into " + WIDE_TABLE + " select id, v, v, cast(v as string) from " + limited + " s");
        shapes.add("insert into " + WIDE_TABLE + " (id, v, txt) select id, v, cast(v['name'] as string) from "
                + limited + " s");
        // a second templated column with another template
        shapes.add("insert into " + TWO_TEMPLATES_TABLE + " select id, v, v from " + limited + " s");
        // or with a template that differs only in how its pattern matches
        shapes.add("insert into " + PATTERN_TYPES_TABLE + " select id, v, v from " + limited + " s");
        // a child of a UNION ALL that hands its slot to two columns of the union
        shapes.add("insert into " + WIDE_TABLE + " (id, v, raw) select id, v, v from " + limited + " a"
                + " union all select id, v, v from " + limited + " b");
        // a value that is grouped or compared before it is stored
        shapes.add("insert into " + TABLE + " select distinct id, v from " + parsed + " s");
        for (String operation : ImmutableList.of("union", "intersect", "except")) {
            shapes.add("insert into " + TABLE + " select id, v from " + parsed + " a " + operation
                    + " select id, v from " + parsed + " b");
        }
        // MERGE INTO: the ON clause, and the condition of a WHEN clause
        shapes.add("merge into " + UNIQUE_TABLE + " t using (select id, parse_to_variant(j) as p from " + SOURCE
                + ") s on t.id = s.id and cast(t.v['name'] as string) = cast(s.p['name'] as string)"
                + " when matched then update set v = s.p");
        shapes.add("merge into " + UNIQUE_TABLE + " t using (select id, parse_to_variant(j) as p from " + SOURCE
                + ") s on t.id = s.id when matched and cast(s.p['name'] as string) = 'n' then update set v = s.p");
        for (String sql : shapes) {
            List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
            Assertions.assertFalse(types.isEmpty(), sql);
            for (VariantType type : types) {
                Assertions.assertTrue(type.getPredefinedFields().isEmpty(), sql);
            }
        }
    }

    @Test
    public void testACallReadElsewhereAsACopyStaysAsItIs() throws Exception {
        // A reader does not have to name the call's slot: it may hold a copy of the call. Every copy then stays
        // as it is, the column's own included, so the column stores what such a reader computes from it.
        List<String> copies = new ArrayList<>();
        // other columns that parse the text themselves
        copies.add("insert into " + WIDE_TABLE
                + " select id, parse_to_variant(j), parse_to_variant(j), cast(parse_to_variant(j)['name'] as string)"
                + " from " + SOURCE);
        // A filter the planner has given its own copy. With a subquery in the statement, subquery unnesting
        // pushes a filter that sits directly on the alias's project below it and substitutes the alias.
        copies.add("insert into " + TABLE
                + " select id, v from (select id, v from (select id, parse_to_variant(j) as v from " + SOURCE
                + ") x where cast(v['money']['amount'] as string) = '1.5') b"
                + " where id in (select id from " + SOURCE + ")");
        // A generated column over the templated column: BindSink computes it from that column's expression, and
        // merging the sink's project into the one below gives it the call. The stored row keeps g = f(v).
        copies.add("insert into " + GENERATED_TABLE + " (id, v) select id, parse_to_variant(j) from " + SOURCE);
        copies.add("insert into " + GENERATED_TABLE + " (id, v) select 1, parse_to_variant('" + DOCUMENT + "')");
        // two templated columns that each write the call, with two templates
        copies.add("insert into " + TWO_TEMPLATES_TABLE + " select id, parse_to_variant(j), parse_to_variant(j) from "
                + SOURCE);
        for (String sql : copies) {
            List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
            Assertions.assertTrue(types.size() >= 2, sql + " holds " + types.size() + " call(s)");
            for (VariantType type : types) {
                Assertions.assertTrue(type.getPredefinedFields().isEmpty(), sql);
            }
        }
        // Sibling columns of a derived table the planner merges into the sink's project: with a subquery in the
        // statement the whole plan's projects are merged, through a filtered LIMIT as well.
        String siblings = "insert into " + WIDE_TABLE + " select id, v, v, cast(v as string) from"
                + " (select id, parse_to_variant(j) as v from " + SOURCE + " where id in (select id from " + SOURCE
                + ") limit 10) s where id > 0";
        List<VariantType> types = callReturnTypes(rewrittenPlan(siblings));
        Assertions.assertFalse(types.isEmpty(), siblings);
        for (VariantType type : types) {
            Assertions.assertTrue(type.getPredefinedFields().isEmpty(), siblings);
        }
    }

    @Test
    public void testACallWrittenTwiceForOneColumnIsTyped() throws Exception {
        // every place the call occurs is a place the one templated column takes its value from
        VariantType template = templateOfTargetColumn();
        String sql = "insert into " + TABLE + " select id, if(id > 0, parse_to_variant(j), parse_to_variant(j)) from "
                + SOURCE;
        List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
        Assertions.assertFalse(types.isEmpty(), sql);
        for (VariantType type : types) {
            Assertions.assertTrue(template.equalsWithPatternTypes(type), sql);
        }
    }

    @Test
    public void testTheRuleDecidesOnceForAStatement() throws Exception {
        // The rewrite of a plan a materialized view was put into runs the Rewriter's rules again
        // (NereidsPlanner's pre-rewrite, through MaterializedViewUtils.rewriteByRules) on the plan recorded
        // after this rule. Readers the rule counted by their slots may be copies of the call there, so the rule
        // is skipped: the statement keeps the one decision. Here the plan is handed over before the rule ran,
        // so a skipped rule shows as an untyped call where the statement's own rewrite types it.
        // (a constant row: inside rewriteByRules the session's own disabled rules are set aside, and the empty
        // source table of this test would be pruned away with the call)
        String sql = "insert into " + TABLE + " select 1, parse_to_variant('" + DOCUMENT + "')";
        VariantType template = templateOfTargetColumn();
        Assertions.assertFalse(callReturnTypes(rewrittenPlan(sql)).isEmpty(), sql);
        for (VariantType type : callReturnTypes(rewrittenPlan(sql))) {
            Assertions.assertTrue(template.equalsWithPatternTypes(type), sql);
        }
        createStatementCtx(sql);
        Explainable command = (Explainable) new NereidsParser().parseSingle(sql);
        CascadesContext analyzed = PlanChecker.from(connectContext)
                .analyze(command.getExplainPlan(connectContext))
                .getCascadesContext();
        Plan plan = analyzed.getRewritePlan();
        Plan rewritten = MaterializedViewUtils.rewriteByRules(analyzed, childContext -> {
            Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(childContext).execute();
            return childContext.getRewritePlan();
        }, plan, plan, false);
        List<VariantType> types = callReturnTypes(rewritten);
        Assertions.assertFalse(types.isEmpty(), sql);
        for (VariantType type : types) {
            Assertions.assertTrue(type.getPredefinedFields().isEmpty(), sql);
        }
    }

    @Test
    public void testAReaderOfTheSlotIsSeenRightAfterAnalysis() throws Exception {
        // the same decision without the Rewriter's other rules: the filter names the slot, so the call stays
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
        Assertions.assertEquals(1, filters.size());
        boolean filterReadsTheCall = false;
        for (Expression conjunct : filters.get(0).getConjuncts()) {
            for (Slot slot : conjunct.getInputSlots()) {
                filterReadsTheCall |= slot.getExprId().equals(calls.get(0).getExprId());
            }
        }
        Assertions.assertTrue(filterReadsTheCall, filters.get(0).toString());
        ParseToVariant call = (ParseToVariant) calls.get(0).child();
        Assertions.assertTrue(call.getReturnType().getPredefinedFields().isEmpty(), call.toString());
        Assertions.assertFalse(call.toString().contains("template<"), call.toString());
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
    public void testATemplateThatSendsNothingLeavesTheCallAsItIs() throws Exception {
        // A template with no DECIMAL or LARGEINT field does not ride to the BE, which parses as without one.
        // Typing the call for it would only make it unequal to the other copies of itself in the statement.
        for (String sql : ImmutableList.of(
                "insert into " + PLAIN_TEMPLATE_TABLE + " select id, parse_to_variant(j) from " + SOURCE,
                "insert into " + PLAIN_TEMPLATE_TABLE + " select id, if(id > 0, try_parse_to_variant(j), null) from "
                        + SOURCE)) {
            List<VariantType> types = callReturnTypes(rewrittenPlan(sql));
            Assertions.assertFalse(types.isEmpty(), sql);
            for (VariantType type : types) {
                Assertions.assertTrue(type.getPredefinedFields().isEmpty(), sql);
            }
        }
    }

    @Test
    public void testTheRuleAndTheWireAgreeOnWhichTemplatesRide() {
        DataType decimal = DecimalV3Type.createDecimalV3Type(20, 6);
        for (DataType fieldType : ImmutableList.of(StringType.INSTANCE, BigIntType.INSTANCE, DoubleType.INSTANCE,
                ArrayType.of(BigIntType.INSTANCE), LargeIntType.INSTANCE, decimal,
                DecimalV3Type.createDecimalV3Type(38, 10), ArrayType.of(LargeIntType.INSTANCE),
                ArrayType.of(decimal))) {
            VariantType type = new VariantType(ImmutableList.of(new VariantField("name", StringType.INSTANCE, ""),
                    new VariantField("f", fieldType, "")));
            boolean rides = !((org.apache.doris.catalog.VariantType) type.toCatalogDataType())
                    .collectTemplatePaths().isEmpty();
            Assertions.assertEquals(rides, type.readsNumbersFromText(), fieldType.toSql());
        }
        Assertions.assertFalse(VariantType.INSTANCE.readsNumbersFromText());
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
