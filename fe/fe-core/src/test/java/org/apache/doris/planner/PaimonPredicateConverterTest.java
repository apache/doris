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

package org.apache.doris.planner;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.paimon.source.PaimonPredicateConverter;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

public class PaimonPredicateConverterTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        // Create database `db1`.
        createDatabase("db1");

        String tbl1 = "create table db1.tbl1(" + "k1 int," + " k2 int," + " v1 int)" + " distributed by hash(k1)"
                + " properties('replication_num' = '1');";
        createTables(tbl1);
        createTables("create table db1.cast_predicates (id int, code string, amount decimal(10, 2))"
                + " distributed by hash(id) properties('replication_num' = '1')");
    }

    @ParameterizedTest
    @ValueSource(strings = {"cast(code as int) = 5", "5 = cast(code as int)", "cast(code as int) > 5",
            "cast(code as int) in (5, 6)", "cast(code as int) not in (5, 6)",
            "cast(code as int) is null", "cast(code as int) is not null",
            "cast(code as int) = 5 or id = 1"})
    public void rejectStringToIntegerCast(String predicate) throws Exception {
        List<Expr> conjuncts = planCastPredicate(predicate);
        Assertions.assertTrue(conjuncts.stream().anyMatch(expr -> expr.contains(CastExpr.class)), predicate);
        List<Expr> remaining = Expr.cloneList(conjuncts);
        Assertions.assertTrue(castPredicateConverter().convertToPaimonExpr(conjuncts).isEmpty(), predicate);
        Assertions.assertEquals(remaining, conjuncts);
    }

    @Test
    public void rejectDecimalScaleCast() throws Exception {
        List<Expr> conjuncts = planCastPredicate("cast(amount as decimal(10, 1)) = 1.2");
        Assertions.assertTrue(conjuncts.stream().anyMatch(expr -> expr.contains(CastExpr.class)));
        Assertions.assertTrue(castPredicateConverter().convertToPaimonExpr(conjuncts).isEmpty());
    }

    @Test
    public void retainIndependentUncastPredicates() throws Exception {
        List<Expr> conjuncts = planCastPredicate("cast(code as int) = 5 and id = 1");
        List<Expr> remaining = Expr.cloneList(conjuncts);
        List<Predicate> predicates = castPredicateConverter().convertToPaimonExpr(conjuncts);
        Assertions.assertEquals(1, predicates.size());
        Assertions.assertEquals("id", ((LeafPredicate) predicates.get(0)).fieldName());
        Assertions.assertEquals(remaining, conjuncts);

        for (String predicate : Lists.newArrayList("code = '5'", "id > 1", "id in (1, 2)")) {
            Assertions.assertEquals(1, castPredicateConverter().convertToPaimonExpr(planCastPredicate(predicate)).size(),
                    predicate);
        }
    }

    private List<Expr> planCastPredicate(String predicate) throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext,
                "select * from db1.cast_predicates where " + predicate);
        executor.execute();
        return executor.planner().getScanNodes().get(0).getConjuncts();
    }

    private PaimonPredicateConverter castPredicateConverter() {
        return new PaimonPredicateConverter(new RowType(Lists.newArrayList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "code", new VarCharType()),
                new DataField(2, "amount", new DecimalType(10, 2)))));
    }

    @Test
    public void equal() throws Exception {
        DataField paimonFieldK1 = new DataField(0, "k1", new IntType());
        DataField paimonFieldK2 = new DataField(1, "k2", new IntType());
        DataField paimonFieldV1 = new DataField(2, "v1", new IntType());
        RowType rowType = new RowType(Lists.newArrayList(paimonFieldK1, paimonFieldK2, paimonFieldV1));
        PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
        connectContext.getSessionVariable().setParallelResultSink(false);

        // k1=1
        String sql1 = "SELECT * from db1.tbl1 where k1 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql1);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        List<Expr> conjuncts = fragments.get(0).getPlanRoot().getChild(0).conjuncts;
        List<Predicate> predicates = converter.convertToPaimonExpr(conjuncts);
        Assertions.assertEquals(predicates.size(), 1);
        Assertions.assertTrue(predicates.get(0) instanceof LeafPredicate);
        LeafPredicate leafPredicate = (LeafPredicate) predicates.get(0);
        Assertions.assertEquals(leafPredicate.fieldName(), "k1");

        // k1=1 and k2=2
        sql1 = "SELECT * from db1.tbl1 where k1 = 1 and k2 = 2";
        stmtExecutor = new StmtExecutor(connectContext, sql1);
        stmtExecutor.execute();
        planner = stmtExecutor.planner();
        fragments = planner.getFragments();
        conjuncts = fragments.get(0).getPlanRoot().getChild(0).conjuncts;
        predicates = converter.convertToPaimonExpr(conjuncts);
        Assertions.assertEquals(predicates.size(), 2);

        // k1 =1 or k2 = 2
        sql1 = "SELECT * from db1.tbl1 where k1 = 1 or k2 = 2";
        stmtExecutor = new StmtExecutor(connectContext, sql1);
        stmtExecutor.execute();
        planner = stmtExecutor.planner();
        fragments = planner.getFragments();
        conjuncts = fragments.get(0).getPlanRoot().getChild(0).conjuncts;
        predicates = converter.convertToPaimonExpr(conjuncts);
        Assertions.assertEquals(predicates.size(), 1);
        Assertions.assertTrue(predicates.get(0) instanceof CompoundPredicate);
        CompoundPredicate predicate = (CompoundPredicate) predicates.get(0);
        Assertions.assertTrue(predicate.function() instanceof Or);
        Assertions.assertEquals(predicate.children().size(), 2);
        Assertions.assertTrue(predicate.children().get(0) instanceof LeafPredicate);
        Assertions.assertTrue(predicate.children().get(1) instanceof LeafPredicate);
    }
}
