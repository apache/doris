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

import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class EliminateOrderByKeyTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.eliminate_order_by_constant_t(a int null, b int not null,"
                + "c varchar(10) null, d date, dt datetime, id int)\n"
                + "distributed by hash(a) properties(\"replication_num\"=\"1\");");
        createTable("create table test.eliminate_order_by_constant_t2(a int, b int, c int, d int) "
                + "distributed by hash(a) properties(\"replication_num\"=\"1\");");
        createTable("create table test.test_unique_order_by2(a int not null, b int not null, c int, d int) "
                + "unique key(a,b) distributed by hash(a) properties('replication_num'='1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testEliminateByCompositeKeys() {
        PlanChecker.from(connectContext)
                .analyze("select * from test_unique_order_by2 order by a,'abc',d,b,d,c")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testEliminateByFd() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from test.eliminate_order_by_constant_t order by a,abs(a),a+1")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByFdAndDup() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a,abs(a),a+1")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByFdTopN() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a+1 limit 5")
                .rewrite()
                .printlnTree()
                .matches(logicalTopN().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByFdAndDupTopN() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a,abs(a),a+1,id limit 5")
                .rewrite()
                .printlnTree()
                .matches(logicalTopN().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testEliminateByDup() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByDupExpr() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a+1,a+1")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByDupTopN() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,a limit 5")
                .rewrite()
                .printlnTree()
                .matches(logicalTopN().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByUniformPredicate() {
        PlanChecker.from(connectContext)
                .analyze("select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalSort());
    }

    @Test
    void testEliminateByUniformWithAgg() {
        PlanChecker.from(connectContext)
                .analyze("select 1 as c1,a from eliminate_order_by_constant_t where a=1 group by c1,a order by a")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalSort());
    }

    @Test
    void testEliminateByUniformMultiKey() {
        PlanChecker.from(connectContext)
                .analyze("select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',b,c1")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEliminateByUniformTopN() {
        PlanChecker.from(connectContext)
                .analyze("select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',c1 limit 5")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalTopN());
    }

    @Test
    void notEliminateNonDeterministic() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,a+random(1,10)")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testMultiColumnFd() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,b,a+b")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testMultiColumnFdWithOtherInMiddle() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,c,b,a+b")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testWindowDup() {
        PlanChecker.from(connectContext)
                .analyze("select sum(a) over (partition by a order by a,a)  from eliminate_order_by_constant_t")
                .rewrite()
                .printlnTree()
                .matches(logicalWindow()
                        .when(window -> ((WindowExpression) window.getWindowExpressions().get(0).child(0))
                                .getOrderKeys().size() == 1));
    }

    @Test
    void testWindowFd() {
        PlanChecker.from(connectContext)
                .analyze("select sum(a) over (partition by a order by a,a+1,abs(a),1-a,b)  from eliminate_order_by_constant_t")
                .rewrite()
                .printlnTree()
                .matches(logicalWindow()
                        .when(window -> ((WindowExpression) window.getWindowExpressions().get(0).child(0))
                                .getOrderKeys().size() == 2));
    }

    @Test
    void testWindowUniform() {
        PlanChecker.from(connectContext)
                .analyze("select sum(a) over (partition by a order by b) from eliminate_order_by_constant_t where b=100")
                .rewrite()
                .printlnTree()
                .matches(logicalWindow()
                        .when(window -> ((WindowExpression) window.getWindowExpressions().get(0).child(0))
                                .getOrderKeys().isEmpty()));
    }

    @Test
    void testWindowMulti() {
        PlanChecker.from(connectContext)
                .analyze("select sum(a) over (partition by a order by a,a+1,abs(a),1-a,b)"
                        + ", max(a) over (partition by a order by b,b+1,b,abs(b)) from eliminate_order_by_constant_t")
                .rewrite()
                .printlnTree()
                .matches(logicalWindow()
                        .when(window -> ((WindowExpression) window.getWindowExpressions().get(0).child(0))
                                .getOrderKeys().size() == 2
                                && ((WindowExpression) window.getWindowExpressions().get(1).child(0))
                                .getOrderKeys().size() == 1));
    }

    @Test
    void testWindowMultiDesc() {
        PlanChecker.from(connectContext)
                .analyze("select sum(a) over (partition by a order by a desc,a+1 asc,abs(a) desc,1-a,b),"
                        + "max(a) over (partition by a order by b desc,b+1 desc,b asc,abs(b) desc) "
                        + "from eliminate_order_by_constant_t")
                .rewrite()
                .printlnTree()
                .matches(logicalWindow()
                        .when(window -> ((WindowExpression) window.getWindowExpressions().get(0).child(0))
                                .getOrderKeys().size() == 2
                                && ((WindowExpression) window.getWindowExpressions().get(1).child(0))
                                .getOrderKeys().size() == 1));
    }

    @Test
    void testEqualSet() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t where a=id order by a,id")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t where a=id order by id,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t where a=id and a=b order by id,a,b")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testEqualSetAndFd() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t  where a=b order by a,a+b, b ")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testUniformAndFd() {
        PlanChecker.from(connectContext)
                .analyze("select a,b from eliminate_order_by_constant_t where a=b and a=1 order by a,b,a+b")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalSort());
    }

    @Test
    void testUniformAndFd2() {
        PlanChecker.from(connectContext)
                .analyze("select a from eliminate_order_by_constant_t  where a=1 order by a,b,b,abs(a),a ")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void testFdValidSlotsAddEqualSet() {
        PlanChecker.from(connectContext)
                .analyze("select c,d,a,a+100,b+a+100,b from eliminate_order_by_constant_t where b=a order by c,d,a,a+100,b+a+100")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));

        // TODO After removing b from the projection column, b+a+100 cannot be deleted from sort.
        //  This is because the equal set after the project does not output a=b because the b projection column does not output it.
        PlanChecker.from(connectContext)
                .analyze("select c,d,a,a+100,b+a+100 from eliminate_order_by_constant_t where b=a order by c,d,a,a+100,b+a+100")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 4));
    }

    @Test
    void testEqualSetUniform() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 where b=a and a=c and d=1 order by d,a,b,c,c,b,a,d,d")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 where b=a and a=d and a=c and d=1 order by d,a,b,c,c,b,a,d,d")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalSort());
    }

    @Test
    void testEqualSetUniformFd() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 where b=a and a=d and a=c and d=1 order by d,a,a+1,b+1,c+1,c,b,a,d,d")
                .rewrite()
                .printlnTree()
                .nonMatch(logicalSort());
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 where d=1 order by d,a,b,c,d+b+a-100,d+b+a,b,a,d,d")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testEqualSetFd() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 where d=b and a=d order by a,c,d+b+a,b,a,d,d+1,abs(d)")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testInnerJoinEqual() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t2.a,t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 "
                        + "inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a "
                        + "order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 4));
    }

    @Test
    void testLeftJoinEqual() {
        // t1.a=t2.a but should not eliminate
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 "
                        + "left outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a "
                        + "order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 5));
        // left is converted to inner join
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 "
                        + "left outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a  where t2.c=100 "
                        + "order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testRightJoinEqual() {
        // right join and has uniform
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1"
                        + " right outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a where t2.a=1 "
                        + "order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 4));
    }

    @Test
    void testSemiAntiJoin() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b from eliminate_order_by_constant_t t1 "
                        + "left semi join eliminate_order_by_constant_t2 t2 on t1.a = t2.a order by t1.a, t1.b, t1.a+1,t1.a+t1.b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select t2.a, t2.b from eliminate_order_by_constant_t t1 "
                        + " right semi join eliminate_order_by_constant_t2 t2 on t1.a = t2.a "
                        + " order by t2.a, t2.b, t2.a+1,t2.a+t2.b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b "
                        + "from eliminate_order_by_constant_t t1 "
                        + "left anti join eliminate_order_by_constant_t2 t2 on t1.a = t2.a "
                        + "order by t1.a, t1.b, t1.a+1,t1.a+t1.b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select t2.a, t2.b"
                        + " from eliminate_order_by_constant_t t1"
                        + " right anti join eliminate_order_by_constant_t2 t2 on t1.a = t2.a"
                        + " order by t2.a, t2.b, t2.a+1,t2.a+t2.b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testAgg() {
        PlanChecker.from(connectContext)
                .analyze("select a, count(b) as cnt "
                        + "from eliminate_order_by_constant_t2 "
                        + "group by a "
                        + "order by a, cnt, a,a+cnt,a+100;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));

        PlanChecker.from(connectContext)
                .analyze("select a, b, count(c) as cnt"
                        + " from eliminate_order_by_constant_t2"
                        + " group by cube(a, b)"
                        + " order by a, b, cnt, a, b+1;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testJoinWindow() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b, t2.c, t2.d, t2.a,"
                        + " row_number() over (partition by t1.a order by t1.b) as rn"
                        + " from eliminate_order_by_constant_t t1"
                        + " inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a"
                        + " order by t1.a, t2.a,t2.c, t1.b, t2.d, abs(t1.a), abs(t2.a), t2.c,rn,rn+100;;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 5));
        PlanChecker.from(connectContext)
                .analyze("select a, b, count(c) as cnt, "
                        + " row_number() over (partition by a order by b) as rn "
                        + " from eliminate_order_by_constant_t2 "
                        + " group by a, b "
                        + " order by a, b, cnt, a+100, b, rn, rn+cnt, abs(rn+cnt);")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 4));
    }

    @Test
    void testAggWindowJoin() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a, t1.b, count(t2.c) as cnt,"
                        + " row_number() over (partition by t1.a order by t1.b) as rn"
                        + " from eliminate_order_by_constant_t t1"
                        + " inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a"
                        + " group by t1.a, t1.b ,t2.a"
                        + " order by t1.a,t2.a,t1.b, cnt, -t1.a, -t1.b-1000,rn, cnt, rn+111;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 4));
    }

    @Test
    void testUnionAll() {
        PlanChecker.from(connectContext)
                .analyze("select * from ("
                        + "    select a, b from eliminate_order_by_constant_t2 "
                        + "    union all "
                        + "    select a, b from eliminate_order_by_constant_t ) t "
                        + "    order by a, b, abs(a),abs(a)+b,a+b,a,b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select * from ("
                        + "    select a, b from eliminate_order_by_constant_t2 "
                        + "    union all "
                        + "    select a, b from eliminate_order_by_constant_t ) t "
                        + "    where a=b order by a, b, abs(a),abs(a)+b,a+b,a,b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }

    // TODO LogicalUnion compute uniform can expand support scope when each child has same uniform output
    //  and corresponding same position
    @Test
    void testUnionJoin() {
        PlanChecker.from(connectContext)
                .analyze("select * from (select t1.a, t1.b "
                        + "    from eliminate_order_by_constant_t t1 "
                        + "    inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a "
                        + "    union "
                        + "    select t1.a, t1.b "
                        + "    from eliminate_order_by_constant_t t1 "
                        + "    left join eliminate_order_by_constant_t2 t2 on t1.a = t2.a ) t"
                        + "    where a=1"
                        + "    order by a, b, a+100,abs(a)+b;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testEliminateByMonotonicBasic() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by a+1,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by 1+a,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by a-1,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by 1-a,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by 3*a,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by a*3,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testPositiveNegative() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by positive(a),4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by negative(a),4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
    }

    @Test
    void testEliminateByMonotonicComplex() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by a+1,1-a,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by 1-a,1+a,a,a,1+a,a-1,4,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by 1+a-1-a,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by a-1+a-a,b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 3));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by 1-(1+a),b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t2 order by 2-(1+3*a),2+(3*a+100),b,a")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }

    @Test
    void testMonotonicDateFunction() {
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by cast(d as datetime) ,d;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by cast(dt as datetimev2(6)) ,dt")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by days_add(dt,3) ,dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by days_sub(dt,3) ,dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by months_sub(dt,3) ,dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by convert_tz(dt,'Europe/Paris','Asia/ShangHai') ,dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by years_add(convert_tz(dt,'Europe/Paris','Asia/ShangHai'),3) ,dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
        PlanChecker.from(connectContext)
                .analyze("select * from eliminate_order_by_constant_t order by years_add(convert_tz(dt,'Europe/Paris','Asia/ShangHai'),3) ,convert_tz(dt,'Europe/Paris','Asia/ShangHai'),dt;")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 1));
    }
}
