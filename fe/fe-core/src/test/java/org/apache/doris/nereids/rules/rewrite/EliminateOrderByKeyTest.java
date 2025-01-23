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
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
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
                .analyze("select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',b,c1 limit 5")
                .rewrite()
                .printlnTree()
                .matches(logicalTopN().when(sort -> sort.getOrderKeys().size() == 1));
    }

    @Test
    void NotEliminateNonDeterministic() {
        PlanChecker.from(connectContext)
                .analyze("select a,b,c,d,dt from eliminate_order_by_constant_t order by a,a+random(1,10)")
                .rewrite()
                .printlnTree()
                .matches(logicalSort().when(sort -> sort.getOrderKeys().size() == 2));
    }
}
