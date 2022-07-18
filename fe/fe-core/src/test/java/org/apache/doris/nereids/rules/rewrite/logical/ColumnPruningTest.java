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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * column prune ut.
 */
public class ColumnPruningTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {

        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n" + "sid int not null, \n" + "cid int not null, \n" + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n" + "properties('replication_num' = '1');");

        createTable("create table test.course (\n" + "cid int not null, \n" + "cname varchar(128), \n"
                + "teacher varchar(128))\n" + "distributed by hash(cid) buckets 10\n"
                + "properties('replication_num' = '1');");


        connectContext.setDatabase("default_cluster:test");

    }

    @Test
    public void testPruneColumns1() {
        String sql
                = "select id,name,grade from student left join score on student.id = score.sid where score.grade > 60";
        Plan plan = AnalyzeUtils.analyze(sql, connectContext);

        Memo memo = new Memo();
        memo.initialize(plan);

        Plan out = process(memo);

        System.out.println(out.treeString());
        Plan l1 = out.child(0).child(0);
        Plan l20 = l1.child(0).child(0);
        Plan l21 = l1.child(0).child(1);

        LogicalProject p1 = (LogicalProject) l1;
        LogicalProject p20 = (LogicalProject) l20;
        LogicalProject p21 = (LogicalProject) l21;

        List<String> target;
        List<String> source;

        source = getStringList(p1);
        target = Lists.newArrayList("default_cluster:test.student.name", "default_cluster:test.student.id",
                "default_cluster:test.score.grade");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p20);
        target = Lists.newArrayList("default_cluster:test.student.id", "default_cluster:test.student.name");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p21);
        target = Lists.newArrayList("default_cluster:test.score.sid", "default_cluster:test.score.grade");
        Assertions.assertTrue(source.containsAll(target));

    }

    @Test
    public void testPruneColumns2() {

        String sql
                = "select name,sex,cid,grade from student left join score on student.id = score.sid "
                + "where score.grade > 60";
        Plan plan = AnalyzeUtils.analyze(sql, connectContext);

        Memo memo = new Memo();
        memo.initialize(plan);

        Plan out = process(memo);

        Plan l1 = out.child(0).child(0);
        Plan l20 = l1.child(0).child(0);
        Plan l21 = l1.child(0).child(1);

        LogicalProject p1 = (LogicalProject) l1;
        LogicalProject p20 = (LogicalProject) l20;
        Assertions.assertTrue(l21 instanceof LogicalRelation);

        List<String> target;
        List<String> source;

        source = getStringList(p1);
        target = Lists.newArrayList("default_cluster:test.student.name", "default_cluster:test.score.cid",
                "default_cluster:test.score.grade", "default_cluster:test.student.sex");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p20);
        target = Lists.newArrayList("default_cluster:test.student.id", "default_cluster:test.student.name",
                "default_cluster:test.student.sex");
        Assertions.assertTrue(source.containsAll(target));
    }


    @Test
    public void testPruneColumns3() {

        String sql = "select id,name from student where age > 18";
        Plan plan = AnalyzeUtils.analyze(sql, connectContext);

        Memo memo = new Memo();
        memo.initialize(plan);

        Plan out = process(memo);

        Plan l1 = out.child(0).child(0);
        LogicalProject p1 = (LogicalProject) l1;

        List<String> target;
        List<String> source;

        source = getStringList(p1);
        target = Lists.newArrayList("default_cluster:test.student.name", "default_cluster:test.student.id",
                "default_cluster:test.student.age");
        Assertions.assertTrue(source.containsAll(target));

    }

    @Test
    public void testPruneColumns4() {

        String sql
                = "select name,cname,grade from student left join score on student.id = score.sid left join course "
                + "on score.cid = course.cid where score.grade > 60";
        Plan plan = AnalyzeUtils.analyze(sql, connectContext);

        Memo memo = new Memo();
        memo.initialize(plan);

        Plan out = process(memo);

        Plan l1 = out.child(0).child(0);
        Plan l20 = l1.child(0).child(0);
        Plan l21 = l1.child(0).child(1);

        Plan l20Left = l20.child(0).child(0);
        Plan l20Right = l20.child(0).child(1);

        Assertions.assertTrue(l20 instanceof LogicalProject);
        Assertions.assertTrue(l20Left instanceof LogicalProject);
        Assertions.assertTrue(l20Right instanceof LogicalRelation);

        LogicalProject p1 = (LogicalProject) l1;
        LogicalProject p20 = (LogicalProject) l20;
        LogicalProject p21 = (LogicalProject) l21;

        LogicalProject p20lo = (LogicalProject) l20Left;

        List<String> target;
        List<String> source;

        source = getStringList(p1);
        target = Lists.newArrayList("default_cluster:test.student.name", "default_cluster:test.course.cname",
                "default_cluster:test.score.grade");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p20);
        target = Lists.newArrayList("default_cluster:test.student.name", "default_cluster:test.score.cid",
                "default_cluster:test.score.grade");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p21);
        target = Lists.newArrayList("default_cluster:test.course.cid", "default_cluster:test.course.cname");
        Assertions.assertTrue(source.containsAll(target));

        source = getStringList(p20lo);
        target = Lists.newArrayList("default_cluster:test.student.id", "default_cluster:test.student.name");
        Assertions.assertTrue(source.containsAll(target));
    }

    private Plan process(Memo memo) {
        PlannerContext plannerContext = new PlannerContext(memo, new ConnectContext());
        JobContext jobContext = new JobContext(plannerContext, new PhysicalProperties(), 0);
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(),
                new ColumnPruning().buildRules(), jobContext);
        plannerContext.pushJob(rewriteTopDownJob);
        plannerContext.getJobScheduler().executeJobPool(plannerContext);
        return memo.copyOut();
    }

    private List<String> getStringList(LogicalProject<Plan> p) {
        return p.getProjects().stream().map(NamedExpression::getQualifiedName).collect(Collectors.toList());
    }
}
