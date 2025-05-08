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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

public class ShowBackupCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Override
    public void createTable(String sql) throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        ((CreateTableCommand) plan).run(connectContext, null);
    }

    @Test
    void testGetSnapshotPredicate() throws AnalysisException, UserException {
        // test where is null
        ShowBackupCommand bc = new ShowBackupCommand("", null);
        Predicate<String> re = bc.getSnapshotPredicate();
        Assertions.assertTrue(re.test("test"));

        // test isAccurateMatch = true
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new StringLiteral("mysql_edu"));
        bc = new ShowBackupCommand("test", where);
        bc.validate(connectContext); // set isAccurateMatch = true
        Predicate<String> re1 = bc.getSnapshotPredicate();
        Assertions.assertTrue(re1.test("mysql_EDU"));

        // test isAccurateMatch = false
        Expression where1 = new Like(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new StringLiteral("mysql_edu%"));
        bc = new ShowBackupCommand("", where1);
        bc.validate(connectContext);
        Predicate<String> re2 = bc.getSnapshotPredicate();
        Assertions.assertTrue(re2.test("mysql_edu%"));
    }

    @Test
    void testValidate() throws UserException {
        // test No database selected
        ShowBackupCommand bc = new ShowBackupCommand("", null);
        ShowBackupCommand finalBc = bc;
        connectContext.setDatabase("");
        Assertions.assertThrows(AnalysisException.class, () -> finalBc.validate(connectContext));
        connectContext.setDatabase("test");  // reset database

        // test where is null
        bc = new ShowBackupCommand("test_db", null);
        Assertions.assertTrue(bc.validate(connectContext));

        // test where is not Like and where is not EqualTo
        bc = new ShowBackupCommand("test_db", new LessThan(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new VarcharLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921")));
        Assertions.assertFalse(bc.validate(connectContext));

        // test left key is not snapshotname
        bc = new ShowBackupCommand("test_db", new EqualTo(new UnboundSlot(Lists.newArrayList("notsnapshotname")),
                new VarcharLiteral("mysql_edu_mall_mall_source_occupy_1741669209_174166921")));
        Assertions.assertFalse(bc.validate(connectContext));

        // test right key is StringLikeLiteral, class is EqualTo
        bc = new ShowBackupCommand("test_db", new EqualTo(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new StringLiteral("mysql_edu%")));
        Assertions.assertTrue(bc.validate(connectContext));

        // test right key is StringLikeLiteral, class is Like
        bc = new ShowBackupCommand("test_db", new Like(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new StringLiteral("mysql_edu%")));
        Assertions.assertTrue(bc.validate(connectContext));

        // test right key is StringLikeLiteral but value is empty, class is Like,
        bc = new ShowBackupCommand("test_db", new Like(new UnboundSlot(Lists.newArrayList("snapshotname")),
                new StringLiteral("")));
        Assertions.assertFalse(bc.validate(connectContext));
    }
}
