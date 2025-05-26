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
import org.apache.doris.common.proc.BuildIndexProcDir;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowBuildIndexCommandTest extends TestWithFeService {

    private ConnectContext ctx = new ConnectContext();
    @Mocked
    private BuildIndexProcDir procDir;
    private String catalog = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testValidateNormal() throws AnalysisException {

        // test where is null but db is not null
        ShowBuildIndexCommand command = new ShowBuildIndexCommand(catalog, "test_db", null, null, -1, -1);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        // test where is null and db is null
        ShowBuildIndexCommand command1 = new ShowBuildIndexCommand(null, null, null, null, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate(ctx));

        // test where is not null
        Expression where1 = new EqualTo(new UnboundSlot(Lists.newArrayList("TableName")),
                new StringLiteral("table1"));
        ShowBuildIndexCommand command2 = new ShowBuildIndexCommand(catalog, "test_db", null, where1, -1, -1);
        Assertions.assertDoesNotThrow(() -> command2.validate(ctx));

        //WHERE compound predicate
        Expression subWhere1 = new GreaterThan(new UnboundSlot(Lists.newArrayList("createtime")),
                new StringLiteral("2019-12-02 14:54:00"));
        Expression subWhere2 = new EqualTo(new UnboundSlot(Lists.newArrayList("state")),
                new StringLiteral("RUNNING"));
        Expression where2 = new And(subWhere1, subWhere2);
        ShowBuildIndexCommand command3 = new ShowBuildIndexCommand(catalog, "test_db", null, where2, -1, -1);
        Assertions.assertDoesNotThrow(() -> command3.validate(ctx));
    }

    @Test
    public void testProcessOrderBy() throws AnalysisException {
        UnboundSlot key = new UnboundSlot(Lists.newArrayList("TableName"));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(key, false, false));
        ShowBuildIndexCommand command = new ShowBuildIndexCommand(catalog, "test_db", orderKeys, null, -1, -1);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        UnboundSlot key2 = new UnboundSlot(Lists.newArrayList("test_order"));
        List<OrderKey> orderKeys2 = Lists.newArrayList(new OrderKey(key2, false, false));
        ShowBuildIndexCommand command2 = new ShowBuildIndexCommand(catalog, "test_db", orderKeys2, null, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx));
    }

    @Test
    public void testValidateException() {
        //like
        Expression where = new Like(new UnboundSlot(Lists.newArrayList("col")), new StringLiteral("xxx"));
        ShowBuildIndexCommand command = new ShowBuildIndexCommand(catalog, "test_db", null, where, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));

        //left is wrong
        Expression where2 = new EqualTo(new UnboundSlot(Lists.newArrayList("wrongLeft")), new StringLiteral("testKey"));
        ShowBuildIndexCommand command2 = new ShowBuildIndexCommand(catalog, "test_db", null, where2, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx));

        //right is wrong
        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("state")), new IntegerLiteral(111));
        ShowBuildIndexCommand command3 = new ShowBuildIndexCommand(catalog, "test_db", null, where3, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(ctx));

        Expression where4 = new EqualTo(new UnboundSlot(Lists.newArrayList("createtime")), new IntegerLiteral(113));
        ShowBuildIndexCommand command4 = new ShowBuildIndexCommand(catalog, "test_db", null, where4, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(ctx));

        //wrong operator
        Expression where5 = new GreaterThan(new UnboundSlot(Lists.newArrayList("state")), new StringLiteral("RUNNING"));
        ShowBuildIndexCommand command5 = new ShowBuildIndexCommand(catalog, "test_db", null, where5, -1, -1);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(ctx));
    }
}
