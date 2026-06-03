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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleMappingCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRoleMappingCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RoleMappingParserTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testCreateRoleMappingParse() {
        LogicalPlan plan = parser.parseSingle("CREATE ROLE MAPPING IF NOT EXISTS corp_mapping "
                + "ON AUTHENTICATION INTEGRATION corp_oidc "
                + "RULE ( USING CEL 'has_group(\"oncall\")' GRANT ROLE analyst, auditor ) "
                + ", RULE ( USING CEL 'has_scope(\"reports:view\")' GRANT ROLE reports_reader ) "
                + "COMMENT 'oidc mapping'");

        Assertions.assertInstanceOf(CreateRoleMappingCommand.class, plan);
        CreateRoleMappingCommand command = (CreateRoleMappingCommand) plan;
        Assertions.assertEquals("corp_mapping", command.getMappingName());
        Assertions.assertTrue(command.isSetIfNotExists());
        Assertions.assertEquals("corp_oidc", command.getIntegrationName());
        Assertions.assertEquals("oidc mapping", command.getComment());
        Assertions.assertEquals(2, command.getRules().size());
        Assertions.assertEquals("has_group(\"oncall\")", command.getRules().get(0).getCondition());
        Assertions.assertEquals(ImmutableSet.of("analyst", "auditor"),
                command.getRules().get(0).getGrantedRoles());
        Assertions.assertEquals("has_scope(\"reports:view\")", command.getRules().get(1).getCondition());
        Assertions.assertEquals(ImmutableSet.of("reports_reader"),
                command.getRules().get(1).getGrantedRoles());
    }

    @Test
    public void testCreateRoleMappingRejectInvalidRuleClause() {
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(
                "CREATE ROLE MAPPING corp_mapping "
                        + "ON AUTHENTICATION INTEGRATION corp_oidc "
                        + "RULE ( USING CEL 'true' GRANT analyst )"));
    }

    @Test
    public void testDropRoleMappingParse() {
        LogicalPlan plan1 = parser.parseSingle("DROP ROLE MAPPING corp_mapping");
        Assertions.assertInstanceOf(DropRoleMappingCommand.class, plan1);
        DropRoleMappingCommand drop1 = (DropRoleMappingCommand) plan1;
        Assertions.assertEquals("corp_mapping", drop1.getMappingName());
        Assertions.assertFalse(drop1.isIfExists());

        LogicalPlan plan2 = parser.parseSingle("DROP ROLE MAPPING IF EXISTS corp_mapping");
        Assertions.assertInstanceOf(DropRoleMappingCommand.class, plan2);
        DropRoleMappingCommand drop2 = (DropRoleMappingCommand) plan2;
        Assertions.assertTrue(drop2.isIfExists());
    }
}
