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
import org.apache.doris.nereids.trees.plans.commands.AlterAuthenticationIntegrationCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateAuthenticationIntegrationCommand;
import org.apache.doris.nereids.trees.plans.commands.DropAuthenticationIntegrationCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthenticationIntegrationParserTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testCreateAuthenticationIntegrationParse() {
        LogicalPlan plan = parser.parseSingle("CREATE AUTHENTICATION INTEGRATION IF NOT EXISTS corp_ldap "
                + "PROPERTIES ('type'='ldap', 'ldap.server'='ldap://127.0.0.1:389') "
                + "COMMENT 'ldap integration'");

        Assertions.assertInstanceOf(CreateAuthenticationIntegrationCommand.class, plan);
        CreateAuthenticationIntegrationCommand command = (CreateAuthenticationIntegrationCommand) plan;
        Assertions.assertEquals("corp_ldap", command.getIntegrationName());
        Assertions.assertTrue(command.isSetIfNotExists());
        Assertions.assertEquals("ldap", command.getProperties().get("type"));
        Assertions.assertEquals("ldap://127.0.0.1:389", command.getProperties().get("ldap.server"));
        Assertions.assertEquals("ldap integration", command.getComment());
    }

    @Test
    public void testCreateAuthenticationIntegrationRequireType() {
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(
                "CREATE AUTHENTICATION INTEGRATION corp_ldap "
                        + "PROPERTIES ('ldap.server'='ldap://127.0.0.1:389')"));
    }

    @Test
    public void testAlterAuthenticationIntegrationParse() {
        LogicalPlan alterProperties = parser.parseSingle("ALTER AUTHENTICATION INTEGRATION corp_ldap "
                + "SET PROPERTIES ('ldap.server'='ldap://127.0.0.1:1389')");
        Assertions.assertInstanceOf(AlterAuthenticationIntegrationCommand.class, alterProperties);

        AlterAuthenticationIntegrationCommand alterPropertiesCommand =
                (AlterAuthenticationIntegrationCommand) alterProperties;
        Assertions.assertEquals("corp_ldap", alterPropertiesCommand.getIntegrationName());
        Assertions.assertEquals(AlterAuthenticationIntegrationCommand.AlterType.SET_PROPERTIES,
                alterPropertiesCommand.getAlterType());
        Assertions.assertEquals("ldap://127.0.0.1:1389",
                alterPropertiesCommand.getProperties().get("ldap.server"));

        LogicalPlan unsetProperties = parser.parseSingle("ALTER AUTHENTICATION INTEGRATION corp_ldap "
                + "UNSET PROPERTIES ('ldap.server')");
        Assertions.assertInstanceOf(AlterAuthenticationIntegrationCommand.class, unsetProperties);

        AlterAuthenticationIntegrationCommand unsetPropertiesCommand =
                (AlterAuthenticationIntegrationCommand) unsetProperties;
        Assertions.assertEquals(AlterAuthenticationIntegrationCommand.AlterType.UNSET_PROPERTIES,
                unsetPropertiesCommand.getAlterType());
        Assertions.assertTrue(unsetPropertiesCommand.getUnsetProperties().contains("ldap.server"));

        LogicalPlan alterComment = parser.parseSingle(
                "ALTER AUTHENTICATION INTEGRATION corp_ldap SET COMMENT 'new comment'");
        Assertions.assertInstanceOf(AlterAuthenticationIntegrationCommand.class, alterComment);

        AlterAuthenticationIntegrationCommand alterCommentCommand =
                (AlterAuthenticationIntegrationCommand) alterComment;
        Assertions.assertEquals(AlterAuthenticationIntegrationCommand.AlterType.SET_COMMENT,
                alterCommentCommand.getAlterType());
        Assertions.assertEquals("new comment", alterCommentCommand.getComment());
    }

    @Test
    public void testAlterAuthenticationIntegrationRejectType() {
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(
                "ALTER AUTHENTICATION INTEGRATION corp_ldap SET PROPERTIES ('TYPE'='oidc')"));
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(
                "ALTER AUTHENTICATION INTEGRATION corp_ldap UNSET PROPERTIES ('TYPE')"));
    }

    @Test
    public void testDropAuthenticationIntegrationParse() {
        LogicalPlan plan1 = parser.parseSingle("DROP AUTHENTICATION INTEGRATION corp_ldap");
        Assertions.assertInstanceOf(DropAuthenticationIntegrationCommand.class, plan1);
        DropAuthenticationIntegrationCommand drop1 = (DropAuthenticationIntegrationCommand) plan1;
        Assertions.assertEquals("corp_ldap", drop1.getIntegrationName());
        Assertions.assertFalse(drop1.isIfExists());

        LogicalPlan plan2 = parser.parseSingle("DROP AUTHENTICATION INTEGRATION IF EXISTS corp_ldap");
        Assertions.assertInstanceOf(DropAuthenticationIntegrationCommand.class, plan2);
        DropAuthenticationIntegrationCommand drop2 = (DropAuthenticationIntegrationCommand) plan2;
        Assertions.assertTrue(drop2.isIfExists());
    }
}
