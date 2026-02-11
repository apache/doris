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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for SET PASSWORD statement parsing.
 * This test verifies that UserIdentity is correctly parsed with proper isDomain flag.
 */
public class SetPasswordParseTest {

    /**
     * Test SET PASSWORD FOR 'user'@'%' - should NOT be a domain user.
     * Bug fix: Previously, ATSIGN() check incorrectly set isDomain=true.
     */
    @Test
    public void testSetPasswordNonDomainUser() throws AnalysisException {
        String sql = "SET PASSWORD FOR 't1'@'%' = PASSWORD('123')";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetPassVarOp setPassOp = (SetPassVarOp) command.getSetVarOps().get(0);

        UserIdentity userIdent = setPassOp.getUserIdent();
        userIdent.analyze(); // Need to analyze before calling getQualifiedUser()
        Assertions.assertEquals("t1", userIdent.getQualifiedUser());
        Assertions.assertEquals("%", userIdent.getHost());
        Assertions.assertFalse(userIdent.isDomain(),
                "User 't1'@'%' should NOT be a domain user (isDomain should be false)");
    }

    /**
     * Test SET PASSWORD FOR 'user'@'192.168.1.1' - single quotes, non-domain user.
     */
    @Test
    public void testSetPasswordWithSingleQuotes() throws AnalysisException {
        String sql = "SET PASSWORD FOR 'testuser'@'192.168.1.1' = PASSWORD('pass')";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetPassVarOp setPassOp = (SetPassVarOp) command.getSetVarOps().get(0);

        UserIdentity userIdent = setPassOp.getUserIdent();
        userIdent.analyze();
        Assertions.assertEquals("testuser", userIdent.getQualifiedUser());
        Assertions.assertEquals("192.168.1.1", userIdent.getHost());
        Assertions.assertFalse(userIdent.isDomain(),
                "User with single quotes should NOT be a domain user");
    }

    /**
     * Test SET PASSWORD FOR 'user'@("domain") - SHOULD be a domain user.
     * Domain users are identified by parentheses around host.
     */
    @Test
    public void testSetPasswordDomainUser() throws AnalysisException {
        String sql = "SET PASSWORD FOR 'domainuser'@(\"example.com\") = PASSWORD('pass')";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetPassVarOp setPassOp = (SetPassVarOp) command.getSetVarOps().get(0);

        UserIdentity userIdent = setPassOp.getUserIdent();
        userIdent.analyze();
        Assertions.assertEquals("domainuser", userIdent.getQualifiedUser());
        Assertions.assertEquals("example.com", userIdent.getHost());
        Assertions.assertTrue(userIdent.isDomain(),
                "User with parentheses SHOULD be a domain user (isDomain should be true)");
    }

    /**
     * Test SET PASSWORD without FOR clause - should use current user.
     */
    @Test
    public void testSetPasswordNoForClause() {
        String sql = "SET PASSWORD = PASSWORD('newpass')";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetPassVarOp setPassOp = (SetPassVarOp) command.getSetVarOps().get(0);

        // Without FOR clause, userIdent should be null (will be set to current user later)
        Assertions.assertNull(setPassOp.getUserIdent(),
                "SET PASSWORD without FOR clause should have null userIdent");
    }

    /**
     * Test SET PASSWORD FOR 'user'@'%%' - verify isDomain is false even with %%.
     */
    @Test
    public void testSetPasswordDoublePercent() throws AnalysisException {
        String sql = "SET PASSWORD FOR 'testuser'@'%%' = PASSWORD('pass')";
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        SetOptionsCommand command = (SetOptionsCommand) plan;
        SetPassVarOp setPassOp = (SetPassVarOp) command.getSetVarOps().get(0);

        UserIdentity userIdent = setPassOp.getUserIdent();
        userIdent.analyze();
        Assertions.assertEquals("testuser", userIdent.getQualifiedUser());
        Assertions.assertEquals("%%", userIdent.getHost());
        Assertions.assertFalse(userIdent.isDomain(),
                "User 'testuser'@'%%' with single quotes should NOT be a domain user");
    }
}
