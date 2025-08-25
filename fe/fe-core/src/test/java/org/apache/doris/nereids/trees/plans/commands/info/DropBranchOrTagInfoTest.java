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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropBranchOrTagInfoTest {

    @Test
    public void testDropTagToSql() {
        DropTagInfo dropTagInfo = new DropTagInfo("tag", false);
        String expected = "DROP TAG tag";
        Assertions.assertEquals(expected, dropTagInfo.toSql());
    }

    @Test
    public void testDropTagIfExistsToSql() {
        DropTagInfo dropTagInfo = new DropTagInfo("tag", true);
        String expected = "DROP TAG IF EXISTS tag";
        Assertions.assertEquals(expected, dropTagInfo.toSql());
    }

    @Test
    public void testDropBranchToSql() {
        DropBranchInfo dropBranchInfo = new DropBranchInfo("branch", false);
        String expected = "DROP BRANCH branch";
        Assertions.assertEquals(expected, dropBranchInfo.toSql());
    }

    @Test
    public void testDropBranchIfExistsToSql() {
        DropBranchInfo dropBranchInfo = new DropBranchInfo("branch", true);
        String expected = "DROP BRANCH IF EXISTS branch";
        Assertions.assertEquals(expected, dropBranchInfo.toSql());
    }
}
