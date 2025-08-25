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
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowIndexCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    @Test
    void testAnalyze() throws Exception {
        TableNameInfo tableName = new TableNameInfo("test", "abc");
        ShowIndexCommand si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);
        tableName = new TableNameInfo("hive_catalog", "test", "abc");
        si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);
        tableName = new TableNameInfo("internal", "test", "abc");
        si = new ShowIndexCommand(tableName);
        si.analyze(connectContext);

        tableName = new TableNameInfo("", "");
        si = new ShowIndexCommand(tableName);
        ShowIndexCommand finalSi1 = si;
        Assertions.assertThrows(AnalysisException.class, () -> finalSi1.analyze(connectContext));

        connectContext.setDatabase(null);
        tableName = new TableNameInfo("", "test");
        si = new ShowIndexCommand(tableName);
        ShowIndexCommand finalSi2 = si;
        Assertions.assertThrows(AnalysisException.class, () -> finalSi2.analyze(connectContext));
    }
}
