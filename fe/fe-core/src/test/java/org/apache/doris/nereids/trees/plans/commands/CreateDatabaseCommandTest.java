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

import org.apache.doris.analysis.DbName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CreateDatabaseCommandTest extends TestWithFeService {
    @Test
    public void testValidateNormal() throws UserException {
        CreateDatabaseCommand command = new CreateDatabaseCommand(false, new DbName("", "test"), new HashMap<>());

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("test", command.getDbName());
    }

    @Test
    public void testAnalyzeWithException() {
        CreateDatabaseCommand command = new CreateDatabaseCommand(false, new DbName("", ""), new HashMap<>());
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testAnalyzeIcebergNormal() {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.database", "doris");
        properties.put("iceberg.hive.metastore.uris", "thrift://127.0.0.1:9087");

        CreateDatabaseCommand command = new CreateDatabaseCommand(false, new DbName("ctl", "test"), properties);

        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
        Assertions.assertEquals("ctl", command.getCtlName());
        Assertions.assertEquals("test", command.getDbName());
    }

    @Test
    public void testAnalyzeIcebergWithException() {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.database", "doris");
        properties.put("iceberg.hive.metastore.uris", "thrift://127.0.0.1:9087");

        CreateDatabaseCommand command = new CreateDatabaseCommand(false, new DbName("", ""), properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }
}
