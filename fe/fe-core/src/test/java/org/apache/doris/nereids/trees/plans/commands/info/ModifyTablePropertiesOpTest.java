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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertiesOpTest {

    @Test
    public void testExternalTablePropertiesAllowConnectorOptions() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("file.format", "parquet");
        properties.put("write-buffer-size", "128 mb");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(properties);
        op.setTableName(new TableNameInfo("external", "db", "tbl"));

        op.validate(new ConnectContext());
    }

    @Test
    public void testExternalAutoAnalyzePolicyCannotBeMixedWithConnectorOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY,
                PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY);
        properties.put("file.format", "parquet");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(properties);
        op.setTableName(new TableNameInfo("external", "db", "tbl"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> op.validate(new ConnectContext()));
        Assertions.assertTrue(exception.getMessage().contains("cannot be set with external table properties"));
    }

    @Test
    public void testExternalAutoAnalyzePolicyIsValidated() {
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(
                java.util.Collections.singletonMap(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY, "invalid"));
        op.setTableName(new TableNameInfo("external", "db", "tbl"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> op.validate(new ConnectContext()));
        Assertions.assertTrue(exception.getMessage().contains("Table auto analyze policy only support"));
    }
}
