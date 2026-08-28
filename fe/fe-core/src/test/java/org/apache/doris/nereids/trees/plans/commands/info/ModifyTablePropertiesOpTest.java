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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertiesOpTest {

    @Test
    public void testExternalTablePropertiesAllowMultipleConnectorOptions() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("snapshot.num-retained.min", "2");
        properties.put("snapshot.num-retained.max", "5");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(properties);
        op.setTableName(new TableNameInfo("paimon", "db", "tbl"));

        op.validate(new ConnectContext());

        Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC,
                op.translateToLegacyAlterClause().getOpType());
    }

    @Test
    public void testInternalTableStillRejectsMultipleUnrelatedProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("snapshot.num-retained.min", "2");
        properties.put("snapshot.num-retained.max", "5");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(properties);
        op.setTableName(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db", "tbl"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> op.validate(new ConnectContext()));
        Assertions.assertTrue(exception.getMessage().contains("Can only set one table property"));
    }

    @Test
    public void testExternalAutoAnalyzePolicyCannotBeMixedWithConnectorOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY,
                PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY);
        properties.put("write.target-file-size-bytes", "134217728");
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(properties);
        op.setTableName(new TableNameInfo("iceberg", "db", "tbl"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> op.validate(new ConnectContext()));
        Assertions.assertTrue(exception.getMessage().contains("cannot be set with external table properties"));
    }

    @Test
    public void testExternalAutoAnalyzePolicyIsValidated() {
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(
                java.util.Collections.singletonMap(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY, "invalid"));
        op.setTableName(new TableNameInfo("iceberg", "db", "tbl"));

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> op.validate(new ConnectContext()));
        Assertions.assertTrue(exception.getMessage().contains("Table auto analyze policy only support"));
    }

    @Test
    public void testExternalAutoAnalyzePolicyKeepsDorisPropertyPath() throws Exception {
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(
                java.util.Collections.singletonMap(
                        PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY,
                        PropertyAnalyzer.DISABLE_AUTO_ANALYZE_POLICY));
        op.setTableName(new TableNameInfo("paimon", "db", "tbl"));

        op.validate(new ConnectContext());

        Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC,
                op.translateToLegacyAlterClause().getOpType());
    }
}
