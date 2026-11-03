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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ModifyTablePropertiesClauseTest {

    @Test
    public void testPartitionInvertedIndexStorageFormat() throws AnalysisException {
        String originalDeployMode = Config.deploy_mode;
        String originalCloudUniqueId = Config.cloud_unique_id;
        boolean originalRollout = Config.enable_partition_inverted_index_storage_format_rollout;
        try {
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "";
            Config.enable_partition_inverted_index_storage_format_rollout = true;

            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "V3");
            ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
            clause.analyze();
            Assertions.assertEquals(AlterOpType.MODIFY_TABLE_PROPERTY_SYNC, clause.getOpType());
            Assertions.assertFalse(clause.isNeedTableStable());
            Assertions.assertEquals("V3", clause.getProperties().get(
                    PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT));

            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "default");
            AnalysisException defaultError = Assertions.assertThrows(AnalysisException.class,
                    () -> new ModifyTablePropertiesClause(properties).analyze());
            Assertions.assertTrue(defaultError.getMessage().contains("does not support 'default'"));

            Config.deploy_mode = "local";
            properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_INVERTED_INDEX_STORAGE_FORMAT, "V3");
            AnalysisException localError = Assertions.assertThrows(AnalysisException.class,
                    () -> new ModifyTablePropertiesClause(properties).analyze());
            Assertions.assertTrue(localError.getMessage().contains("only supported in cloud mode"));
        } finally {
            Config.deploy_mode = originalDeployMode;
            Config.cloud_unique_id = originalCloudUniqueId;
            Config.enable_partition_inverted_index_storage_format_rollout = originalRollout;
        }
    }
}
