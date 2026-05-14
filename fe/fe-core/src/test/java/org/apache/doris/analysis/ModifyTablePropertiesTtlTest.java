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

import org.apache.doris.cloud.alter.CloudSchemaChangeHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyTablePropertiesOp;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ModifyTablePropertiesTtlTest {
    @Test
    public void testLegacyAlterRejectsFileCacheTtl() {
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(ttlProperties("31536000"));

        AnalysisException exception = Assert.assertThrows(AnalysisException.class, clause::analyze);
        assertTtlAlterRejected(exception);
    }

    @Test
    public void testNereidsAlterRejectsFileCacheTtl() {
        ModifyTablePropertiesOp op = new ModifyTablePropertiesOp(ttlProperties("31536000"));

        AnalysisException exception = Assert.assertThrows(AnalysisException.class, () -> op.validate(null));
        assertTtlAlterRejected(exception);
    }

    @Test
    public void testMixedPropertiesStillRejectsFileCacheTTL() {
        Map<String, String> properties = ttlProperties("31536000");
        properties.put(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION, "true");

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new ModifyTablePropertiesClause(properties).analyze());
        assertTtlAlterRejected(exception);
    }

    @Test
    public void testCloudSchemaChangeRejectsFileCacheTtlBeforeExecution() {
        CloudSchemaChangeHandler handler = new CloudSchemaChangeHandler();

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> handler.updateTableProperties(null, "tbl", ttlProperties("31536000")));
        assertTtlAlterRejected(exception);
    }

    private static Map<String, String> ttlProperties(String ttlSeconds) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS, ttlSeconds);
        return properties;
    }

    private static void assertTtlAlterRejected(AnalysisException exception) {
        Assert.assertTrue(exception.getMessage().contains(PropertyAnalyzer.FILE_CACHE_TTL_ALTER_RESTRICTION_MSG));
    }
}
