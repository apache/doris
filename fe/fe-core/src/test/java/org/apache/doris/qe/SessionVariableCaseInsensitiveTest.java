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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter.PreRewriteStrategy;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AesCryptoFunction;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TSerdeDialect;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class SessionVariableCaseInsensitiveTest {
    private static void set(SessionVariable variables, String name, String value) throws Exception {
        VariableMgr.setVar(variables, new SetVar(SetType.SESSION, name, new StringLiteral(value)));
    }

    @Test
    void serdeDialectAcceptsAnyLetterCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "serde_dialect", "PRESTO");
        Assertions.assertEquals(TSerdeDialect.PRESTO, variables.getSerdeDialect());
        set(variables, "serde_dialect", "Hive");
        Assertions.assertEquals(TSerdeDialect.HIVE, variables.getSerdeDialect());
        set(variables, "serde_dialect", "DORIS");
        Assertions.assertEquals(TSerdeDialect.DORIS, variables.getSerdeDialect());
    }

    @Test
    void sqlDialectIsStoredInLowerCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "sql_dialect", "DORIS");
        Assertions.assertEquals("doris", variables.getSqlDialect());
    }

    @Test
    void blockEncryptionModeIsUsedByCryptoFunctionsInAnyLetterCase() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            set(context.getSessionVariable(), "block_encryption_mode", "aes_256_cbc");
            Assertions.assertEquals("AES_256_CBC", AesCryptoFunction.getDefaultBlockEncryptionMode().getValue());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void preMaterializedViewRewriteStrategyAcceptsAnyLetterCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "pre_materialized_view_rewrite_strategy", "not_in_rbo");
        Assertions.assertEquals(PreRewriteStrategy.NOT_IN_RBO,
                PreRewriteStrategy.getEnum(variables.getPreMaterializedViewRewriteStrategy()));
    }

    @Test
    void forceSortAlgorithmIsStoredInLowerCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "force_sort_algorithm", "HEAP");
        Assertions.assertEquals("heap", variables.forceSortAlgorithm);
    }

    @Test
    void fileCacheBasePathNormalizesOnlyTheRandomKeyword() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "file_cache_base_path", "RANDOM");
        Assertions.assertEquals("random", variables.getFileCacheBasePath());
        set(variables, "file_cache_base_path", "/Data/MixedCaseCache");
        Assertions.assertEquals("/Data/MixedCaseCache", variables.getFileCacheBasePath());
    }

    @Test
    void hiveTextCompressionIsNormalized() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "hive_text_compression", "UNCOMPRESSED");
        Assertions.assertEquals("plain", variables.hiveTextCompression());
        set(variables, "hive_text_compression", "ZSTD");
        Assertions.assertEquals("zstd", variables.hiveTextCompression());
    }

    @Test
    void sqlConvertorFeaturesAreSentInLowerCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "enable_sql_convertor_features", "CTAS,DELETE_ALL_COMMENT");
        Assertions.assertEquals(Arrays.asList("ctas", "delete_all_comment"),
                Arrays.asList(variables.getSqlConvertorFeatures()));
    }

    @Test
    void mongodbReadConcernHashDoesNotDependOnLetterCase() throws Exception {
        SessionVariable upper = new SessionVariable();
        SessionVariable lower = new SessionVariable();
        set(upper, "mongodb_read_concern", "LOCAL");
        set(lower, "mongodb_read_concern", "local");
        Assertions.assertEquals(lower.getAffectQueryResultVariableHashCode(),
                upper.getAffectQueryResultVariableHashCode());
    }

    @Test
    void traceIdKeyIsCaseInsensitiveAndValueKeepsItsCase() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            SessionVariable variables = context.getSessionVariable();
            set(variables, "session_context", "TRACE_ID:AbCdEf");
            Assertions.assertEquals("AbCdEf", variables.getTraceId());
            Assertions.assertEquals("AbCdEf", context.traceId());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    void shapeNodeNamesMatchInAnyLetterCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "ignore_shape_nodes", "physicaldistribute, PHYSICALPROJECT");
        Assertions.assertTrue(variables.getIgnoreShapePlanNodes().contains("PhysicalDistribute"));
        Assertions.assertTrue(variables.getIgnoreShapePlanNodes().contains("PhysicalProject"));
        set(variables, "detail_shape_nodes", "physicalhashjoin, PHYSICALFILTER");
        Assertions.assertTrue(variables.getDetailShapePlanNodesSet().contains("PhysicalHashJoin"));
        Assertions.assertTrue(variables.getDetailShapePlanNodesSet().contains("PhysicalFilter"));
    }

    @Test
    void groupCommitIsStoredInLowerCase() throws Exception {
        SessionVariable variables = new SessionVariable();
        set(variables, "group_commit", "ASYNC_MODE");
        Assertions.assertEquals("async_mode", variables.getGroupCommit());
    }

    @Test
    void nodeRoleTagIsValidInAnyLetterCase() {
        Assertions.assertTrue(Tag.validNodeRoleTag("COMPUTATION"));
        Assertions.assertTrue(Tag.validNodeRoleTag("Mix"));
        Assertions.assertFalse(Tag.validNodeRoleTag("unknown"));
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, Tag.VALUE_DEFAULT_TAG,
                Tag.TYPE_ROLE, "COMPUTATION"));
        Assertions.assertTrue(backend.isComputeNode());
        Assertions.assertEquals(Tag.VALUE_COMPUTATION, backend.getTagMap().get(Tag.TYPE_ROLE));
    }

    @Test
    void defaultCompressionResolvesDefaultAndExplicitTypes() throws Exception {
        String original = Config.default_compression_type;
        try {
            Config.default_compression_type = "default_compression";
            Assertions.assertEquals(TCompressionType.LZ4F,
                    PropertyAnalyzer.getCompressionTypeFromProperties(
                            Collections.singletonMap(PropertyAnalyzer.PROPERTIES_COMPRESSION,
                                    "DEFAULT_COMPRESSION")));
            Config.default_compression_type = "zstd";
            Assertions.assertEquals(TCompressionType.ZSTD,
                    PropertyAnalyzer.getCompressionTypeFromProperties(
                            Collections.singletonMap(PropertyAnalyzer.PROPERTIES_COMPRESSION,
                                    "default_compression")));
        } finally {
            Config.default_compression_type = original;
        }
    }
}
