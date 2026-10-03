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

package org.apache.doris.planner;

import org.apache.doris.common.Config;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.load.NereidsLoadUtils;
import org.apache.doris.nereids.load.NereidsStreamLoadTask;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StreamLoadPlannerTest {
    @Test
    public void testParseStmt() throws Exception {
        String sql = new String("k1, k2, k3=abc(), k4=default_value()");
        List<Expression> expressions = NereidsLoadUtils.parseExpressionSeq(sql);
        Assertions.assertEquals(4, expressions.size());
    }

    @Test
    public void testStreamLoadSinkUploadRequestOverridesDefaultConfig() throws Exception {
        boolean original = Config.cloud_stream_load_default_memtable_sink_upload;
        try {
            for (boolean configured : new boolean[] {false, true}) {
                Config.cloud_stream_load_default_memtable_sink_upload = configured;
                TStreamLoadPutRequest request = new TStreamLoadPutRequest();
                request.setLoadId(new TUniqueId(1, 2));
                request.setTxnId(3);
                request.setFileType(TFileType.FILE_STREAM);
                request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
                Assertions.assertEquals(configured, NereidsStreamLoadTask.fromTStreamLoadPutRequest(request)
                        .isCloudMemtableSinkUpload());
                for (boolean requested : new boolean[] {false, true}) {
                    request.setCloudMemtableSinkUpload(requested);
                    Assertions.assertEquals(requested, NereidsStreamLoadTask.fromTStreamLoadPutRequest(request)
                            .isCloudMemtableSinkUpload());
                }
            }
        } finally {
            Config.cloud_stream_load_default_memtable_sink_upload = original;
        }
    }

    @Test
    public void testExprIdGenerator() {
        IdGenerator<ExprId> exprIdGenerator1 = StatementScopeIdGenerator.getExprIdGenerator();
        CascadesContext context = CascadesContext.initTempContext();
        IdGenerator<ExprId> exprIdGenerator2 = context.getStatementContext().getExprIdGenerator();
        // we get different IdGenerator instance
        Assertions.assertTrue(exprIdGenerator1 != exprIdGenerator2);
    }
}
