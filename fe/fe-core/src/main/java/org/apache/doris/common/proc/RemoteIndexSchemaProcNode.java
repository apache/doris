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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema/indexId/partitionName"
 * show index schema
 */
public class RemoteIndexSchemaProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra")
            .build();

    @FunctionalInterface
    interface RemoteSchemaFetcher {
        List<Column> fetch() throws AnalysisException;
    }

    private final ProcResult baseSchema;
    private final Set<String> bfColumns;
    private final RemoteSchemaFetcher remoteSchemaFetcher;

    RemoteIndexSchemaProcNode(ProcResult baseSchema, Set<String> bfColumns, RemoteSchemaFetcher remoteSchemaFetcher) {
        this.baseSchema = baseSchema;
        this.bfColumns = bfColumns;
        this.remoteSchemaFetcher = remoteSchemaFetcher;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<Column> remoteSchema = remoteSchemaFetcher.fetch();
        ProcResult remoteResult = IndexSchemaProcNode.createResult(remoteSchema, bfColumns, List.of());
        BaseProcResult result = new BaseProcResult(baseSchema.getColumnNames(), new ArrayList<>(baseSchema.getRows()));
        result.addRows(remoteResult.getRows());
        return result;
    }
}
