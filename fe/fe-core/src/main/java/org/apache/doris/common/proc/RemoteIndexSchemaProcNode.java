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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Fetches extended column metadata for a snapshot of an index or its selected partitions.
 */
public class RemoteIndexSchemaProcNode implements ProcNodeInterface {
    private final ProcResult baseSchema;
    private final Set<String> bfColumns;
    private final Supplier<List<Column>> remoteSchemaFetcher;

    public RemoteIndexSchemaProcNode(ProcResult baseSchema, Set<String> bfColumns, List<Tablet> tablets) {
        this(baseSchema, bfColumns, new FetchRemoteTabletSchemaUtil(tablets)::fetch);
    }

    public RemoteIndexSchemaProcNode(ProcResult baseSchema, Set<String> bfColumns,
            Supplier<List<Column>> remoteSchemaFetcher) {
        this.baseSchema = baseSchema;
        this.bfColumns = bfColumns;
        this.remoteSchemaFetcher = remoteSchemaFetcher;
    }

    @Override
    public ProcResult fetchResult() {
        List<Column> remoteSchema = remoteSchemaFetcher.get();
        ProcResult remoteResult = IndexSchemaProcNode.createResult(remoteSchema, bfColumns, Collections.emptyList());
        BaseProcResult result = new BaseProcResult(baseSchema.getColumnNames(), new ArrayList<>(baseSchema.getRows()));
        result.addRows(remoteResult.getRows());
        return result;
    }
}
