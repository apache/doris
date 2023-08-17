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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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

    private List<Partition> partitions;
    private List<Column> schema;
    private Set<String> bfColumns;

    public RemoteIndexSchemaProcNode(List<Partition> partitions, List<Column> schema, Set<String> bfColumns) {
        this.partitions = partitions;
        this.schema = schema;
        this.bfColumns = bfColumns;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(schema);
        Preconditions.checkNotNull(partitions);
        List<Tablet> tablets = Lists.newArrayList();
        for (Partition partition : partitions) {
            MaterializedIndex idx = partition.getBaseIndex();
            for (Tablet tablet : idx.getTablets()) {
                tablets.add(tablet);
            }
        }
        List<Column> remoteSchema = new FetchRemoteTabletSchemaUtil(tablets).fetch();
        if (remoteSchema == null || remoteSchema.isEmpty()) {
            throw new AnalysisException("fetch remote tablet schema failed");
        }
        this.schema = remoteSchema;
        return IndexSchemaProcNode.createResult(this.schema, this.bfColumns);
    }
}
