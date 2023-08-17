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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema/indexId"
 * show index schema
 */
public class RemoteIndexSchemaProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra")
            .build();

    private List<Column> schema;
    private Set<String> bfColumns;
    private TableIf table;

    public RemoteIndexSchemaProcDir(TableIf table, List<Column> schema, Set<String> bfColumns) {
        this.table = table;
        this.schema = schema;
        this.bfColumns = bfColumns;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(schema);
        List<Tablet> tablets = null;
        table.readLock();
        try {
            OlapTable olapTable = (OlapTable) table;
            tablets = olapTable.getAllTablets();
        } finally {
            table.readUnlock();
        }
        List<Column> remoteSchema = new FetchRemoteTabletSchemaUtil(tablets).fetch();
        if (remoteSchema == null || remoteSchema.isEmpty()) {
            throw new AnalysisException("fetch remote tablet schema failed");
        }
        this.schema = remoteSchema;
        return IndexSchemaProcNode.createResult(this.schema, this.bfColumns);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionString) throws AnalysisException {
        Preconditions.checkNotNull(table);

        List<String> partitionNameList = new ArrayList<String>(Arrays.asList(partitionString.split(",")));
        if (partitionNameList == null || partitionNameList.isEmpty()) {
            throw new AnalysisException("Describe table[" + table.getName() + "] failed");
        }
        List<Partition> partitions = Lists.newArrayList();
        table.readLock();
        try {
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                for (String partitionName : partitionNameList) {
                    Partition partition = olapTable.getPartition(partitionName);
                    if (partition == null) {
                        throw new AnalysisException("Partition " + partitionName + " does not exist");
                    }
                    partitions.add(partition);
                }
            } else {
                throw new AnalysisException("Describe table[" + table.getName() + "] failed");
            }
        } catch (Throwable t) {
            throw new AnalysisException("Describe table[" + table.getName() + "] failed");
        } finally {
            table.readUnlock();
        }
        return new RemoteIndexSchemaProcNode(partitions, this.schema, this.bfColumns);
    }
}
