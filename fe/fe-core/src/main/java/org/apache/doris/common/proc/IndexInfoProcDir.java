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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema
 * show indexNames(to schema)
 */
public class IndexInfoProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>().add("IndexId").add("IndexName").add("SchemaVersion").add("SchemaHash")
                    .add("ShortKeyColumnCount").add("StorageType").add("Keys").build();

    private DatabaseIf db;
    private TableIf table;

    public IndexInfoProcDir(DatabaseIf db, TableIf table) {
        this.db = db;
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        table.readLock();
        try {
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;

                // indices order
                List<Long> indices = Lists.newArrayList();
                indices.add(olapTable.getBaseIndexId());
                indices.addAll(olapTable.getIndexIdListExceptBaseIndex());

                for (long indexId : indices) {
                    MaterializedIndexMeta indexMeta = olapTable.getIndexIdToMeta().get(indexId);

                    String type = olapTable.getKeysType().name();
                    StringBuilder builder = new StringBuilder();
                    builder.append(type).append("(");
                    List<String> columnNames = Lists.newArrayList();
                    List<Column> columns = olapTable.getSchemaByIndexId(indexId);
                    for (Column column : columns) {
                        if (column.isKey()) {
                            columnNames.add(column.getName());
                        }
                    }
                    builder.append(Joiner.on(", ").join(columnNames)).append(")");

                    result.addRow(Lists.newArrayList(String.valueOf(indexId),
                            olapTable.getIndexNameById(indexId),
                            String.valueOf(indexMeta.getSchemaVersion()),
                            String.valueOf(indexMeta.getSchemaHash()),
                            String.valueOf(indexMeta.getShortKeyColumnCount()),
                            indexMeta.getStorageType().name(),
                            builder.toString()));
                }
            } else {
                result.addRow(Lists.newArrayList(String.valueOf(table.getId()), table.getName(), "", "", "", "", ""));
            }

            return result;
        } finally {
            table.readUnlock();
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String idxIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        long idxId;
        try {
            idxId = Long.valueOf(idxIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid index id format: " + idxIdStr);
        }

        table.readLock();
        try {
            List<Column> schema = null;
            Set<String> bfColumns = null;
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                schema = olapTable.getSchemaByIndexId(idxId);
                if (schema == null) {
                    throw new AnalysisException("Index " + idxId + " does not exist");
                }
                bfColumns = olapTable.getCopiedBfColumns();
                if (olapTable.hasVariantColumns()) {
                    return new RemoteIndexSchemaProcDir(table, schema, bfColumns);
                }
            } else {
                schema = table.getBaseSchema();
            }
            return new IndexSchemaProcNode(schema, bfColumns);
        } finally {
            table.readUnlock();
        }
    }
}
