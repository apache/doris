// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.proc;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.thrift.TStorageType;

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
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("IndexName").add("SchemaVersion").add("SchemaHash").add("ShortKeyColumnCount")
            .add("StorageType").add("Keys")
            .build();

    private Database db;
    private Table table;

    public IndexInfoProcDir(Database db, Table table) {
        this.db = db;
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        db.readLock();
        try {
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;

                // indices order
                List<Long> indices = Lists.newArrayList();
                indices.add(olapTable.getId());
                for (Long indexId : olapTable.getIndexIdToSchema().keySet()) {
                    if (indexId != olapTable.getId()) {
                        indices.add(indexId);
                    }
                }

                for (long indexId : indices) {
                    int schemaVersion = olapTable.getSchemaVersionByIndexId(indexId);
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    short shortKeyColumnCount = olapTable.getShortKeyColumnCountByIndexId(indexId);
                    TStorageType storageType = olapTable.getStorageTypeByIndexId(indexId);
                    String indexName = olapTable.getIndexNameById(indexId);

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

                    result.addRow(Lists.newArrayList(indexName,
                                                     String.valueOf(schemaVersion),
                                                     String.valueOf(schemaHash),
                                                     String.valueOf(shortKeyColumnCount),
                                                     storageType.name(),
                                                     builder.toString()));
                }
            } else {
                result.addRow(Lists.newArrayList(table.getName(), "", "", "", "", ""));
            }

            return result;
        } finally {
            db.readUnlock();
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String indexName) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);
        
        db.readLock();
        try {
            List<Column> schema = null;
            Set<String> bfColumns = null;
            if (table.getType() == TableType.OLAP) {
                OlapTable olapTable = (OlapTable) table;
                Long indexId = olapTable.getIndexIdByName(indexName);
                if (indexId == null) {
                    throw new AnalysisException("Index[" + indexName + "] does not exist in table["
                            + table.getName() + "]");
                }
                schema = olapTable.getSchemaByIndexId(indexId);

                bfColumns = olapTable.getCopiedBfColumns();
            } else {
                schema = table.getBaseSchema();
            }
            return new IndexSchemaProcNode(schema, bfColumns);
        } finally {
            db.readUnlock();
        }
    }

}
