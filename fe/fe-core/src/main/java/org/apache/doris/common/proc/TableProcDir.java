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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/*
 * SHOW PROC /dbs/dbId/tableId/
 * show choice to schema or to partitions
 */
public class TableProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("Nodes").build();

    public static final String INDEX_SCHEMA = "index_schema";
    private static final String PARTITIONS = "partitions";
    private static final String TEMP_PARTITIONS = "temp_partitions";
    private static final String INDEXES = "indexes";

    private static final ImmutableList<String> CHILDREN_NODES =
            new ImmutableList.Builder<String>().add(PARTITIONS).add(TEMP_PARTITIONS).add(INDEX_SCHEMA)
                    .add(INDEXES).build();

    private DatabaseIf db;
    private TableIf table;

    public TableProcDir(DatabaseIf db, TableIf table) {
        this.db = db;
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        for (String name : CHILDREN_NODES) {
            result.addRow(Lists.newArrayList(name));
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String entryName) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(table);

        if (Strings.isNullOrEmpty(entryName)) {
            throw new AnalysisException("Entry name is null");
        }

        if (entryName.equals(PARTITIONS)) {
            if (table.isManagedTable()) {
                return new PartitionsProcDir((Database) db, (OlapTable) table, false);
            } else if (table.getType() == TableType.ELASTICSEARCH) {
                return new EsPartitionsProcDir((Database) db, (EsTable) table);
            } else {
                throw new AnalysisException("Table[" + table.getName() + "] is not a OLAP or ELASTICSEARCH table");
            }
        } else if (entryName.equals(TEMP_PARTITIONS)) {
            if (table instanceof OlapTable) {
                return new PartitionsProcDir((Database) db, (OlapTable) table, true);
            } else {
                throw new AnalysisException("Table[" + table.getName() + "] does not have temp partitions");
            }
        } else if (entryName.equals(INDEX_SCHEMA)) {
            return new IndexInfoProcDir(db, table);
        } else if (entryName.equals(INDEXES)) {
            return new IndexesProcNode(table);
        } else {
            throw new AnalysisException("Not implemented yet: " + entryName);
        }
    }

}
