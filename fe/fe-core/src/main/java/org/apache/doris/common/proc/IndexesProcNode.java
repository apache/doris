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

import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/indexes
 * show indexes' detail info within a table
 */
public class IndexesProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Table").add("IndexId")
            .add("NonUnique").add("KeyName")
            .add("SeqInIndex").add("ColumnName").add("Collation").add("Cardinality")
            .add("SubPart").add("Packed").add("Null").add("IndexType").add("Comment")
            .add("Properties").build();

    private TableIf table;

    public IndexesProcNode(TableIf table) {
        this.table = table;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(table);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        if (table instanceof OlapTable) {
            table.readLock();
            try {
                List<Index> indexes = ((OlapTable) table).getIndexes();
                for (Index index : indexes) {
                    List<String> rowList = Arrays.asList(table.getName(),
                            String.valueOf(index.getIndexId()),
                            "",
                            index.getIndexName(),
                            "",
                            String.join(",", index.getColumns()),
                            "", "", "", "", "",
                            index.getIndexType().name(),
                            index.getComment(),
                            index.getPropertiesString());
                    result.addRow(rowList);
                }
            } finally {
                table.readUnlock();
            }
        }
        return result;
    }
}
