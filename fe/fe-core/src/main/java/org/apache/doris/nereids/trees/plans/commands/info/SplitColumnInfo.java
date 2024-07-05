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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * The SplitColumnInfo class encapsulates information required for splitting and filtering SQL statements
 * during batch INSERT operations. It holds details about the column used for data partitioning and the
 * associated table
 */
public class SplitColumnInfo implements Writable {

    @SerializedName(value = "tni")
    private TableNameInfo tableNameInfo;

    @SerializedName(value = "scn")
    private String columnName;

    /**
     * Constructor for SplitColumnInfo
     *
     * @param parts List of strings containing the parts of the split column info eg. [ctl,db, tbl, col]
     */
    public SplitColumnInfo(List<String> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("split column info parts can't be empty");
        }
        if (parts.size() <= 1) {
            throw new IllegalArgumentException("split column info parts must have at least 2 elements");
        }
        this.columnName = parts.get(parts.size() - 1);
        this.tableNameInfo = new TableNameInfo(parts.subList(0, parts.size() - 1));
    }

    /**
     * Analyze
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                .getDbOrAnalysisException(tableNameInfo.getDb())
                .getTableOrAnalysisException(tableNameInfo.getTbl());

        Column column = table.getFullSchema().stream().filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst().orElse(null);
        if (null == column) {
            throw new IllegalArgumentException(columnName + " is not a column in table " + tableNameInfo.getTbl());
        }
        if (!column.getDataType().isIntegerType()) {
            throw new IllegalArgumentException(columnName + " is not an integer type column in table "
                    + tableNameInfo.getTbl());
        }
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
