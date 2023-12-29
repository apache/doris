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

package org.apache.doris.insertoverwrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class InsertOverwriteTask {
    @SerializedName(value = "cancel")
    private boolean cancel;
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tId")
    private long tableId;
    @SerializedName(value = "tpns")
    private List<String> tempPartitionNames;

    public InsertOverwriteTask() {
    }

    public InsertOverwriteTask(long dbId, long tableId, List<String> tempPartitionNames) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tempPartitionNames = tempPartitionNames;
        this.cancel = false;
    }

    public boolean isCancel() {
        return cancel;
    }

    public void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames;
    }

    public void setTempPartitionNames(List<String> tempPartitionNames) {
        this.tempPartitionNames = tempPartitionNames;
    }

    public OlapTable getTable() throws DdlException {
        return (OlapTable) Env.getCurrentEnv().getInternalCatalog().getDbOrDdlException(dbId)
                .getTableOrDdlException(tableId,
                        TableType.OLAP);
    }

    @Override
    public String toString() {
        return "InsertOverwriteTask{"
                + "cancel=" + cancel
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", tempPartitionNames=" + tempPartitionNames
                + '}';
    }
}
