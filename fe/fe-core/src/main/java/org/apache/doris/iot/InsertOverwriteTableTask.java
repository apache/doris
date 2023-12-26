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

package org.apache.doris.iot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class InsertOverwriteTableTask {
    private static final Logger LOG = LogManager.getLogger(InsertOverwriteTableTask.class);

    private boolean normal;
    private long dbId;
    private long tableId;
    private List<String> tempPartitionNames;

    public InsertOverwriteTableTask() {
    }

    public InsertOverwriteTableTask(long dbId, long tableId, List<String> tempPartitionNames) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tempPartitionNames = tempPartitionNames;
        this.normal = true;
    }

    public boolean isNormal() {
        return normal;
    }

    public void setNormal(boolean normal) {
        this.normal = normal;
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
}
