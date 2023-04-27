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

package org.apache.doris.external.iceberg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the record of Iceberg table automating creation in an Iceberg database
 */
public class IcebergTableCreationRecord {
    private static final Logger LOG = LogManager.getLogger(IcebergTableCreationRecord.class);

    private long dbId;
    private long tableId;
    private String db;
    private String table;
    private String status;
    private String createTime;
    private String errorMsg;

    public IcebergTableCreationRecord(long dbId, long tableId, String db, String table, String status,
                                      String createTime, String errorMsg) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.db = db;
        this.table = table;
        this.status = status;
        this.createTime = createTime;
        this.errorMsg = errorMsg;
    }

    public List<Comparable> getTableCreationRecord() {
        List<Comparable> record = new ArrayList<>();
        record.add(this.db);
        record.add(this.table);
        record.add(this.status);
        record.add(this.createTime);
        record.add(this.errorMsg);
        return record;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getStatus() {
        return status;
    }

    public String getCreateTime() {
        return createTime;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
}
