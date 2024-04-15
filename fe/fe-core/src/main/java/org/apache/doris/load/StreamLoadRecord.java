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

package org.apache.doris.load;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class StreamLoadRecord {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecord.class);

    private String label;
    private String db;
    private String table;
    private String user;
    private String clientIp;
    private String status;
    private String message;
    private String url;
    private String totalRows;
    private String loadedRows;
    private String filteredRows;
    private String unselectedRows;
    private String loadBytes;
    private String startTime;
    private String finishTime;
    private String comment;


    public StreamLoadRecord(String label, String db, String table, String clientIp, String status,
            String message, String url, String totalRows, String loadedRows, String filteredRows, String unselectedRows,
            String loadBytes, String startTime, String finishTime, String user, String comment) {
        this.label = label;
        this.db = db;
        this.table = table;
        this.user = user;
        this.clientIp = clientIp;
        this.status = status;
        this.message = message;
        this.url = url;
        this.totalRows = totalRows;
        this.loadedRows = loadedRows;
        this.filteredRows = filteredRows;
        this.unselectedRows = unselectedRows;
        this.loadBytes = loadBytes;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.comment = comment;
    }

    public List<Comparable> getStreamLoadInfo() {
        List<Comparable> streamLoadInfo = Lists.newArrayList();
        streamLoadInfo.add(this.label);
        streamLoadInfo.add(this.db);
        streamLoadInfo.add(this.table);
        streamLoadInfo.add(this.clientIp);
        streamLoadInfo.add(this.status);
        streamLoadInfo.add(this.message);
        streamLoadInfo.add(this.url);
        streamLoadInfo.add(this.totalRows);
        streamLoadInfo.add(this.loadedRows);
        streamLoadInfo.add(this.filteredRows);
        streamLoadInfo.add(this.unselectedRows);
        streamLoadInfo.add(this.loadBytes);
        streamLoadInfo.add(this.startTime);
        streamLoadInfo.add(this.finishTime);
        streamLoadInfo.add(this.user);
        streamLoadInfo.add(this.comment);
        return streamLoadInfo;
    }

    public String getStatus() {
        return status;
    }

    public String getFinishTime() {
        return this.finishTime;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }
}
