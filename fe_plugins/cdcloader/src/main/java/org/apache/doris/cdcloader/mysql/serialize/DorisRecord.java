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

package org.apache.doris.cdcloader.mysql.serialize;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DorisRecord implements Serializable {
    public static DorisRecord EOF = new DorisRecord(null, null, Arrays.asList("EOF"));
    private String database;
    private String table;
    private List<String> rows;

    public DorisRecord() {}

    public DorisRecord(String database, String table, List<String> rows) {
        this.database = database;
        this.table = table;
        this.rows = rows;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getRows() {
        return rows;
    }

    public void setRows(List<String> rows) {
        this.rows = rows;
    }

    public void addRow(String row) {
        this.rows.add(row);
    }

    public String identifier() {
        return database + "." + table;
    }
}
