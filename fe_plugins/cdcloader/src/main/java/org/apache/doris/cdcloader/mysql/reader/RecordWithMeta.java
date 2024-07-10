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

package org.apache.doris.cdcloader.mysql.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecordWithMeta {

    private Map<String, String> meta;
    private List<String> records;
    private List<String> sqls;

    public RecordWithMeta(Map<String, String> meta) {
        this.meta = meta;
        this.records = new ArrayList<>();
        this.sqls = new ArrayList<>();
    }

    public RecordWithMeta(Map<String, String> meta, List<String> records, List<String> sqls) {
        this.meta = meta;
        this.records = records;
        this.sqls = sqls;
    }

    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    public List<String> getRecords() {
        return records;
    }

    public void setRecords(List<String> records) {
        this.records = records;
    }

    public List<String> getSqls() {
        return sqls;
    }

    public void setSqls(List<String> sqls) {
        this.sqls = sqls;
    }
}
