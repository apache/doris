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

import java.util.List;

import com.google.common.collect.Lists;

public class BaseProcResult implements ProcResult {
    protected List<String> names;
    protected List<List<String>> rows;

    public BaseProcResult() {
        names = Lists.newArrayList();
        rows = Lists.newArrayList();
    }

    public BaseProcResult(List<String> col, List<List<String>> val) {
        this.names = col;
        this.rows = val;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public void addRow(List<String> row) {
        rows.add(row);
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }

    @Override
    public List<String> getColumnNames() {
        return names;
    }

    @Override
    public List<List<String>> getRows() {
        return rows;
    }
}
