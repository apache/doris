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

package org.apache.doris.load.sync.model;

import com.google.common.collect.Lists;

import java.util.List;

// Equivalent to a batch send to be
// T = dataType
public class Data<T> {
    private List<T> datas;

    public Data() {
        this(Lists.newArrayList());
    }

    public Data(List<T> datas) {
        this.datas = datas;
    }

    public List<T> getDatas() {
        return datas;
    }

    public void addRow(T row) {
        this.datas.add(row);
    }

    public void addRows(List<T> rows) {
        this.datas.addAll(rows);
    }

    public boolean isNotEmpty() {
        return datas.size() > 0;
    }
}
