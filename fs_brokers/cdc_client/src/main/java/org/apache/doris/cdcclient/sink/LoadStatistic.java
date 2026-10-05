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

package org.apache.doris.cdcclient.sink;

import lombok.Data;

@Data
public class LoadStatistic {
    private long filteredRows = 0;
    private long loadedRows = 0;
    private long loadBytes = 0;

    public void add(RespContent respContent) {
        this.filteredRows += respContent.getNumberFilteredRows();
        this.loadedRows += respContent.getNumberLoadedRows();
        this.loadBytes += respContent.getLoadBytes();
    }

    public void clear() {
        this.filteredRows = 0;
        this.loadedRows = 0;
        this.loadBytes = 0;
    }
}
