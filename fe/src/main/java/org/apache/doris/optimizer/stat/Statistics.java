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

package org.apache.doris.optimizer.stat;

import com.google.common.collect.Maps;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;

import java.util.Map;

public class Statistics {

    private long rowCount;
    private OptColumnRefSet statColumns;
    private Map<Integer, Long> columnsCardinalityMap;
    private RequiredLogicalProperty property;

    public Statistics(RequiredLogicalProperty property) {
        this.rowCount = 0;
        this.statColumns = new OptColumnRefSet();
        this.columnsCardinalityMap = Maps.newHashMap();
        this.property = property;
    }

    public void addColumnCardinality(int id, long cardinality) {
        this.columnsCardinalityMap.put(id, cardinality);
        this.statColumns.include(id);
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public Long getCardinality(int id) {
        return columnsCardinalityMap.get(id);
    }

    // TODO ch, return width according column type.
    public int width(OptColumnRefSet columns) {
        return columns.cardinality();
    }

    public OptColumnRefSet getStatColumns() {
        return statColumns;
    }

    public RequiredLogicalProperty getProperty() { return property; }
}
