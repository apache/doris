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

package org.apache.doris.common.util;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.catalog.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnsUtil {
    private final Map<String, Integer> nameToUniqueId;

    public ColumnsUtil(List<Column> columns) {
        nameToUniqueId = new HashMap<>(columns.size());
        for (Column column : columns) {
            String name = column.getName();
            nameToUniqueId.put(name.toLowerCase(), column.getUniqueId());
        }
    }

    public Integer getColumnUniqueId(String columnName) {
        columnName = columnName.toLowerCase();
        Integer uniqueId = nameToUniqueId.get(columnName);
        if (uniqueId != null) {
            return uniqueId;
        }
        // column name will be changed on schema change
        columnName = SchemaChangeHandler.SHADOW_NAME_PREFIX + columnName;
        uniqueId = nameToUniqueId.get(columnName);
        if (uniqueId != null) {
            return uniqueId;
        }
        return Column.COLUMN_UNIQUE_ID_INIT_VALUE;
    }

    public List<Integer> getColumnUniqueId(List<String> columnNames) {
        List<Integer> result = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
            result.add(getColumnUniqueId(columnName));
        }
        return result;
    }
}
