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

package org.apache.doris.analysis;

import org.apache.doris.catalog.KeysType;

import java.util.List;

public class KeysDesc {
    private final KeysType type;
    private final List<String> keysColumnNames;
    private List<String> orderByKeysColumnNames;

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames, List<String> orderByKeysColumnNames) {
        this(type, keysColumnNames);
        this.orderByKeysColumnNames = orderByKeysColumnNames;
    }

    public KeysType getKeysType() {
        return type;
    }

    public int keysColumnSize() {
        return keysColumnNames.size();
    }

    public List<String> getOrderByKeysColumnNames() {
        return orderByKeysColumnNames;
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.toSql()).append("(");
        int i = 0;
        for (String columnName : keysColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")");
        if (orderByKeysColumnNames != null) {
            stringBuilder.append("\nORDER BY (");
            i = 0;
            for (String columnName : orderByKeysColumnNames) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append("`").append(columnName).append("`");
                i++;
            }
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }
}
