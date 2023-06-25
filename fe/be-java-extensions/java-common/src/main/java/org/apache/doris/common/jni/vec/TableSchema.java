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

package org.apache.doris.common.jni.vec;

import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.annotations.VisibleForTesting;

/**
 * Used to parse the file structure of table-value-function type.
 * like avro file.
 */
public class TableSchema {

    private final String[] fields;
    private final TPrimitiveType[] schemaTypes;
    private final Integer fieldSize;
    private String tableSchema;

    public TableSchema(String[] fields, TPrimitiveType[] schemaTypes) {
        this.fields = fields;
        this.schemaTypes = schemaTypes;
        this.fieldSize = fields.length;
        fillSchema();
    }

    /**
     * Fill schema.
     * Format like: field1,field2,field3#type1,type2,type3
     */
    public void fillSchema() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldSize; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(fields[i]);
        }
        sb.append("#");
        for (int i = 0; i < fieldSize; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(schemaTypes[i].getValue());
        }
        tableSchema = sb.toString();
    }

    public String getTableSchema() {
        return tableSchema;
    }

    @VisibleForTesting
    public String[] getFields() {
        return fields;
    }

    @VisibleForTesting
    public TPrimitiveType[] getSchemaTypes() {
        return schemaTypes;
    }

}
