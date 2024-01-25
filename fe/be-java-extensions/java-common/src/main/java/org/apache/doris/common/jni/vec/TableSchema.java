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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * Used to parse the file structure of table-value-function type.
 * like avro file.
 */
public class TableSchema {
    private final List<SchemaColumn> schemaColumns;
    private final ObjectMapper objectMapper;

    public TableSchema(List<SchemaColumn> schemaColumns) {
        this.schemaColumns = schemaColumns;
        this.objectMapper = new ObjectMapper();
    }

    public String getTableSchema() throws IOException {
        try {
            return objectMapper.writeValueAsString(schemaColumns);
        } catch (JsonProcessingException e) {
            throw new IOException(e);
        }
    }

    public static class SchemaColumn {
        private String name;
        private int type;
        private List<SchemaColumn> childColumns;

        public SchemaColumn() {

        }

        public String getName() {
            return name;
        }

        public List<SchemaColumn> getChildColumns() {
            return childColumns;
        }

        public int getType() {
            return type;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setType(TPrimitiveType type) {
            this.type = type.getValue();
        }

        public void addChildColumns(List<SchemaColumn> childColumns) {
            this.childColumns = childColumns;
        }
    }

}
