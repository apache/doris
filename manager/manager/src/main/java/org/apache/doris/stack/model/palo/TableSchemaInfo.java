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

package org.apache.doris.stack.model.palo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class TableSchemaInfo {

    private String engineType;

    private SchemaInfo schemaInfo;

    /**
     * SchemaInfo
     */
    @Data
    public static class SchemaInfo {

        // tbl(index name) -> schema
        private Map<String, TableSchema> schemaMap;
    }

    /**
     * TableSchema
     */
    @Data
    public static class TableSchema {

        private List<Schema> schema;

        private boolean isBaseIndex;

        private String keyType;

        /**
         * @return List<String>
         */
        public List<String> fieldList() {
            List<String> list = new ArrayList<>();
            for (Schema field : schema) {
                list.add(field.getField());
            }
            return list;
        }
    }

    /**
     * Schema
     */
    @Data
    public static class Schema {

        private String field;

        private String type;

        private String isNull;

        private String defaultVal;

        private String key;

        private String aggrType;

        private String comment;
    }
}
