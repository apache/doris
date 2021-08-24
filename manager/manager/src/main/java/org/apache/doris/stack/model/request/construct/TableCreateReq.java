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

package org.apache.doris.stack.model.request.construct;

import lombok.Data;

import java.util.List;

@Data
public class TableCreateReq {

    private String name;

    private String describe;

    private List<FieldInfo> fieldInfos;

    private KeyType keyType;

    private List<String> keyColumnNames;

    private List<String> hashColumnNames;

    private int buckets;

    private Engine engine;

    private OlapProperties properties;

    private List<String> partitionColumnNames;

    private List<Partition> partitionInfos;

    /**
     * olap attribute
     */
    @Data
    public static class OlapProperties {

        private String storageMedium;

        private String storageCooldownTime;

        private int replicationNum;

        /**
         * transformation DatabaseString
         *
         * @return string
         */
        public String tranDatabaseString() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("\"");
            buffer.append("replication_num");
            buffer.append("\"");
            buffer.append("=");
            buffer.append("\"");
            buffer.append(replicationNum);
            buffer.append("\"");
            if (storageMedium != null && storageMedium.isEmpty()) {
                buffer.append(",");
                buffer.append("\"");
                buffer.append("storage_medium");
                buffer.append("\"");
                buffer.append("=");
                buffer.append("\"");
                buffer.append(storageMedium);
                buffer.append("\"");
            }

            if (storageCooldownTime != null && storageCooldownTime.isEmpty()) {
                buffer.append(",");
                buffer.append("\"");
                buffer.append("storage_cooldown_time");
                buffer.append("\"");
                buffer.append("=");
                buffer.append("\"");
                buffer.append(storageCooldownTime);
                buffer.append("\"");
            }
            return buffer.toString();
        }
    }

    @Data
    public static class Partition {

        private String name;

        private List<List<String>> values;
    }

    /**
     * Primary key type
     */
    public enum KeyType {
        PRIMARY_KEYS,
        DUP_KEYS,
        UNIQUE_KEYS,
        AGG_KEYS;

        /**
         * transformation sqlString
         *
         * @return string
         */
        public String toSqlString() {
            switch (this) {
                case AGG_KEYS:
                    return "AGGREGATE KEY";
                case UNIQUE_KEYS:
                    return "UNIQUE KEY";
                case DUP_KEYS:
                    return "DUPLICATE KEY";
                case PRIMARY_KEYS:
                    return "PRIMARY KEY";
                default:
                    return "Error";
            }
        }
    }

    /**
     * engine type
     */
    public enum Engine {
        olap,
        mysql,
        broker,
        hive;
    }
}
