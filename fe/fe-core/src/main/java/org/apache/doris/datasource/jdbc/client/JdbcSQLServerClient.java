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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

public class JdbcSQLServerClient extends JdbcClient {

    protected JdbcSQLServerClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String originSqlserverType = fieldSchema.getDataTypeName().orElse("unknown");
        // For sqlserver IDENTITY type, such as 'INT IDENTITY'
        // originSqlserverType is "int identity", so we only get "int".
        String sqlserverType = originSqlserverType.split(" ")[0];
        switch (sqlserverType) {
            case "bit":
                return Type.BOOLEAN;
            case "tinyint":
            case "smallint":
                return Type.SMALLINT;
            case "int":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "real":
                return Type.FLOAT;
            case "float":
                return Type.DOUBLE;
            case "money":
                return ScalarType.createDecimalV3Type(19, 4);
            case "smallmoney":
                return ScalarType.createDecimalV3Type(10, 4);
            case "decimal":
            case "numeric": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return ScalarType.createDecimalV3Type(precision, scale);
            }
            case "date":
                return ScalarType.createDateV2Type();
            case "datetime":
            case "datetime2":
            case "smalldatetime": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "char":
            case "varchar":
            case "nchar":
            case "nvarchar":
            case "text":
            case "ntext":
            case "time":
            case "datetimeoffset":
            case "uniqueidentifier":
            case "timestamp":
                return ScalarType.createStringType();
            case "image":
            case "binary":
            case "varbinary":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
