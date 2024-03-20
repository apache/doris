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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.TableMetadata;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;
import java.util.Map;

public class HiveTableMetadata implements TableMetadata {
    private String dbName;
    private String tableName;
    private List<Column> columns;
    private List<FieldSchema> partitionKeys;
    private String fileFormat;
    private Map<String, String> properties;
    // private String viewSql;

    public HiveTableMetadata(String dbName,
                             String tblName,
                             List<Column> columns,
                             List<FieldSchema> partitionKeys,
                             Map<String, String> props,
                             String fileFormat) {
        this.dbName = dbName;
        this.tableName = tblName;
        this.columns = columns;
        this.partitionKeys = partitionKeys;
        this.fileFormat = fileFormat;
        this.properties = props;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<FieldSchema> getPartitionKeys() {
        return partitionKeys;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public static HiveTableMetadata of(String dbName,
                                       String tblName,
                                       List<Column> columns,
                                       List<FieldSchema> partitionKeys,
                                       Map<String, String> props,
                                       String fileFormat) {
        return new HiveTableMetadata(dbName, tblName, columns, partitionKeys, props, fileFormat);
    }
}
