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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveTableMetadata implements TableMetadata {
    private final String dbName;
    private final String tableName;
    private final Optional<String> location;
    private final List<Column> columns;
    private final List<String> partitionKeys;
    private final String fileFormat;
    private final String comment;
    private final Map<String, String> properties;
    private List<String> bucketCols;
    private int numBuckets;
    // private String viewSql;

    public HiveTableMetadata(String dbName,
                             String tblName,
                             Optional<String> location,
                             List<Column> columns,
                             List<String> partitionKeys,
                             Map<String, String> props,
                             String fileFormat,
                             String comment) {
        this(dbName, tblName, location, columns, partitionKeys, new ArrayList<>(), 0, props, fileFormat, comment);
    }

    public HiveTableMetadata(String dbName, String tableName,
                             Optional<String> location,
                             List<Column> columns,
                             List<String> partitionKeys,
                             List<String> bucketCols,
                             int numBuckets,
                             Map<String, String> props,
                             String fileFormat,
                             String comment) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columns;
        this.partitionKeys = partitionKeys;
        this.bucketCols = bucketCols;
        this.numBuckets = numBuckets;
        this.properties = props;
        this.fileFormat = fileFormat;
        this.location = location;
        this.comment = comment;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public Optional<String> getLocation() {
        return location;
    }

    public String getComment() {
        return comment == null ? "" : comment;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public List<String> getBucketCols() {
        return bucketCols;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public static HiveTableMetadata of(String dbName,
                                       String tblName,
                                       Optional<String> location,
                                       List<Column> columns,
                                       List<String> partitionKeys,
                                       Map<String, String> props,
                                       String fileFormat,
                                       String comment) {
        return new HiveTableMetadata(dbName, tblName, location, columns, partitionKeys, props, fileFormat, comment);
    }

    public static HiveTableMetadata of(String dbName,
                                       String tblName,
                                       Optional<String> location,
                                       List<Column> columns,
                                       List<String> partitionKeys,
                                       List<String> bucketCols,
                                       int numBuckets,
                                       Map<String, String> props,
                                       String fileFormat,
                                       String comment) {
        return new HiveTableMetadata(dbName, tblName, location, columns, partitionKeys,
                bucketCols, numBuckets, props, fileFormat, comment);
    }
}
