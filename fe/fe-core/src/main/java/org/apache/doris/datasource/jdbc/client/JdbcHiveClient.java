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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcHiveClient extends JdbcClient {
    public JdbcHiveClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getDatabaseQuery() {
        return "SHOW DATABASES";
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String hiveType = fieldSchema.getDataTypeName();
        switch (hiveType) {
            case "BOOLEAN":
                return Type.INT;
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "INT":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "STRING":
                return Type.BOOLEAN;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
                return ScalarType.createDatetimeV2Type(6);
            default:
                break;
        }
        return Type.UNSUPPORTED;
    }

    // partition name foramt: col1=xxx/col2=yyy
    public List<String> getPartitionNameList(String dbName, String tblName) {
        Connection conn = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        List<String> partitionNames = Lists.newArrayList();
        try {
            stmt = conn.createStatement();
            String sql = String.format("SHOW PARTITIONS `%s`.`%s`", dbName, tblName);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String partName = rs.getString(1);
                partitionNames.add(partName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get partition name list from jdbc", e);
        } finally {
            close(rs, stmt, conn);
        }
        return partitionNames;
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        Connection conn = getConnection();
        try {
            return readPartition(conn, dbName, tblName, Joiner.on("/").join(partitionValues));
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get partition name list from jdbc", e);
        } finally {
            close(conn);
        }
    }

    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        List<Partition> partitions = Lists.newArrayListWithCapacity(partitionNames.size());
        Connection conn = getConnection();
        try {
            for (String partitionName : partitionNames) {
                Partition partition = readPartition(conn, dbName, tblName, partitionName);
                System.out.println(partition);
                partitions.add(partition);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get partition name list from jdbc", e);
        } finally {
            close(conn);
        }
        return partitions;
    }

    private Partition readPartition(Connection conn,
            String dbName, String tblName, String partitionValues) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            String sql = String.format("DESCRIBE FORMATTED `%s`.`%s` PARTITION(%s)",
                    dbName, tblName, partitionValues);
            rs = stmt.executeQuery(sql);
            boolean findPartitionInfo = false;
            String location = null;
            String inputFormat = null;
            while (rs.next()) {
                // 1. find # Detailed Partition Information
                String val = rs.getString(1);
                if (!findPartitionInfo && !val.startsWith("# Detailed Partition Information")) {
                    continue;
                }
                findPartitionInfo = true;
                if (val.startsWith("Location:")) {
                    location = rs.getString(2);
                } else if (val.startsWith("InputFormat:")) {
                    inputFormat = rs.getString(2);
                }
            }
            // List<FieldSchema> cols, String location, String inputFormat, String outputFormat,
            // boolean compressed, int numBuckets, SerDeInfo serdeInfo, List<String> bucketCols,
            // List<Order> sortCols, Map<String, String> parameters) {
            StorageDescriptor sd = new StorageDescriptor(Lists.newArrayList(), location, inputFormat, "",
                    false, 0, null, Lists.newArrayList(), Lists.newArrayList(), null);
            // List<String> values, String dbName, String tableName, int createTime, int lastAccessTime,
            // StorageDescriptor sd, Map<String, String> parameters) {
            return new Partition(Lists.newArrayList(partitionValues.split("/")), dbName, tblName, 0, 0, sd,
                    Maps.newHashMap());
        } finally {
            close(rs, stmt);
        }
    }

    public Table getTable(String dbName, String tblName) {
    }
}
