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

package org.apache.doris.datasource.jdbc.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.thrift.TOdbcTableType;

import lombok.Getter;


/**
 * JdbcSplit represents a single JDBC scan "split" (which is always one split
 * since JDBC queries cannot be partitioned).
 *
 * <p>It extends {@link FileSplit} so it can integrate with the
 * {@link org.apache.doris.datasource.FileQueryScanNode} framework.
 * The path/start/length fields are set to dummy values since JDBC
 * does not read files; the real parameters are in the JDBC-specific fields.
 */
@Getter
public class JdbcSplit extends FileSplit {

    private final String querySql;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String driverClass;
    private final String driverUrl;
    private final long catalogId;
    private final TOdbcTableType tableType;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;
    private final int connectionPoolMaxWaitTime;
    private final int connectionPoolMaxLifeTime;
    private final boolean connectionPoolKeepAlive;

    public JdbcSplit(String querySql, String jdbcUrl, String jdbcUser,
            String jdbcPassword, String driverClass, String driverUrl,
            long catalogId, TOdbcTableType tableType,
            int connectionPoolMinSize, int connectionPoolMaxSize,
            int connectionPoolMaxWaitTime, int connectionPoolMaxLifeTime,
            boolean connectionPoolKeepAlive) {
        // Use a dummy path — JDBC does not read actual files.
        // start=0, length=0, fileLength=0
        super(LocationPath.of("jdbc://virtual"),
                0, 0, 0, 0, null, null);
        this.querySql = querySql;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.driverClass = driverClass;
        this.driverUrl = driverUrl;
        this.catalogId = catalogId;
        this.tableType = tableType;
        this.connectionPoolMinSize = connectionPoolMinSize;
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        this.connectionPoolMaxWaitTime = connectionPoolMaxWaitTime;
        this.connectionPoolMaxLifeTime = connectionPoolMaxLifeTime;
        this.connectionPoolKeepAlive = connectionPoolKeepAlive;
    }

    @Override
    public Object getInfo() {
        return null;
    }

    @Override
    public String getPathString() {
        return "jdbc://virtual";
    }
}
