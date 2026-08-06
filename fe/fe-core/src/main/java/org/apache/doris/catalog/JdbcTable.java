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

package org.apache.doris.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Persistence-only stub for legacy internal JDBC tables.
 *
 * <p>This class is retained solely for backward compatibility: clusters with
 * existing {@code ENGINE=JDBC} tables serialized in metadata can still
 * deserialize them through Gson. <b>No functional logic remains.</b>
 *
 * <p>New JDBC table access should use {@code CREATE CATALOG ... "type"="jdbc"}
 * and {@link org.apache.doris.datasource.jdbc.JdbcExternalTable}.
 *
 * @deprecated Use JDBC Catalog instead.
 */
@Deprecated
public class JdbcTable extends Table {

    @SerializedName("rn")
    private String resourceName;
    @SerializedName("etn")
    private String externalTableName;
    @SerializedName("rdn")
    private String remoteDatabaseName;
    @SerializedName("rtn")
    private String remoteTableName;
    @SerializedName("rcn")
    private Map<String, String> remoteColumnNames;
    @SerializedName("jtn")
    private String jdbcTypeName;
    @SerializedName("jurl")
    private String jdbcUrl;
    @SerializedName("jusr")
    private String jdbcUser;
    @SerializedName("jpwd")
    private String jdbcPasswd;
    @SerializedName("dc")
    private String driverClass;
    @SerializedName("du")
    private String driverUrl;
    @SerializedName("cs")
    private String checkSum;
    @SerializedName("cid")
    private long catalogId = -1;
    @SerializedName("frs")
    private String functionRulesString;

    // Required for Gson deserialization
    public JdbcTable() {
        super(TableType.JDBC);
    }
}
