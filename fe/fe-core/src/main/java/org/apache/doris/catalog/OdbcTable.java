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

/**
 * Persistence-only stub for legacy internal ODBC tables.
 *
 * <p>This class is retained solely for backward compatibility: clusters with
 * existing {@code ENGINE=ODBC} tables serialized in metadata can still
 * deserialize them through Gson. <b>No functional logic remains.</b>
 *
 * <p>ODBC table support has been fully removed. Use JDBC Catalog instead.
 *
 * @deprecated ODBC tables are no longer supported.
 */
@Deprecated
public class OdbcTable extends Table {

    @SerializedName("ocrn")
    private String odbcCatalogResourceName;
    @SerializedName("h")
    private String host;
    @SerializedName("p")
    private String port;
    @SerializedName("un")
    private String userName;
    @SerializedName("pwd")
    private String passwd;
    @SerializedName("odn")
    private String odbcDatabaseName;
    @SerializedName("otn")
    private String odbcTableName;
    @SerializedName("d")
    private String driver;
    @SerializedName("ottn")
    private String odbcTableTypeName;
    @SerializedName("c")
    private String charset;
    @SerializedName("ep")
    private String extraParam;

    // Required for Gson deserialization
    public OdbcTable() {
        super(TableType.ODBC);
    }
}
