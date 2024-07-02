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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TOdbcTable;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OdbcTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    public static final String ODBC_CATALOG_RESOURCE = "odbc_catalog_resource";
    public static final String ODBC_HOST = "host";
    public static final String ODBC_PORT = "port";
    public static final String ODBC_USER = "user";
    public static final String ODBC_PASSWORD = "password";
    public static final String ODBC_DATABASE = "database";
    public static final String ODBC_TABLE = "table";
    public static final String ODBC_DRIVER = "driver";
    public static final String ODBC_TYPE = "odbc_type";
    public static final String ODBC_CHARSET = "charset";
    public static final String ODBC_EXTRA_PARAM = "extra_param";


    // map now odbc external table Doris support now
    private static Map<String, TOdbcTableType> TABLE_TYPE_MAP;

    static {
        Map<String, TOdbcTableType> tempMap = new HashMap<>();
        tempMap.put("oracle", TOdbcTableType.ORACLE);
        tempMap.put("mysql", TOdbcTableType.MYSQL);
        tempMap.put("postgresql", TOdbcTableType.POSTGRESQL);
        tempMap.put("sqlserver", TOdbcTableType.SQLSERVER);
        TABLE_TYPE_MAP = Collections.unmodifiableMap(tempMap);
    }

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
    private Map<String, String> resourceProperties;

    public OdbcTable() {
        super(TableType.ODBC);
    }

    public OdbcTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.ODBC, schema);
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of odbc table, "
                    + "they are: odbc_catalog_resource or [host, port, user, password, driver, odbc_type]"
                    + " and database and table");
        }
        if (properties.containsKey(ODBC_CATALOG_RESOURCE)) {
            odbcCatalogResourceName = properties.get(ODBC_CATALOG_RESOURCE);

            // 1. check whether resource exist
            Resource oriResource = Env.getCurrentEnv().getResourceMgr().getResource(odbcCatalogResourceName);
            if (oriResource == null) {
                throw new DdlException("Resource does not exist. name: " + odbcCatalogResourceName);
            }

            // 2. check resource usage privilege
            if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(),
                    odbcCatalogResourceName,
                    PrivPredicate.USAGE)) {
                throw new DdlException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                        + "'@'" + ConnectContext.get().getRemoteIP()
                        + "' for resource '" + odbcCatalogResourceName + "'");
            }
            resourceProperties = new HashMap<>(oriResource.getCopiedProperties());
            resourceProperties.remove(ODBC_HOST);
            resourceProperties.remove(ODBC_PORT);
            resourceProperties.remove(ODBC_USER);
            resourceProperties.remove(ODBC_PASSWORD);
            resourceProperties.remove(ODBC_DRIVER);
            resourceProperties.remove(ODBC_CHARSET);
            resourceProperties.remove(ODBC_TYPE);
            resourceProperties.remove("type");
            resourceProperties.remove(ODBC_DATABASE);
        } else {
            Map<String, String> copiedProperties = new HashMap<>();
            copiedProperties.putAll(properties);
            // Set up
            host = properties.get(ODBC_HOST);
            if (Strings.isNullOrEmpty(host)) {
                throw new DdlException("Host of Odbc table is null. "
                        + "Please set proper resource or add properties('host'='xxx.xxx.xxx.xxx') when create table");
            }
            copiedProperties.remove(ODBC_HOST);

            port = properties.get(ODBC_PORT);
            if (Strings.isNullOrEmpty(port)) {
                // Maybe null pointer or number convert
                throw new DdlException("Port of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('port'='3306') when create table");
            } else {
                try {
                    Integer.valueOf(port);
                } catch (Exception e) {
                    throw new DdlException("Port of Odbc table must be a number."
                            + "Please set odbc_catalog_resource or add properties('port'='3306') when create table");

                }
            }
            copiedProperties.remove(ODBC_PORT);

            userName = properties.get(ODBC_USER);
            if (Strings.isNullOrEmpty(userName)) {
                throw new DdlException("User of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('user'='root') when create table");
            }
            copiedProperties.remove(ODBC_USER);

            passwd = properties.get(ODBC_PASSWORD);
            if (passwd == null) {
                throw new DdlException("Password of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('password'='xxxx') when create table");
            }
            copiedProperties.remove(ODBC_PASSWORD);

            driver = properties.get(ODBC_DRIVER);
            if (Strings.isNullOrEmpty(driver)) {
                throw new DdlException("Driver of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('diver'='xxxx') when create table");
            }
            copiedProperties.remove(ODBC_DRIVER);


            charset = properties.get(ODBC_CHARSET);
            copiedProperties.remove(ODBC_CHARSET);

            String tableType = properties.get(ODBC_TYPE);
            if (Strings.isNullOrEmpty(tableType)) {
                throw new DdlException("Type of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('odbc_type'='xxxx') when create table");
            } else {
                odbcTableTypeName = tableType.toLowerCase();
                if (!TABLE_TYPE_MAP.containsKey(odbcTableTypeName)) {
                    throw new DdlException("Invalid Odbc table type:" + tableType
                            + " Now Odbc table type only support:" + supportTableType());
                }
            }
            copiedProperties.remove(ODBC_TYPE);
            copiedProperties.remove(ODBC_DATABASE);
            copiedProperties.remove(ODBC_TABLE);
            extraParam = getExtraParameter(copiedProperties);
        }

        odbcDatabaseName = properties.get(ODBC_DATABASE);
        if (Strings.isNullOrEmpty(odbcDatabaseName)) {
            throw new DdlException("Database of Odbc table is null. "
                    + "Please add properties('database'='xxxx') when create table");
        }

        odbcTableName = properties.get(ODBC_TABLE);
        if (Strings.isNullOrEmpty(odbcTableName)) {
            throw new DdlException("Table of Odbc table is null. "
                    + "Please add properties('table'='xxxx') when create table");
        }
    }

    private String getPropertyFromResource(String propertyName) {
        OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource)
                (Env.getCurrentEnv().getResourceMgr().getResource(odbcCatalogResourceName));
        if (odbcCatalogResource == null) {
            throw new RuntimeException("Resource does not exist. name: " + odbcCatalogResourceName);
        }

        String property = odbcCatalogResource.getProperty(propertyName);
        if (property == null) {
            throw new RuntimeException("The property:" + propertyName
                    + " do not set in resource " + odbcCatalogResourceName);
        }
        return property;
    }

    public String getExtraParameter(Map<String, String> extraMap) {
        if (extraMap == null || extraMap.isEmpty()) {
            return "";
        }
        return ";" + extraMap.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    public String getExtraParam() {
        if (extraParam != null) {
            return extraParam;
        }
        return getExtraParameter(resourceProperties);
    }

    public String getOdbcCatalogResourceName() {
        return odbcCatalogResourceName;
    }

    public String getHost() {
        if (host != null) {
            return host;
        }
        return getPropertyFromResource(ODBC_HOST);
    }

    public String getPort() {
        if (port != null) {
            return port;
        }
        return getPropertyFromResource(ODBC_PORT);
    }

    public String getUserName() {
        if (userName != null) {
            return userName;
        }
        return getPropertyFromResource(ODBC_USER);
    }

    public String getPasswd() {
        if (passwd != null) {
            return passwd;
        }
        return getPropertyFromResource(ODBC_PASSWORD);
    }

    public String getOdbcDatabaseName() {
        return odbcDatabaseName;
    }

    public String getOdbcTableName() {
        return odbcTableName;
    }

    public String getOdbcDriver() {
        if (driver != null) {
            return driver;
        }
        return getPropertyFromResource(ODBC_DRIVER);
    }

    public String getCharset() {
        if (charset != null) {
            return charset;
        }
        String resourceCharset = "utf8";
        try {
            resourceCharset = getPropertyFromResource(ODBC_CHARSET);
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        return resourceCharset;
    }

    public String getOdbcTableTypeName() {
        if (odbcTableTypeName != null) {
            return odbcTableTypeName;
        }
        return getPropertyFromResource(ODBC_TYPE);
    }

    public String getConnectString() {
        String connectString = "";
        // different database have different connection string
        switch (getOdbcTableType()) {
            case ORACLE:
                connectString = String.format("Driver=%s;Dbq=//%s:%s/%s;DataBase=%s;Uid=%s;Pwd=%s;charset=%s",
                        getOdbcDriver(),
                        getHost(),
                        getPort(),
                        getOdbcDatabaseName(),
                        getOdbcDatabaseName(),
                        getUserName(),
                        getPasswd(),
                        getCharset());
                break;
            case POSTGRESQL:
                connectString = String.format("Driver=%s;Server=%s;Port=%s;DataBase=%s;"
                                + "Uid=%s;Pwd=%s;charset=%s;UseDeclareFetch=1;Fetch=4096",
                        getOdbcDriver(),
                        getHost(),
                        getPort(),
                        getOdbcDatabaseName(),
                        getUserName(),
                        getPasswd(),
                        getCharset());
                break;
            case MYSQL:
                connectString = String.format("Driver=%s;Server=%s;Port=%s;DataBase=%s;"
                                + "Uid=%s;Pwd=%s;charset=%s;forward_cursor=1;no_cache=1",
                        getOdbcDriver(),
                        getHost(),
                        getPort(),
                        getOdbcDatabaseName(),
                        getUserName(),
                        getPasswd(),
                        getCharset());
                break;
            case SQLSERVER:
                connectString = String.format("Driver=%s;Server=%s,%s;DataBase=%s;Uid=%s;Pwd=%s",
                        getOdbcDriver(),
                        getHost(),
                        getPort(),
                        getOdbcDatabaseName(),
                        getUserName(),
                        getPasswd());
                break;
            default:
        }
        return connectString + getExtraParam();
    }

    public TOdbcTableType getOdbcTableType() {
        return TABLE_TYPE_MAP.get(getOdbcTableTypeName());
    }

    @Override
    public OdbcTable clone() {
        OdbcTable copied = DeepCopy.copy(this, OdbcTable.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to copy odbc table: " + getName());
            return null;
        }
        return copied;
    }

    public void resetIdsForRestore(Env env) {
        id = env.getNextId();
    }

    public TTableDescriptor toThrift() {
        TOdbcTable tOdbcTable = new TOdbcTable();

        tOdbcTable.setHost(getHost());
        tOdbcTable.setPort(getPort());
        tOdbcTable.setUser(getUserName());
        tOdbcTable.setPasswd(getPasswd());
        tOdbcTable.setDb(getOdbcDatabaseName());
        tOdbcTable.setTable(getOdbcTableName());
        tOdbcTable.setDriver(getOdbcDriver());
        tOdbcTable.setType(getOdbcTableType());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ODBC_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setOdbcTable(tOdbcTable);
        return tTableDescriptor;
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type);
        if (odbcCatalogResourceName != null) {
            sb.append(odbcCatalogResourceName);
            sb.append(odbcDatabaseName);
            sb.append(odbcTableName);
        } else {
            sb.append(host);
            sb.append(port);
            sb.append(userName);
            sb.append(passwd);
            sb.append(driver);
            sb.append(odbcTableTypeName);
            sb.append(charset);
            sb.append(extraParam);
        }
        String md5 = DigestUtils.md5Hex(sb.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of odbc table {}: {}. signature string: {}", name, md5, sb.toString());
        }
        return md5;
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // Read Odbc meta
        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }

        odbcCatalogResourceName = serializeMap.get(ODBC_CATALOG_RESOURCE);
        host = serializeMap.get(ODBC_HOST);
        port = serializeMap.get(ODBC_PORT);
        userName = serializeMap.get(ODBC_USER);
        passwd = serializeMap.get(ODBC_PASSWORD);
        odbcDatabaseName = serializeMap.get(ODBC_DATABASE);
        odbcTableName = serializeMap.get(ODBC_TABLE);
        driver = serializeMap.get(ODBC_DRIVER);
        odbcTableTypeName = serializeMap.get(ODBC_TYPE);
        charset = serializeMap.get(ODBC_CHARSET);
        extraParam = serializeMap.get(ODBC_EXTRA_PARAM);
    }

    public static String supportTableType() {
        String supportTable = "";
        for (String table : TABLE_TYPE_MAP.keySet()) {
            supportTable += table + " ";
        }
        return supportTable;
    }
}
