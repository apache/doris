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
import org.apache.doris.common.io.Text;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMySQLTable;
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
import java.util.List;
import java.util.Map;

public class MysqlTable extends Table {
    private static final Logger LOG = LogManager.getLogger(MysqlTable.class);

    private static final String ODBC_CATALOG_RESOURCE = "odbc_catalog_resource";
    private static final String MYSQL_HOST = "host";
    private static final String MYSQL_PORT = "port";
    private static final String MYSQL_USER = "user";
    private static final String MYSQL_PASSWORD = "password";
    private static final String MYSQL_DATABASE = "database";
    private static final String MYSQL_TABLE = "table";
    private static final String MYSQL_CHARSET = "charset";

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
    @SerializedName("mdn")
    private String mysqlDatabaseName;
    @SerializedName("mtn")
    private String mysqlTableName;
    @SerializedName("c")
    private String charset;

    public MysqlTable() {
        super(TableType.MYSQL);
    }

    public MysqlTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.MYSQL, schema);
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of mysql table, "
                    + "they are: odbc_catalog_resource or [host, port, user, password] and database and table");
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
        } else {
            // Set up
            host = properties.get(MYSQL_HOST);
            if (Strings.isNullOrEmpty(host)) {
                throw new DdlException("Host of MySQL table is null. "
                        + "Please set proper resource or add properties('host'='xxx.xxx.xxx.xxx') when create table");
            }

            port = properties.get(MYSQL_PORT);
            if (Strings.isNullOrEmpty(port)) {
                // Maybe null pointer or number convert
                throw new DdlException("Port of MySQL table is null. "
                        + "Please set proper resource or add properties('port'='3306') when create table");
            } else {
                try {
                    Integer.valueOf(port);
                } catch (Exception e) {
                    throw new DdlException("Port of MySQL table must be a number."
                            + "Please set proper resource or add properties('port'='3306') when create table");

                }
            }

            userName = properties.get(MYSQL_USER);
            if (Strings.isNullOrEmpty(userName)) {
                throw new DdlException("User of MySQL table is null. "
                        + "Please set proper resource or add properties('user'='root') when create table");
            }

            passwd = properties.get(MYSQL_PASSWORD);
            if (passwd == null) {
                throw new DdlException("Password of MySQL table is null. "
                        + "Please set proper resource or add properties('password'='xxxx') when create table");
            }

            charset = properties.get(MYSQL_CHARSET);
            if (charset == null) {
                charset = "utf8";
            }
            if (!charset.equalsIgnoreCase("utf8") && !charset.equalsIgnoreCase("utf8mb4")) {
                throw new DdlException("Unknown character set of MySQL table. "
                        + "Please set charset 'utf8' or 'utf8mb4', other charsets not be unsupported now.");
            }
        }

        mysqlDatabaseName = properties.get(MYSQL_DATABASE);
        if (Strings.isNullOrEmpty(mysqlDatabaseName)) {
            throw new DdlException("Database of MySQL table is null. "
                    + "Please add properties('database'='xxxx') when create table");
        }

        mysqlTableName = properties.get(MYSQL_TABLE);
        if (Strings.isNullOrEmpty(mysqlTableName)) {
            throw new DdlException("Database of MySQL table is null. "
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

    public String getOdbcCatalogResourceName() {
        return odbcCatalogResourceName;
    }

    public String getHost() {
        if (host != null) {
            return host;
        }
        return getPropertyFromResource(MYSQL_HOST);
    }

    public String getPort() {
        if (port != null) {
            return port;
        }
        return getPropertyFromResource(MYSQL_PORT);
    }

    public String getUserName() {
        if (userName != null) {
            return userName;
        }
        return getPropertyFromResource(MYSQL_USER);
    }

    public String getPasswd() {
        if (passwd != null) {
            return passwd;
        }
        return getPropertyFromResource(MYSQL_PASSWORD);
    }

    public String getMysqlDatabaseName() {
        return mysqlDatabaseName;
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    public String getCharset() {
        if (charset != null) {
            return charset;
        }
        return "utf8";
    }

    public TTableDescriptor toThrift() {
        TMySQLTable tMySQLTable = new TMySQLTable(getHost(), getPort(), getUserName(), getPasswd(),
                mysqlDatabaseName, mysqlTableName, getCharset());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.MYSQL_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setMysqlTable(tMySQLTable);
        return tTableDescriptor;
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type.name());
        sb.append(getHost());
        sb.append(getPort());
        sb.append(getUserName());
        sb.append(getPasswd());
        sb.append(mysqlDatabaseName);
        sb.append(mysqlTableName);
        sb.append(getCharset());
        String md5 = DigestUtils.md5Hex(sb.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of mysql table {}: {}. signature string: {}", name, md5, sb.toString());
        }
        return md5;
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // Read MySQL meta
        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }

        odbcCatalogResourceName = serializeMap.get(ODBC_CATALOG_RESOURCE);
        host = serializeMap.get(MYSQL_HOST);
        port = serializeMap.get(MYSQL_PORT);
        userName = serializeMap.get(MYSQL_USER);
        passwd = serializeMap.get(MYSQL_PASSWORD);
        mysqlDatabaseName = serializeMap.get(MYSQL_DATABASE);
        mysqlTableName = serializeMap.get(MYSQL_TABLE);
        charset = serializeMap.get(MYSQL_CHARSET);
    }
}
