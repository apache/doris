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

import com.google.common.collect.Maps;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TOdbcTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;
import org.apache.doris.thrift.TOdbcTableType;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;

public class OdbcTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    private static final String ODBC_CATALOG_RESOURCE = "odbc_catalog_resource";
    private static final String ODBC_HOST = "host";
    private static final String ODBC_PORT = "port";
    private static final String ODBC_USER = "user";
    private static final String ODBC_PASSWORD = "password";
    private static final String ODBC_DATABASE = "database";
    private static final String ODBC_TABLE = "table";
    private static final String ODBC_DRIVER = "driver";
    private static final String ODBC_TYPE = "odbc_type";

    private static Map<String, TOdbcTableType> TABLE_TYPE_MAP;
    static {
        Map<String, TOdbcTableType> tempMap = new HashMap<>();
        tempMap.put("oracle", TOdbcTableType.ORACLE);
        // we will support mysql driver in the future after we solve the core problem of
        // driver and static library
        tempMap.put("mysql", TOdbcTableType.MYSQL);
        TABLE_TYPE_MAP = Collections.unmodifiableMap(tempMap);
    }

    private String odbcCatalogResourceName;
    private String host;
    private String port;
    private String userName;
    private String passwd;
    private String odbcDatabaseName;
    private String odbcTableName;
    private String driver;
    private String odbcTableTypeName;

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
                    + "they are: odbc_catalog_resource or [host, port, user, password, driver, odbc_type]" +
                    " and database and table");
        }

        if (properties.containsKey(ODBC_CATALOG_RESOURCE)) {
            odbcCatalogResourceName = properties.get(ODBC_CATALOG_RESOURCE);

            // 1. check whether resource exist
            Resource oriResource = Catalog.getCurrentCatalog().getResourceMgr().getResource(odbcCatalogResourceName);
            if (oriResource == null) {
                throw new DdlException("Resource does not exist. name: " + odbcCatalogResourceName);
            }

            // 2. check resource usage privilege
            if (!Catalog.getCurrentCatalog().getAuth().checkResourcePriv(ConnectContext.get(),
                    odbcCatalogResourceName,
                    PrivPredicate.USAGE)) {
                throw new DdlException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                        + "'@'" + ConnectContext.get().getRemoteIP()
                        + "' for resource '" + odbcCatalogResourceName + "'");
            }
        } else {
            // Set up
            host = properties.get(ODBC_HOST);
            if (Strings.isNullOrEmpty(host)) {
                throw new DdlException("Host of Odbc table is null. "
                        + "Please set proper resource or add properties('host'='xxx.xxx.xxx.xxx') when create table");
            }

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

            userName = properties.get(ODBC_USER);
            if (Strings.isNullOrEmpty(userName)) {
                throw new DdlException("User of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('user'='root') when create table");
            }

            passwd = properties.get(ODBC_PASSWORD);
            if (passwd == null) {
                throw new DdlException("Password of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('password'='xxxx') when create table");
            }

            driver = properties.get(ODBC_DRIVER);
            if (Strings.isNullOrEmpty(driver)) {
                throw new DdlException("Driver of Odbc table is null. "
                        + "Please set odbc_catalog_resource or add properties('diver'='xxxx') when create table");
            }

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
        }

        odbcDatabaseName = properties.get(ODBC_DATABASE);
        if (Strings.isNullOrEmpty(odbcDatabaseName)) {
            throw new DdlException("Database of Odbc table is null. "
                    + "Please add properties('database'='xxxx') when create table");
        }

        odbcTableName = properties.get(ODBC_TABLE);
        if (Strings.isNullOrEmpty(odbcTableName)) {
            throw new DdlException("Database of Odbc table is null. "
                    + "Please add properties('table'='xxxx') when create table");
        }
    }

    private String getPropertyFromResource(String propertyName) {
        OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource)
                (Catalog.getCurrentCatalog().getResourceMgr().getResource(odbcCatalogResourceName));
        if (odbcCatalogResource == null) {
            throw new RuntimeException("Resource does not exist. name: " + odbcCatalogResourceName);
        }

        String property = odbcCatalogResource.getProperties(propertyName);
        if (property == null) {
            throw new RuntimeException("The property:" + propertyName + " do not set in resource " + odbcCatalogResourceName);
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

    public String getOdbcTableTypeName() {
        if (odbcTableTypeName != null) {
            return odbcTableTypeName;
        }
        return getPropertyFromResource(ODBC_TYPE);
    }

    public TOdbcTableType getOdbcTableType() {
        return TABLE_TYPE_MAP.get(getOdbcTableTypeName());
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
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        String charsetName = "UTF-8";

        try {
            // resource name
            adler32.update(odbcCatalogResourceName.getBytes(charsetName));
            // name
            adler32.update(name.getBytes(charsetName));
            // type
            adler32.update(type.name().getBytes(charsetName));
            // host
            adler32.update(host.getBytes(charsetName));
            // port
            adler32.update(port.getBytes(charsetName));
            // username
            adler32.update(userName.getBytes(charsetName));
            // passwd
            adler32.update(passwd.getBytes(charsetName));
            // odbc db
            adler32.update(odbcDatabaseName.getBytes(charsetName));
            // odbc table
            adler32.update(odbcTableName.getBytes(charsetName));
            // odbc driver
            adler32.update(driver.getBytes(charsetName));
            // odbc type
            adler32.update(odbcTableTypeName.getBytes(charsetName));
        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Map<String, String> serializeMap = Maps.newHashMap();

        serializeMap.put(ODBC_CATALOG_RESOURCE, odbcCatalogResourceName);
        serializeMap.put(ODBC_HOST, host);
        serializeMap.put(ODBC_PORT, port);
        serializeMap.put(ODBC_USER, userName);
        serializeMap.put(ODBC_PASSWORD, passwd);
        serializeMap.put(ODBC_DATABASE, odbcDatabaseName);
        serializeMap.put(ODBC_TABLE, odbcTableName);
        serializeMap.put(ODBC_DRIVER, driver);
        serializeMap.put(ODBC_TYPE, odbcTableTypeName);

        int size = (int) serializeMap.values().stream().filter(v -> {
            return v != null;
        }).count();
        out.writeInt(size);

        for (Map.Entry<String, String> kv : serializeMap.entrySet()) {
            if (kv.getValue() != null) {
                Text.writeString(out, kv.getKey());
                Text.writeString(out, kv.getValue());
            }
        }
    }

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
    }

    public static String supportTableType() {
        String supportTable = "";
        for (String table : TABLE_TYPE_MAP.keySet()) {
            supportTable += table + " ";
        }
        return supportTable;
    }
}
