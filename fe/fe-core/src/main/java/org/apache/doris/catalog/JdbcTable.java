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

import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcTable extends Table {
    private static final Logger LOG = LogManager.getLogger(JdbcTable.class);

    private static final String TABLE = "table";
    private static final String RESOURCE = "resource";
    private static final String TABLE_TYPE = "table_type";
    private static final String URL = "jdbc_url";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String DRIVER_CLASS = "driver_class";
    private static final String DRIVER_URL = "driver_url";
    private static final String CHECK_SUM = "checksum";
    private static Map<String, TOdbcTableType> TABLE_TYPE_MAP;
    private String resourceName;
    private String externalTableName;
    private String jdbcTypeName;

    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPasswd;
    private String driverClass;
    private String driverUrl;
    private String checkSum;

    static {
        Map<String, TOdbcTableType> tempMap = new HashMap<>();
        tempMap.put("mysql", TOdbcTableType.MYSQL);
        tempMap.put("postgresql", TOdbcTableType.POSTGRESQL);
        tempMap.put("sqlserver", TOdbcTableType.SQLSERVER);
        tempMap.put("oracle", TOdbcTableType.ORACLE);
        TABLE_TYPE_MAP = Collections.unmodifiableMap(tempMap);
    }

    public JdbcTable() {
        super(TableType.JDBC);
    }

    public JdbcTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.JDBC, schema);
        validate(properties);
    }

    public String getCheckSum() {
        return checkSum;
    }

    public String getExternalTableName() {
        return externalTableName;
    }

    public String getJdbcTypeName() {
        return jdbcTypeName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public String getJdbcPasswd() {
        return jdbcPasswd;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getDriverUrl() {
        return driverUrl;
    }

    @Override
    public TTableDescriptor toThrift() {
        TJdbcTable tJdbcTable = new TJdbcTable();
        tJdbcTable.setJdbcUrl(jdbcUrl);
        tJdbcTable.setJdbcUser(jdbcUser);
        tJdbcTable.setJdbcPassword(jdbcPasswd);
        tJdbcTable.setJdbcTableName(externalTableName);
        tJdbcTable.setJdbcDriverClass(driverClass);
        tJdbcTable.setJdbcDriverUrl(driverUrl);
        tJdbcTable.setJdbcResourceName(resourceName);
        tJdbcTable.setJdbcDriverChecksum(checkSum);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE, fullSchema.size(), 0,
                getName(), "");
        tTableDescriptor.setJdbcTable(tJdbcTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Map<String, String> serializeMap = Maps.newHashMap();
        serializeMap.put(TABLE, externalTableName);
        serializeMap.put(RESOURCE, resourceName);
        serializeMap.put(TABLE_TYPE, jdbcTypeName);
        serializeMap.put(URL, jdbcUrl);
        serializeMap.put(USER, jdbcUser);
        serializeMap.put(PASSWORD, jdbcPasswd);
        serializeMap.put(DRIVER_CLASS, driverClass);
        serializeMap.put(DRIVER_URL, driverUrl);
        serializeMap.put(CHECK_SUM, checkSum);

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

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }
        externalTableName = serializeMap.get(TABLE);
        resourceName = serializeMap.get(RESOURCE);
        jdbcTypeName = serializeMap.get(TABLE_TYPE);
        jdbcUrl = serializeMap.get(URL);
        jdbcUser = serializeMap.get(USER);
        jdbcPasswd = serializeMap.get(PASSWORD);
        driverClass = serializeMap.get(DRIVER_CLASS);
        driverUrl = serializeMap.get(DRIVER_URL);
        checkSum = serializeMap.get(CHECK_SUM);
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getJdbcTable() {
        return externalTableName;
    }

    public String getTableTypeName() {
        return jdbcTypeName;
    }

    public TOdbcTableType getJdbcTableType() {
        return TABLE_TYPE_MAP.get(getTableTypeName());
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type);
        sb.append(resourceName);
        sb.append(externalTableName);
        sb.append(jdbcUrl);
        sb.append(jdbcUser);
        sb.append(jdbcPasswd);
        sb.append(driverClass);
        sb.append(driverUrl);
        sb.append(checkSum);

        String md5 = DigestUtils.md5Hex(sb.toString());
        LOG.debug("get signature of odbc table {}: {}. signature string: {}", name, md5, sb.toString());
        return md5;
    }

    @Override
    public JdbcTable clone() {
        JdbcTable copied = new JdbcTable();
        if (!DeepCopy.copy(this, copied, JdbcTable.class, FeConstants.meta_version)) {
            LOG.warn("failed to copy jdbc table: " + getName());
            return null;
        }
        return copied;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of jdbc table, "
                    + "they are: host, port, user, password, database and table");
        }

        externalTableName = properties.get(TABLE);
        if (Strings.isNullOrEmpty(externalTableName)) {
            throw new DdlException("property " + TABLE + " must be set");
        }

        resourceName = properties.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + RESOURCE + " must be set");
        }

        jdbcTypeName = properties.get(TABLE_TYPE);
        if (Strings.isNullOrEmpty(jdbcTypeName)) {
            throw new DdlException("property " + TABLE_TYPE + " must be set");
        }
        if (!TABLE_TYPE_MAP.containsKey(jdbcTypeName.toLowerCase())) {
            throw new DdlException("Unknown jdbc table type: " + jdbcTypeName);
        }

        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("jdbc resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != ResourceType.JDBC) {
            throw new DdlException("resource [" + resourceName + "] is not jdbc resource");
        }

        JdbcResource jdbcResource = (JdbcResource) resource;
        jdbcUrl = jdbcResource.getProperty(URL);
        jdbcUser = jdbcResource.getProperty(USER);
        jdbcPasswd = jdbcResource.getProperty(PASSWORD);
        driverClass = jdbcResource.getProperty(DRIVER_CLASS);
        driverUrl = jdbcResource.getProperty(DRIVER_URL);
        checkSum = jdbcResource.getProperty(CHECK_SUM);
    }
}
