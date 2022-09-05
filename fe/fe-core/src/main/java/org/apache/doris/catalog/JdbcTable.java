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
    // TODO: We may have to support various types of databases like ODBC
    // map now jdbc external table Doris support now
    private static Map<String, TOdbcTableType> TABLE_TYPE_MAP;
    private String resourceName;
    private String externalTableName;
    private String jdbcTypeName;

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

    @Override
    public TTableDescriptor toThrift() {
        TJdbcTable tJdbcTable = new TJdbcTable();
        JdbcResource jdbcResource = (JdbcResource) (Env.getCurrentEnv().getResourceMgr().getResource(resourceName));
        tJdbcTable.setJdbcUrl(jdbcResource.getProperty(JdbcResource.URL));
        tJdbcTable.setJdbcUser(jdbcResource.getProperty(JdbcResource.USER));
        tJdbcTable.setJdbcPassword(jdbcResource.getProperty(JdbcResource.PASSWORD));
        tJdbcTable.setJdbcTableName(externalTableName);
        tJdbcTable.setJdbcDriverClass(jdbcResource.getProperty(JdbcResource.DRIVER_CLASS));
        tJdbcTable.setJdbcDriverUrl(jdbcResource.getProperty(JdbcResource.DRIVER_URL));
        tJdbcTable.setJdbcResourceName(jdbcResource.getName());
        tJdbcTable.setJdbcDriverChecksum(jdbcResource.getProperty(JdbcResource.CHECK_SUM));

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.JDBC_TABLE, fullSchema.size(), 0,
                getName(), "");
        tTableDescriptor.setJdbcTable(tJdbcTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, externalTableName);
        Text.writeString(out, resourceName);
        Text.writeString(out, jdbcTypeName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        externalTableName = Text.readString(in);
        resourceName = Text.readString(in);
        jdbcTypeName = Text.readString(in);
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
        JdbcResource jdbcResource = (JdbcResource) (Env.getCurrentEnv().getResourceMgr().getResource(resourceName));
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type);
        sb.append(resourceName);
        sb.append(externalTableName);
        sb.append(jdbcTypeName);
        sb.append(jdbcResource.getProperty(JdbcResource.URL));
        sb.append(jdbcResource.getProperty(JdbcResource.USER));
        sb.append(jdbcResource.getProperty(JdbcResource.PASSWORD));
        sb.append(jdbcResource.getProperty(JdbcResource.DRIVER_CLASS));
        sb.append(jdbcResource.getProperty(JdbcResource.DRIVER_URL));

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

        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("jdbc resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != ResourceType.JDBC) {
            throw new DdlException("resource [" + resourceName + "] is not jdbc resource");
        }
    }
}
