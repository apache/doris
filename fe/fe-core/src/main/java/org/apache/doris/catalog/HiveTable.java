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
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * External hive table
 * Currently only support loading from hive table
 */
public class HiveTable extends Table {
    private static final String PROPERTY_MISSING_MSG = "Hive %s is null. Please add properties('%s'='xxx') when create table";
    private static final String PROPERTY_ERROR_MSG = "Hive table properties('%s'='%s') is illegal or not supported. Please check it";

    private String hiveDb;
    private String hiveTable;
    private Map<String, String> hiveProperties = Maps.newHashMap();

    public static final String HIVE_DB = "database";
    public static final String HIVE_TABLE = "table";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_HDFS_PREFIX = "dfs";
    public static final String S3_PROPERTIES_PREFIX = "AWS";
    public static final String S3_AK = "AWS_ACCESS_KEY";
    public static final String S3_SK = "AWS_SECRET_KEY";
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";

    public HiveTable() {
        super(TableType.HIVE);
    }

    public HiveTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HIVE, schema);
        validate(properties);
    }

    public String getHiveDbTable() {
        return String.format("%s.%s", hiveDb, hiveTable);
    }

    public String getHiveDb() {
        return hiveDb;
    }

    public String getHiveTable() {
        return hiveTable;
    }

    public Map<String, String> getHiveProperties() {
        return hiveProperties;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hive table, "
                + "they are: database, table and 'hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        hiveDb = copiedProps.get(HIVE_DB);
        if (Strings.isNullOrEmpty(hiveDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_DB, HIVE_DB));
        }
        copiedProps.remove(HIVE_DB);

        hiveTable = copiedProps.get(HIVE_TABLE);
        if (Strings.isNullOrEmpty(hiveTable)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_TABLE, HIVE_TABLE));
        }
        copiedProps.remove(HIVE_TABLE);

        // check hive properties
        // hive.metastore.uris 
        String hiveMetaStoreUris = copiedProps.get(HIVE_METASTORE_URIS);
        if (Strings.isNullOrEmpty(hiveMetaStoreUris)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_METASTORE_URIS, HIVE_METASTORE_URIS));
        }
        copiedProps.remove(HIVE_METASTORE_URIS);
        hiveProperties.put(HIVE_METASTORE_URIS, hiveMetaStoreUris);

        // check auth type
        String authType = copiedProps.get(BrokerUtil.HADOOP_SECURITY_AUTHENTICATION);
        if (Strings.isNullOrEmpty(authType)) {
            authType = AuthType.SIMPLE.getDesc();
        }
        if (!AuthType.isSupportedAuthType(authType)) {
            throw new DdlException(String.format(PROPERTY_ERROR_MSG, BrokerUtil.HADOOP_SECURITY_AUTHENTICATION, authType));
        }
        copiedProps.remove(BrokerUtil.HADOOP_SECURITY_AUTHENTICATION);
        hiveProperties.put(BrokerUtil.HADOOP_SECURITY_AUTHENTICATION, authType);

        if (AuthType.KERBEROS.getDesc().equals(authType)) {
            // check principal
            String principal = copiedProps.get(BrokerUtil.HADOOP_KERBEROS_PRINCIPAL);
            if (Strings.isNullOrEmpty(principal)) {
                throw new DdlException(String.format(PROPERTY_MISSING_MSG, BrokerUtil.HADOOP_KERBEROS_PRINCIPAL, BrokerUtil.HADOOP_KERBEROS_PRINCIPAL));
            }
            hiveProperties.put(BrokerUtil.HADOOP_KERBEROS_PRINCIPAL, principal);
            copiedProps.remove(BrokerUtil.HADOOP_KERBEROS_PRINCIPAL);
            // check keytab
            String keytabPath = copiedProps.get(BrokerUtil.HADOOP_KERBEROS_KEYTAB);
            if (Strings.isNullOrEmpty(keytabPath)) {
                throw new DdlException(String.format(PROPERTY_MISSING_MSG, BrokerUtil.HADOOP_KERBEROS_KEYTAB, BrokerUtil.HADOOP_KERBEROS_KEYTAB));
            }
            if (!Strings.isNullOrEmpty(keytabPath)) {
                hiveProperties.put(BrokerUtil.HADOOP_KERBEROS_KEYTAB, keytabPath);
                copiedProps.remove(BrokerUtil.HADOOP_KERBEROS_KEYTAB);
            }
        }
        String HDFSUserName = copiedProps.get(BrokerUtil.HADOOP_USER_NAME);
        if (!Strings.isNullOrEmpty(HDFSUserName)) {
            hiveProperties.put(BrokerUtil.HADOOP_USER_NAME, HDFSUserName);
            copiedProps.remove(BrokerUtil.HADOOP_USER_NAME);
        }
        if (!copiedProps.isEmpty()) {
            Iterator<Map.Entry<String, String>> iter = copiedProps.entrySet().iterator();
            while(iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                String key = entry.getKey();
                if (key.startsWith(HIVE_HDFS_PREFIX) || key.startsWith(S3_PROPERTIES_PREFIX)) {
                    hiveProperties.put(key, entry.getValue());
                    iter.remove();
                }
            }
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, hiveDb);
        Text.writeString(out, hiveTable);
        out.writeInt(hiveProperties.size());
        for (Map.Entry<String, String> entry : hiveProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        hiveDb = Text.readString(in);
        hiveTable = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String val = Text.readString(in);
            hiveProperties.put(key, val);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        THiveTable tHiveTable = new THiveTable(getHiveDb(), getHiveTable(), getHiveProperties());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE,
            fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }
}
