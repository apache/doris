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
import org.apache.doris.thrift.TMySQLTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;

public class MysqlTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    private static final String MYSQL_HOST = "host";
    private static final String MYSQL_PORT = "port";
    private static final String MYSQL_USER = "user";
    private static final String MYSQL_PASSWORD = "password";
    private static final String MYSQL_DATABASE = "database";
    private static final String MYSQL_TABLE = "table";

    private String host;
    private String port;
    private String userName;
    private String passwd;
    private String mysqlDatabaseName;
    private String mysqlTableName;

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
                    + "they are: host, port, user, password, database and table");
        }

        // Set up
        host = properties.get(MYSQL_HOST);
        if (Strings.isNullOrEmpty(host)) {
            throw new DdlException("Host of MySQL table is null. "
                    + "Please add properties('host'='xxx.xxx.xxx.xxx') when create table");
        }

        port = properties.get(MYSQL_PORT);
        if (Strings.isNullOrEmpty(port)) {
            // Maybe null pointer or number convert
            throw new DdlException("Port of MySQL table is null. "
                    + "Please add properties('port'='3306') when create table");
        } else {
            try {
                Integer.valueOf(port);
            } catch (Exception e) {
                throw new DdlException("Port of MySQL table must be a number."
                        + "Please add properties('port'='3306') when create table");

            }
        }

        userName = properties.get(MYSQL_USER);
        if (Strings.isNullOrEmpty(userName)) {
            throw new DdlException("User of MySQL table is null. "
                    + "Please add properties('user'='root') when create table");
        }

        passwd = properties.get(MYSQL_PASSWORD);
        if (passwd == null) {
            throw new DdlException("Password of MySQL table is null. "
                    + "Please add properties('password'='xxxx') when create table");
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

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getMysqlDatabaseName() {
        return mysqlDatabaseName;
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    public TTableDescriptor toThrift() {
        TMySQLTable tMySQLTable = 
                new TMySQLTable(host, port, userName, passwd, mysqlDatabaseName, mysqlTableName);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.MYSQL_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setMysqlTable(tMySQLTable);
        return tTableDescriptor;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        String charsetName = "UTF-8";

        try {
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
            // mysql db
            adler32.update(mysqlDatabaseName.getBytes(charsetName));
            // mysql table
            adler32.update(mysqlTableName.getBytes(charsetName));

        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, host);
        Text.writeString(out, port);
        Text.writeString(out, userName);
        Text.writeString(out, passwd);
        Text.writeString(out, mysqlDatabaseName);
        Text.writeString(out, mysqlTableName);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // Read MySQL meta
        host = Text.readString(in);
        port = Text.readString(in);
        userName = Text.readString(in);
        passwd = Text.readString(in);
        mysqlDatabaseName = Text.readString(in);
        mysqlTableName = Text.readString(in);
    }
}
