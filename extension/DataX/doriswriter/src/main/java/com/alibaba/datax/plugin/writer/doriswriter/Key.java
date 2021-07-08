/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
  -->
 */
package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Key implements Serializable
{
    public static final String JDBC_URL = "jdbcUrl";
    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String BE_LOAD_URL = "beLoadUrl";
    public static final String COLUMN = "column";
    public static final String PRE_SQL = "preSql";
    public static final String POST_SQL = "postSql";
    public static final String LOAD_PROPS = "loadProps";
    public static final String MAX_BATCH_ROWS = "maxBatchRows";
    public static final String MAX_BATCH_BYTE_SIZE = "maxBatchByteSize";
    private final Configuration options;

    public Key(final Configuration options) {
        this.options = options;
    }

    public void doPretreatment() {
        this.validateRequired();
        this.validateStreamLoadUrl();
    }

    public String getJdbcUrl() {
        return this.options.getString(JDBC_URL);
    }

    public String getDatabase() {
        return this.options.getString(DATABASE);
    }

    public String getTable() {
        return this.options.getString(TABLE);
    }

    public String getUsername() {
        return this.options.getString(USERNAME);
    }

    public String getPassword() {
        return this.options.getString(PASSWORD);
    }

    public List<String> getBeLoadUrlList() {
        return this.options.getList(BE_LOAD_URL, String.class);
    }

    public List<String> getColumns() {
        return this.options.getList(COLUMN, String.class);
    }

    public List<String> getPreSqlList() {
        return this.options.getList(PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return this.options.getList(POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps() {
        return this.options.getMap(LOAD_PROPS);
    }

    public int getBatchRows() {
        final Integer rows = this.options.getInt(MAX_BATCH_ROWS);
        return (null == rows) ? 500000 : rows;
    }

    public long getBatchByteSize() {
        final Long size = this.options.getLong(MAX_BATCH_BYTE_SIZE);
        return (null == size) ? 94371840L : size;
    }


    private void validateStreamLoadUrl() {
        final List<String> urlList = this.getBeLoadUrlList();
        for (final String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "loadUrl的格式不正确，请输入 `be_ip:be_http_ip;be_ip:be_http_ip`。");
            }
        }
    }

    private void validateRequired() {
        final String[] requiredOptionKeys =  new String[] { USERNAME, PASSWORD, DATABASE, TABLE, COLUMN, BE_LOAD_URL };
        for (final String optionKey : requiredOptionKeys) {
            this.options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }


}