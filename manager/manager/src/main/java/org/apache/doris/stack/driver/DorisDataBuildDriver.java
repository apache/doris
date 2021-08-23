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

package org.apache.doris.stack.driver;

import org.apache.doris.stack.model.request.construct.FieldInfo;
import org.apache.doris.stack.model.request.construct.HdfsConnectReq;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.model.request.construct.TableCreateReq;
import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

@Component
@Slf4j
public class DorisDataBuildDriver {

    private static final String SPACE = " ";

    private static final String COMMA = ",";

    private static final String LEFT_BRACKET = "(";

    private static final String RIGHT_BRACKET = ")";

    private static final String ENTER = "\n";

    @Autowired
    public DorisDataBuildDriver() {
    }

    public String createDb(String dbName) throws Exception {
        if (StringUtils.isEmpty(dbName)) {
            log.error("Database name is empty");
            throw new RequestFieldNullException();
        }
        StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE DATABASE ");
        buffer.append(dbName);
        return buffer.toString();
    }

    public String createTable(TableCreateReq createInfo) throws Exception {
        if (StringUtils.isEmpty(createInfo.getName()) || createInfo.getFieldInfos() == null
                || createInfo.getFieldInfos().isEmpty()) {
            log.error("Table name or fields is empty");
            throw new RequestFieldNullException();
        }

        StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE TABLE ");
        buffer.append(createInfo.getName());
        buffer.append(LEFT_BRACKET);
        buffer.append(ENTER);

        // field information
        for (FieldInfo field : createInfo.getFieldInfos()) {
            if (StringUtils.isEmpty(field.getName())) {
                log.error("Table field name is null");
                throw new RequestFieldNullException();
            }
            buffer.append(field.getName());
            buffer.append(SPACE + field.transDatabaseType());
            if (!field.isBeNull() || field.isKey()) {
                buffer.append(SPACE + "NOT NULL");
            }

            // default value
            if (field.getDefaultValue() != null && !field.getDefaultValue().isEmpty()) {
                buffer.append(SPACE + "DEFAULT" + SPACE);
                buffer.append("\"");
                buffer.append(field.getDefaultValue());
                buffer.append("\"");
            }

            // Column comment
            if (field.getExtra() != null && !field.getExtra().isEmpty()) {
                buffer.append(SPACE + "COMMENT" + SPACE);
                buffer.append("\"");
                buffer.append(field.getExtra());
                buffer.append("\"");
            }

            buffer.append(COMMA);
        }
        buffer.deleteCharAt(buffer.length() - 1);
        buffer.append(RIGHT_BRACKET);
        buffer.append(ENTER);

        // Engine information
        if (createInfo.getEngine() != null) {
            buffer.append("ENGINE=" + createInfo.getEngine().name());
        } else {
            buffer.append("ENGINE=" + TableCreateReq.Engine.olap.name());
        }
        buffer.append(ENTER);

        // Key type and field information
        if (createInfo.getKeyColumnNames() != null && !createInfo.getKeyColumnNames().isEmpty()) {
            if (createInfo.getKeyType() != null) {
                buffer.append(createInfo.getKeyType().toSqlString());
            } else {
                buffer.append(TableCreateReq.KeyType.DUP_KEYS.toSqlString());
            }

            buffer.append(LEFT_BRACKET);
            for (String key : createInfo.getKeyColumnNames()) {
                buffer.append(key);
                buffer.append(COMMA);
            }
            buffer.deleteCharAt(buffer.length() - 1);
            buffer.append(RIGHT_BRACKET);
            buffer.append(ENTER);
        }

        // Table comment information
        if (!StringUtils.isEmpty(createInfo.getDescribe())) {
            buffer.append("COMMENT" + SPACE);
            buffer.append("\"");
            buffer.append(createInfo.getDescribe());
            buffer.append("\"");
            buffer.append(ENTER);
        }

        // Partition information
        if (createInfo.getPartitionColumnNames() != null && !createInfo.getPartitionColumnNames().isEmpty()) {
            // Partition key information
            buffer.append("PARTITION BY RANGE");
            buffer.append(LEFT_BRACKET);
            for (String partitionKey : createInfo.getPartitionColumnNames()) {
                buffer.append(partitionKey);
                buffer.append(COMMA);
            }
            buffer.deleteCharAt(buffer.length() - 1);
            buffer.append(RIGHT_BRACKET);
            buffer.append(ENTER);

            // Partition value information
            buffer.append(LEFT_BRACKET);
            buffer.append(ENTER);
            for (TableCreateReq.Partition partition : createInfo.getPartitionInfos()) {
                buffer.append("PARTITION");
                buffer.append(SPACE);
                buffer.append(partition.getName());
                buffer.append(SPACE);
                buffer.append("VALUES");
                buffer.append("[");
                for (List<String> values : partition.getValues()) {
                    buffer.append(LEFT_BRACKET);
                    for (String value : values) {
                        buffer.append("\"");
                        buffer.append(value);
                        buffer.append("\"");
                        buffer.append(COMMA);
                    }
                    buffer.deleteCharAt(buffer.length() - 1);
                    buffer.append(RIGHT_BRACKET);
                    buffer.append(COMMA);
                }
                buffer.deleteCharAt(buffer.length() - 1);
                buffer.append(RIGHT_BRACKET);
                buffer.append(COMMA);
                buffer.append(ENTER);
            }
            buffer.deleteCharAt(buffer.length() - 2);
            buffer.append(RIGHT_BRACKET);
            buffer.append(ENTER);
        }

        // BUCKETS information
        if (createInfo.getHashColumnNames() != null && !createInfo.getHashColumnNames().isEmpty()) {
            buffer.append("DISTRIBUTED BY HASH");
            buffer.append(LEFT_BRACKET);
            for (String haskKey : createInfo.getHashColumnNames()) {
                buffer.append(haskKey);
                buffer.append(COMMA);
            }
            buffer.deleteCharAt(buffer.length() - 1);
            buffer.append(RIGHT_BRACKET);
            buffer.append(" BUCKETS ");
            buffer.append(createInfo.getBuckets());
            buffer.append(ENTER);
        }

        // olap properties information
        if ((createInfo.getEngine() == null
                || createInfo.getEngine() == TableCreateReq.Engine.olap)
                && createInfo.getProperties() != null) {
            buffer.append("PROPERTIES");
            buffer.append(LEFT_BRACKET);
            buffer.append(createInfo.getProperties().tranDatabaseString());
            buffer.append(RIGHT_BRACKET);
        }
        buffer.append(";");

        return buffer.toString();
    }

    public String importHdfsFile(String db, String table, HdfsImportReq importReq) throws Exception {
        StringBuffer buffer = new StringBuffer();
        buffer.append("LOAD LABEL ");
        buffer.append(db);
        buffer.append(".");
        if (StringUtils.isEmpty(importReq.getName()) || importReq.getFileInfo() == null
                || importReq.getColumnNames() == null || importReq.getColumnNames().isEmpty()
                || importReq.getConnectInfo() == null) {
            log.error("Hdfs import task name,fileinfo,column names or connect info is null.");
            throw new RequestFieldNullException();
        }
        buffer.append(importReq.getName());
        buffer.append(ENTER);
        buffer.append(LEFT_BRACKET);
        buffer.append(ENTER);

        buffer.append("DATA INFILE");
        buffer.append(LEFT_BRACKET);
        buffer.append("\"");

        buffer.append(importReq.getFileInfo().getFileUrl());
        buffer.append("\"");
        buffer.append(RIGHT_BRACKET);
        buffer.append(ENTER);

        buffer.append("INTO TABLE ");
        buffer.append(table);
        buffer.append(ENTER);

        // Add separator
        String separator = importReq.getFileInfo().getColumnSeparator();
        if (separator != null && !separator.isEmpty()) {
            buffer.append("COLUMNS TERMINATED BY ");
            buffer.append("\"");
            buffer.append(separator);
            buffer.append("\"");
            buffer.append(ENTER);
        }

        // File format assignment
        if (importReq.getFileInfo().getFormat() != null && !importReq.getFileInfo().getFormat().isEmpty()) {
            String format = null;
            if (!importReq.getFileInfo().getFormat().equals(HdfsConnectReq.Format.CSV)) {
                format = importReq.getFileInfo().getFormat().toLowerCase();
            }
            if (format != null) {
                buffer.append("FORMAT AS ");
                buffer.append("\"");
                buffer.append(format);
                buffer.append("\"");
                buffer.append(ENTER);
            }
        }

        buffer.append(LEFT_BRACKET);
        StringBuffer columnNameBuffer = new StringBuffer();
        for (String columnName : importReq.getColumnNames()) {
            columnNameBuffer.append(columnName);
            columnNameBuffer.append(",");
        }
        columnNameBuffer.deleteCharAt(columnNameBuffer.length() - 1);
        buffer.append(columnNameBuffer.toString());
        buffer.append(RIGHT_BRACKET);
        buffer.append(ENTER);
        buffer.append(RIGHT_BRACKET);

        if (importReq.getConnectInfo().getBrokerName() != null) {
            buffer.append("WITH BROKER ");
            buffer.append("'");
            buffer.append(importReq.getConnectInfo().getBrokerName());
            buffer.append("'");
            buffer.append(ENTER);
            buffer.append(LEFT_BRACKET);
            for (String key : importReq.getConnectInfo().getBrokerProps().keySet()) {
                buffer.append(ENTER);
                buffer.append("\"");
                buffer.append(key);
                buffer.append("\"");
                buffer.append("=");
                buffer.append("\"");
                buffer.append(importReq.getConnectInfo().getBrokerProps().get(key));
                buffer.append("\"");
                buffer.append(COMMA);
            }
            buffer.deleteCharAt(buffer.length() - 1);
            buffer.append(ENTER);
            buffer.append(RIGHT_BRACKET);
            buffer.append(ENTER);
        }
        // TODO:PROPERTIES

        return buffer.toString();
    }
}
