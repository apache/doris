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

import org.apache.doris.stack.model.palo.HdfsFilePreviewReq;
import org.apache.doris.stack.model.request.construct.FieldInfo;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.model.request.construct.TableCreateReq;
import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
@Slf4j
public class DorisDataBuildDriverTest {

    @InjectMocks
    private DorisDataBuildDriver buildDriver;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createDbTest() {
        log.debug("Get create db sql test.");

        // Db name is empty
        try {
            buildDriver.createDb(null);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        try {
            buildDriver.createDb("");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        String dbName = "db";
        try {
            String result = buildDriver.createDb(dbName);
            String sql = "CREATE DATABASE db";
            Assert.assertEquals(result, sql);
        } catch (Exception e) {
            log.error("Get create db sql error.");
            e.printStackTrace();
        }
    }

    /**
     * Test CREATE TABLE statement function
     */
    @Test
    public void createTableTest() {
        log.debug("build create table sql test");
        TableCreateReq createReq = new TableCreateReq();
        String tableName = "table";
        // request body exception
        try {
            buildDriver.createTable(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        createReq.setName("");
        try {
            buildDriver.createTable(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        createReq.setName(tableName);
        try {
            buildDriver.createTable(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        List<FieldInfo> fieldInfos = Lists.newArrayList();
        createReq.setFieldInfos(fieldInfos);
        try {
            buildDriver.createTable(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // fields exception
        FieldInfo fieldInfo = new FieldInfo();
        fieldInfos = Lists.newArrayList(fieldInfo);
        createReq.setFieldInfos(fieldInfos);
        try {
            buildDriver.createTable(createReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // fields add
        FieldInfo fieldInfo1 = new FieldInfo();
        fieldInfo1.setName("field1");
        fieldInfo1.setDefaultValue("2013-05-06");
        fieldInfo1.setBeNull(true);
        fieldInfo1.setKey(true);
        fieldInfo1.setExtra("test field");
        fieldInfo1.setType(FieldInfo.FieldType.DATE);

        FieldInfo fieldInfo2 = new FieldInfo();
        fieldInfo2.setName("field2");
        fieldInfo2.setBeNull(false);
        fieldInfo2.setKey(false);
        fieldInfo2.setType(FieldInfo.FieldType.CHAR);

        FieldInfo fieldInfo3 = new FieldInfo();
        fieldInfo3.setName("field3");
        fieldInfo3.setBeNull(true);
        fieldInfo3.setKey(false);
        fieldInfo3.setType(FieldInfo.FieldType.VARCHAR);

        FieldInfo fieldInfo4 = new FieldInfo();
        fieldInfo4.setName("field4");
        fieldInfo4.setBeNull(false);
        fieldInfo4.setKey(true);
        fieldInfo4.setType(FieldInfo.FieldType.DECIMAL);

        fieldInfos = Lists.newArrayList(fieldInfo1, fieldInfo2, fieldInfo3, fieldInfo4);
        createReq.setFieldInfos(fieldInfos);
        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=olap\n"
                    + ";";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }

        // engine type
        createReq.setEngine(TableCreateReq.Engine.mysql);

        // table description
        createReq.setDescribe("for test table");

        // key fields type test
        // default key type, DUP_KEYS
        List<String> keyColumnNames = Lists.newArrayList("field1", "field4");
        createReq.setKeyColumnNames(keyColumnNames);
        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=mysql\n"
                    + "DUPLICATE KEY(field1,field4)\n"
                    + "COMMENT \"for test table\"\n"
                    + ";";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }
        // AGG_KEYS
        createReq.setKeyType(TableCreateReq.KeyType.AGG_KEYS);
        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=mysql\n"
                    + "AGGREGATE KEY(field1,field4)\n"
                    + "COMMENT \"for test table\"\n"
                    + ";";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }

        // UNIQUE_KEYS
        createReq.setKeyType(TableCreateReq.KeyType.UNIQUE_KEYS);
        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=mysql\n"
                    + "UNIQUE KEY(field1,field4)\n"
                    + "COMMENT \"for test table\"\n"
                    + ";";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }

        // PRIMARY_KEYS
        createReq.setKeyType(TableCreateReq.KeyType.PRIMARY_KEYS);
        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=mysql\n"
                    + "PRIMARY KEY(field1,field4)\n"
                    + "COMMENT \"for test table\"\n"
                    + ";";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }

        // other
        createReq.setKeyType(TableCreateReq.KeyType.DUP_KEYS);

        // Partition information
        List<String> partitionColumnNames = Lists.newArrayList("field1");

        TableCreateReq.Partition partition = new TableCreateReq.Partition();
        partition.setName("p1");
        List<String> value = Lists.newArrayList("2010-01-01", "2011-01-01", "2012-01-01");
        List<List<String>> values = new ArrayList<>();
        values.add(value);
        partition.setValues(values);
        List<TableCreateReq.Partition> partitionInfos = Lists.newArrayList(partition);

        createReq.setPartitionColumnNames(partitionColumnNames);
        createReq.setPartitionInfos(partitionInfos);

        // hash
        List<String> hashColumnNames = Lists.newArrayList("field4");
        createReq.setHashColumnNames(hashColumnNames);

        // Property information
        createReq.setEngine(TableCreateReq.Engine.olap);
        TableCreateReq.OlapProperties properties = new TableCreateReq.OlapProperties();
        properties.setReplicationNum(3);
        properties.setStorageCooldownTime("storage_cooldown_time");
        properties.setStorageMedium("storageMedium");
        createReq.setProperties(properties);

        try {
            String result = buildDriver.createTable(createReq);
            String sql = "CREATE TABLE table(\n"
                    + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                    + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                    + "ENGINE=olap\n"
                    + "DUPLICATE KEY(field1,field4)\n"
                    + "COMMENT \"for test table\"\n"
                    + "PARTITION BY RANGE(field1)\n"
                    + "(\n"
                    + "PARTITION p1 VALUES[(\"2010-01-01\",\"2011-01-01\",\"2012-01-01\"))\n"
                    + ")\n"
                    + "DISTRIBUTED BY HASH(field4) BUCKETS 0\n"
                    + "PROPERTIES(\"replication_num\"=\"3\");";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }
    }

    @Test
    public void importHdfsFileTest() {
        log.debug("Get import hdfs file test.");
        String db = "db";
        String table = "table";

        HdfsImportReq importReq = new HdfsImportReq();
        // Request body exception
        // name empty
        try {
            buildDriver.importHdfsFile(db, table, importReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // field empty
        importReq.setName("task1");
        try {
            buildDriver.importHdfsFile(db, table, importReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // column empty
        HdfsFilePreviewReq.FileInfo fileInfo = new HdfsFilePreviewReq.FileInfo();
        fileInfo.setColumnSeparator(",");
        fileInfo.setFileUrl("/test/file1");
        fileInfo.setFormat("CSV");
        importReq.setFileInfo(fileInfo);
        try {
            buildDriver.importHdfsFile(db, table, importReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        List<String> columnNames = new ArrayList<>();
        importReq.setColumnNames(columnNames);
        try {
            buildDriver.importHdfsFile(db, table, importReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // connection information empty
        columnNames.add("field1");
        importReq.setColumnNames(columnNames);
        try {
            buildDriver.importHdfsFile(db, table, importReq);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // request
        HdfsFilePreviewReq.ConnectInfo connectInfo = new HdfsFilePreviewReq.ConnectInfo();
        connectInfo.setBrokerName("broker1");
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put("test", "test");
        connectInfo.setBrokerProps(brokerProps);

        importReq.setConnectInfo(connectInfo);

        try {
            String result = buildDriver.importHdfsFile(db, table, importReq);
            String sql = "LOAD LABEL db.task1\n"
                    + "(\n"
                    + "DATA INFILE(\"/test/file1\")\n"
                    + "INTO TABLE table\n"
                    + "COLUMNS TERMINATED BY \",\"\n"
                    + "FORMAT AS \"csv\"\n"
                    + "(field1)\n"
                    + ")WITH BROKER 'broker1'\n"
                    + "(\n"
                    + "\"test\"=\"test\"\n"
                    + ")\n";
            Assert.assertEquals(sql, result);
        } catch (Exception e) {
            log.error("get create table sql error.");
            e.printStackTrace();
        }
    }
}
