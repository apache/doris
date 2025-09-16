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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class EncryptSQLTest extends ParserTestBase {

    NereidsParser parser = new NereidsParser();
    ConnectContext ctx = ConnectContext.get();
    WorkloadRuntimeStatusMgr mgr = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr();
    List<AuditEvent> auditEvents = Deencapsulation.getField(mgr, "queryAuditEventList");

    @Test
    public void testEncryption() {
        ctx.setDatabase("test");

        String sql = "EXPORT TABLE export_table TO \"s3://abc/aaa\" "
                + "PROPERTIES("
                + " \"format\" = \"csv\","
                + " \"max_file_size\" = \"2048MB\""
                + ")"
                + "WITH s3 ("
                + " \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\","
                + " \"s3.region\" = \"ap-beijing\","
                + " \"s3.secret_key\" = \"abc\","
                + " \"s3.access_key\" = \"abc\""
                + ")";

        String res = "EXPORT TABLE export_table TO \"s3://abc/aaa\" "
                + "PROPERTIES("
                + " \"format\" = \"csv\","
                + " \"max_file_size\" = \"2048MB\""
                + ")"
                + "WITH s3 ("
                + " \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\","
                + " \"s3.region\" = \"ap-beijing\","
                + " \"s3.secret_key\" = \"*XXX\","
                + " \"s3.access_key\" = \"abc\""
                + ")";
        parseAndCheck(sql, res);

        sql = "SELECT * FROM tbl "
                + " INTO OUTFILE \"s3://abc/aaa\""
                + " FORMAT AS ORC"
                + " PROPERTIES ("
                + " \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\","
                + " \"s3.region\" = \"ap-beijing\","
                + " \"s3.secret_key\" = \"abc\","
                + " \"s3.access_key\" = \"abc\""
                + ")";

        res = "SELECT * FROM tbl "
                + " INTO OUTFILE \"s3://abc/aaa\""
                + " FORMAT AS ORC"
                + " PROPERTIES ("
                + " \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\","
                + " \"s3.region\" = \"ap-beijing\","
                + " \"s3.secret_key\" = \"*XXX\","
                + " \"s3.access_key\" = \"abc\""
                + ")";
        parseAndCheck(sql, res);

        sql = "LOAD LABEL test_load_s3_orc_encrypt("
                + " DATA INFILE(\"s3://abc/aaa\")"
                + " INTO TABLE tbl"
                + " FORMAT AS \"ORC\""
                + ")"
                + "WITH S3("
                + " \"provider\" = \"S3\","
                + " \"AWS_ENDPOINT\" = \"xxx\","
                + " \"AWS_ACCESS_KEY\" = \"abc\","
                + " \"AWS_SECRET_KEY\" = \"abc\","
                + " \"AWS_REGION\" = \"ap-beijing\""
                + ")";

        res = "LOAD LABEL test_load_s3_orc_encrypt("
                + " DATA INFILE(\"s3://abc/aaa\")"
                + " INTO TABLE tbl"
                + " FORMAT AS \"ORC\""
                + ")"
                + "WITH S3("
                + " \"provider\" = \"S3\""
                + ")";
        parseAndCheck(sql, res);

        sql = "CREATE CATALOG ctl "
                + "PROPERTIES("
                + " \"type\" = \"iceberg\","
                + " \"iceberg.catalog.type\" = \"hadoop\","
                + " \"warehouse\" = \"s3://bucket/dir/key\","
                + " \"s3.endpoint\" = \"s3.us-east-1.amazonaws.com\","
                + " \"s3.access_key\" = \"abc\","
                + " \"s3.secret_key\" = \"abc\""
                + ")";

        res = "CREATE CATALOG ctl "
                + "PROPERTIES("
                + " \"type\" = \"iceberg\","
                + " \"iceberg.catalog.type\" = \"hadoop\","
                + " \"warehouse\" = \"s3://bucket/dir/key\","
                + " \"s3.endpoint\" = \"s3.us-east-1.amazonaws.com\","
                + " \"s3.access_key\" = \"abc\","
                + " \"s3.secret_key\" = \"*XXX\""
                + ")";
        parseAndCheck(sql, res);

        sql = "CREATE TABLE mysql_tbl("
                + " k1 DATE,"
                + " k2 INT,"
                + " k3 SMALLINT,"
                + " k4 VARCHAR(2048),"
                + " k5 DATETIME"
                + ") "
                + "ENGINE=mysql "
                + "PROPERTIES("
                + " \"host\" = \"127.0.0.1\","
                + " \"port\" = \"8234\","
                + " \"user\" = \"abc\","
                + " \"password\" = \"123\","
                + " \"database\" = \"mysql_db\","
                + " \"table\" = \"mysql_table\""
                + ")";

        res = "CREATE TABLE mysql_tbl("
                + " k1 DATE,"
                + " k2 INT,"
                + " k3 SMALLINT,"
                + " k4 VARCHAR(2048),"
                + " k5 DATETIME"
                + ") "
                + "ENGINE=mysql "
                + "PROPERTIES("
                + " \"host\" = \"127.0.0.1\","
                + " \"port\" = \"8234\","
                + " \"user\" = \"abc\","
                + " \"password\" = \"*XXX\","
                + " \"database\" = \"mysql_db\","
                + " \"table\" = \"mysql_table\""
                + ")";
        parseAndCheck(sql, res);

        sql = "CREATE EXTERNAL TABLE broker_tbl("
                + " k1 tinyint,"
                + " k2 smallint,"
                + " k3 int,"
                + " k4 bigint) "
                + "ENGINE=broker "
                + "PROPERTIES("
                + " \"broker_name\" = \"hdfs\","
                + " \"path\" = \"hdfs://abc/qe/a.txt\""
                + ") "
                + "BROKER PROPERTIES("
                + " \"username\" = \"root\","
                + " \"password\" = \"123\""
                + ")";

        res = "CREATE EXTERNAL TABLE broker_tbl("
            + " k1 tinyint,"
            + " k2 smallint,"
            + " k3 int,"
            + " k4 bigint) "
            + "ENGINE=broker "
            + "PROPERTIES("
            + " \"broker_name\" = \"hdfs\","
            + " \"path\" = \"hdfs://abc/qe/a.txt\""
            + ") "
            + "BROKER PROPERTIES("
            + " \"username\" = \"root\","
            + " \"password\" = \"*XXX\""
            + ")";
        parseAndCheck(sql, res);

        sql = "INSERT INTO test_s3load "
            + "SELECT * FROM s3_tbl("
            + " \"uri\" = \"s3://your_bucket_name/s3load_example.csv\","
            + " \"format\" = \"csv\","
            + " \"provider\" = \"OSS\","
            + " \"s3.endpoint\" = \"oss-cn-hangzhou.aliyuncs.com\","
            + " \"s3.region\" = \"oss-cn-hangzhou\","
            + " \"s3.access_key\" = \"abc\","
            + " \"s3.secret_key\" = \"abc\","
            + " \"column_separator\" = \",\","
            + " \"csv_schema\" = \"user_id:int;name:string;age:int\""
            + ")";

        res = "INSERT INTO test_s3load "
            + "SELECT * FROM s3_tbl("
            + " \"uri\" = \"s3://your_bucket_name/s3load_example.csv\","
            + " \"format\" = \"csv\","
            + " \"provider\" = \"OSS\","
            + " \"s3.endpoint\" = \"oss-cn-hangzhou.aliyuncs.com\","
            + " \"s3.region\" = \"oss-cn-hangzhou\","
            + " \"s3.access_key\" = \"abc\","
            + " \"s3.secret_key\" = \"*XXX\","
            + " \"column_separator\" = \",\","
            + " \"csv_schema\" = \"user_id:int;name:string;age:int\""
            + ")";
        parseAndCheck(sql, res);

        sql = "SELECT * FROM s3_tbl("
            + " \"uri\" = \"s3://your_bucket_name/s3load_example.csv\","
            + " \"format\" = \"csv\","
            + " \"provider\" = \"OSS\","
            + " \"s3.endpoint\" = \"oss-cn-hangzhou.aliyuncs.com\","
            + " \"s3.region\" = \"oss-cn-hangzhou\","
            + " \"s3.access_key\" = \"abc\","
            + " \"s3.secret_key\" = \"abc\","
            + " \"column_separator\" = \",\","
            + " \"csv_schema\" = \"user_id:int;name:string;age:int\""
            + ")";

        res = "SELECT * FROM s3_tbl("
            + " \"uri\" = \"s3://your_bucket_name/s3load_example.csv\","
            + " \"format\" = \"csv\","
            + " \"provider\" = \"OSS\","
            + " \"s3.endpoint\" = \"oss-cn-hangzhou.aliyuncs.com\","
            + " \"s3.region\" = \"oss-cn-hangzhou\","
            + " \"s3.access_key\" = \"abc\","
            + " \"s3.secret_key\" = \"*XXX\","
            + " \"column_separator\" = \",\","
            + " \"csv_schema\" = \"user_id:int;name:string;age:int\""
            + ")";
        parseAndCheck(sql, res);

        sql = "SET LDAP_ADMIN_PASSWORD = PASSWORD('123456')";
        res = "SET LDAP_ADMIN_PASSWORD = PASSWORD('*XXX')";
        parseAndCheck(sql, res);

        sql = "SET PASSWORD FOR 'admin' = PASSWORD('123456')";
        res = "SET PASSWORD FOR 'admin' = PASSWORD('*XXX')";
        parseAndCheck(sql, res);
    }

    private void parseAndCheck(String sql, String expected) {
        StatementBase parsedStmt = parser.parseSQL(sql).get(0);
        AuditLogHelper.logAuditLog(ctx, sql, parsedStmt, null, false);
        AuditEvent event = auditEvents.get(auditEvents.size() - 1);
        Assertions.assertEquals(expected, event.stmt);
    }
}
