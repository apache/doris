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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.SqlParserUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

public class CreateJobStmtTest {

    @Test
    public void createOnceTimeJobStmt() throws Exception {
        String sql = "CREATE JOB job1 ON SCHEDULE AT \"2023-02-15\" DO SELECT * FROM `address` ;";
        CreateJobStmt jobStmt = sqlParse(sql);
        System.out.println(jobStmt.getDoStmt().toSql());
        Assertions.assertEquals("SELECT * FROM `address`", jobStmt.getDoStmt().toSql());

        String badExecuteSql = "CREATE JOB job1 ON SCHEDULE AT \"2023-02-15\" DO selects * from address ;";
        Assertions.assertThrows(AnalysisException.class, () -> {
            sqlParse(badExecuteSql);
        });
        String badSql = "CREATE JOB job1 ON SCHEDULE AT \"2023-02-15\" STARTS \"2023-02-15\" DO selects * from address ;";
        Assertions.assertThrows(AnalysisException.class, () -> {
            sqlParse(badSql);
        });
    }

    private CreateJobStmt sqlParse(String sql) throws Exception {
        org.apache.doris.analysis.SqlScanner input = new org.apache.doris.analysis.SqlScanner(new StringReader(sql));
        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(input);
        CreateJobStmt jobStmt = (CreateJobStmt) SqlParserUtils.getStmt(parser, 0);
        return jobStmt;
    }


    @Test
    public void createCycleJob() throws Exception {
        String sql = "CREATE JOB job1 ON SCHEDULE EVERY  1 SECOND STARTS \"2023-02-15\" DO SELECT * FROM `address` ;";
        CreateJobStmt jobStmt = sqlParse(sql);
        Assertions.assertEquals("SELECT * FROM `address`", jobStmt.getDoStmt().toSql());
        sql = "CREATE JOB job1 ON SCHEDULE EVERY  1 SECOND ENDS \"2023-02-15\" DO SELECT * FROM `address` ;";
        jobStmt = sqlParse(sql);
        Assertions.assertEquals("SELECT * FROM `address`", jobStmt.getDoStmt().toSql());
        sql = "CREATE JOB job1 ON SCHEDULE EVERY  1 SECOND STARTS \"2023-02-15\" ENDS \"2023-02-16\" DO SELECT * FROM `address` ;";
        jobStmt = sqlParse(sql);
        Assertions.assertEquals("SELECT * FROM `address`", jobStmt.getDoStmt().toSql());
        sql = "CREATE JOB job1 ON SCHEDULE EVERY  1 SECOND  DO SELECT * FROM `address` ;";
        jobStmt = sqlParse(sql);
        Assertions.assertEquals("SELECT * FROM `address`", jobStmt.getDoStmt().toSql());
        String badExecuteSql = "CREATE JOB job1 ON SCHEDULE AT \"2023-02-15\" DO selects * from address ;";
        Assertions.assertThrows(AnalysisException.class, () -> {
            sqlParse(badExecuteSql);
        });
        String badSql = "CREATE JOB job1 ON SCHEDULE AT \"2023-02-15\" STARTS \"2023-02-15\" DO selects * from address ;";
        Assertions.assertThrows(AnalysisException.class, () -> {
            sqlParse(badSql);
        });
    }
}
