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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CreateJobStmtTest {

    @Test
    public void testParseExecuteSql() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = CreateJobStmt.class.getDeclaredMethod("parseExecuteSql", String.class, String.class, String.class);
        method.setAccessible(true);
        String executeSql = "insert into table.B select * from table.A ;";
        String comment = "do do do do ";
        String jobName = "do";
        String doKeywordJobSql = "Create job " + jobName
                + "on Scheduler every second comment " + comment + "\n"
                + "do"
                + executeSql;

        String result = (String) method.invoke(null, doKeywordJobSql, jobName, comment);
        Assertions.assertEquals(executeSql, result.trim());
        executeSql = "insert into table.do select * from do.B ;";
        comment = "do starts  end do \n \b \r ";
        jobName = "do";
        doKeywordJobSql = "Create job " + jobName
                + "on Scheduler every second comment " + comment + "do\n"
                + executeSql;
        result = (String) method.invoke(null, doKeywordJobSql, jobName, comment);
        Assertions.assertEquals(executeSql, result.trim());
    }
}
