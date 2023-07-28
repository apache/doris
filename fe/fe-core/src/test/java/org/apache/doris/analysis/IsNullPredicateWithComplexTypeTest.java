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

import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IsNullPredicateWithComplexTypeTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        // create database
        createDatabase("test");

        createTable("CREATE TABLE test.complex (\n"
                + "  `dt` int(11) COMMENT \"\",\n"
                + "  `id` int(11) COMMENT \"\",\n"
                + "  `m` Map<STRING, INT> COMMENT \"\",\n"
                + "  `a` Array<BIGINT> COMMENT \"\",\n"
                + "  `s` Struct<f1:string, f2:int> COMMENT \"\",\n"
                + "  `value` varchar(8) COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`dt`)\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"10\"))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\"\n"
                + ");");
    }

    @Test
    public void testIsNull() throws Exception {
        String testStructIsNUll = "select * from test.complex where s is null";
        String testMapIsNUll = "select * from test.complex where m is null";
        String testArrayIsNUll = "select * from test.complex where a is null";
        Assertions.assertNotNull(getSQLPlanner(testStructIsNUll));
        Assertions.assertNotNull(getSQLPlanner(testMapIsNUll));
        Assertions.assertNotNull(getSQLPlanner(testArrayIsNUll));
    }

    @Test
    public void testCount() throws Exception {
        String testStructIsNUll = "select count(s) from test.complex";
        String testMapIsNUll = "select count(m) from test.complex";
        String testArrayIsNUll = "select count(a) from test.complex";
        Assertions.assertNotNull(getSQLPlanner(testStructIsNUll));
        Assertions.assertNotNull(getSQLPlanner(testMapIsNUll));
        Assertions.assertNotNull(getSQLPlanner(testArrayIsNUll));
    }

}
