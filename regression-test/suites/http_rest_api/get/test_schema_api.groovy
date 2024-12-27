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

import org.apache.doris.regression.util.Http

suite("test_schema_api") {

    def thisDb = sql """select database()""";
    thisDb = thisDb[0][0];
    logger.info("current database is ${thisDb}");

    def tbName = "test_schema_api"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
        CREATE TABLE ${tbName}
        (
            `id` LARGEINT NOT NULL COMMENT "id",
            `c1` DECIMAL(10, 2) COMMENT "decimal columns",
            `c2` date NOT NULL COMMENT "date columns",
            `c3` VARCHAR(20) COMMENT "nullable columns",
            `c4` VARCHAR COMMENT "varchar columns",
            `c5` BIGINT DEFAULT "0" COMMENT "test columns"
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    //exist table
    def url = String.format("http://%s/api/%s/%s/_schema", context.config.feHttpAddress, thisDb, tbName)
    def result = Http.GET(url, true)
    assertTrue(result.code == 0)
    assertEquals(result.msg, "success")
    // parsing
    def resultList = result.data.properties
    assertTrue(resultList.size() == 6)

    // not exist catalog
    def url2 = String.format("http://%s/api/%s/%s/%s/_schema", context.config.feHttpAddress, "notexistctl", thisDb, tbName)
    def result2 = Http.GET(url2, true)
    assertTrue(result2.code != 0)
    assertTrue(result2.data.contains("Unknown catalog"))

}
