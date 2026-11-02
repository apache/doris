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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_select_into_outfile_iam") {
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def tableName = "test_select_into_outfile_iam"

    sql """ drop table if exists ${tableName} force;"""
    sql """
        CREATE TABLE ${tableName}
        (
            siteid INT DEFAULT '10',
            citycode SMALLINT NOT NULL,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        """

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (2, 2, "yyy", 2),
            (3, 3, "zzz", 3)
        """
    sql """sync;"""

    def expectedRows = sql """
        SELECT CAST(siteid AS STRING), CAST(citycode AS STRING), username, CAST(pv AS STRING)
        FROM ${tableName} ORDER BY siteid
    """

    config.authCases.each { authCase ->
        logger.info("run ${name} with ${authCase.name}")
        def outfilePath = "${config.scheme}://${config.bucket}/${config.prefix}/" +
                "test_select_into_outfile_iam/${authCase.name}/${randomStr}"
        sql """
            SELECT * FROM ${tableName}
            INTO OUTFILE "${outfilePath}"
            FORMAT AS CSV
            PROPERTIES(
                "column_separator" = ",",
                ${authCase.storageSqlProperties}
            );
        """
        def exportedRows = sql """
            SELECT c1, c2, c3, c4 FROM s3(
                "uri" = "${outfilePath}*.csv",
                ${authCase.storageSqlProperties},
                "format" = "csv",
                "column_separator" = ","
            ) ORDER BY CAST(c1 AS INT)
        """
        assertEquals(expectedRows, exportedRows, authCase.name)
    }
}
