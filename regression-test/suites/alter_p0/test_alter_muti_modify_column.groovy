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

suite('test_alter_muti_modify_column') {
    def tbl = 'test_alter_muti_modify_column_tbl'
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            `id` BIGINT NOT NULL,
            `v1` BIGINT NULL,
            `v2` VARCHAR(128) NULL,
            `v3` VARCHAR(128) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tbl} VALUES
        (1, 10, 'abc', 'def'),
        (2, 20, 'xyz', 'uvw'),
        (3, 30, 'pqr', 'lmn');
    """

    // Check data before ALTER TABLE
    def resultBefore = sql """ SELECT * FROM ${tbl} ORDER BY id """
    assertEquals(3, resultBefore.size())
    assertEquals([1, 10, 'abc', 'def'], resultBefore[0])
    assertEquals([2, 20, 'xyz', 'uvw'], resultBefore[1])
    assertEquals([3, 30, 'pqr', 'lmn'], resultBefore[2])

    sql """
        ALTER TABLE ${tbl} 
        MODIFY COLUMN `v2` VARCHAR(512) NULL AFTER `v1`,
        MODIFY COLUMN `v3` VARCHAR(512) NULL AFTER `v2`;
    """

    // Wait for ALTER TABLE to take effect
    Thread.sleep(5000)

    // Check table structure after ALTER TABLE
    def resultCreate = sql """ SHOW CREATE TABLE ${tbl} """
    def createTbl = resultCreate[0][1].toString()
    assertTrue(createTbl.indexOf("`v2` VARCHAR(512) NULL") > 0)
    assertTrue(createTbl.indexOf("`v3` VARCHAR(512) NULL") > 0)

    // Check data after ALTER TABLE
    def resultAfter = sql """ SELECT * FROM ${tbl} ORDER BY id """
    assertEquals(3, resultAfter.size())
    assertEquals([1, 10, 'abc', 'def'], resultAfter[0])
    assertEquals([2, 20, 'xyz', 'uvw'], resultAfter[1])
    assertEquals([3, 30, 'pqr', 'lmn'], resultAfter[2])

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
