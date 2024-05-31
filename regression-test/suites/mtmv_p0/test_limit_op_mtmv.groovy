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

import org.junit.Assert;

suite("test_limit_op_mtmv") {
    def tableName = "t_test_limit_op_mtmv_user"
    def mvName = "test_limit_op_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(5000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"',
          `k3` DATE
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k3`)
        (
            FROM ("2020-01-01") TO ("2020-01-03") INTERVAL 1 DAY
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`k3`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${tableName};
    """

    // not allow add partition
    try {
        sql """
            alter table ${mvName} add partition p_20200103_20200104 values less than ("2020-01-04");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow drop partition
    try {
        sql """
            alter table ${mvName} drop partition p_20200102_20200103;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow modify partition
    try {
        sql """
            alter table ${mvName} MODIFY PARTITION (*) SET("storage_medium"="HDD");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow replace partition
    try {
        sql """
            ALTER TABLE ${mvName} REPLACE PARTITION (p_20200102_20200103) WITH TEMPORARY PARTITION (tp_20200102_20200103);
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow rename
    try {
        sql """
            alter table ${mvName} rename ${mvName}1
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow replace table
    try {
        sql """
            alter table ${mvName} REPLACE WITH TABLE tbl2
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }


    // not allow modify property of mv
    try {
        sql """
            alter table ${mvName} set("grace_period"="3333");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow add column
    try {
        sql """
            alter table ${mvName} ADD COLUMN new_col INT DEFAULT "0" AFTER num;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow add columns
    try {
        sql """
            alter table ${mvName} ADD COLUMN (new_col1 INT DEFAULT "0" ,new_col2 INT DEFAULT "0");
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow drop column
    try {
        sql """
            alter table ${mvName} DROP COLUMN num;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow modify column
    try {
        sql """
            alter table ${mvName} modify COLUMN num BIGINT;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }


    // not allow reorder column
    try {
        sql """
            alter table ${mvName} ORDER BY(num,k3,user_id);
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow modify column
    try {
        sql """
            alter table ${mvName} modify COLUMN num BIGINT;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow replace
    try {
        sql """
            alter table ${mvName} REPLACE WITH TABLE ${tableName};
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }


    // not allow add rollup
    try {
        sql """
            alter table ${mvName} ADD ROLLUP example_rollup_index(num, k3);;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // not allow drop rollup
    try {
        sql """
            alter table ${mvName} drop ROLLUP example_rollup_index;
            """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // allow modify comment
    try {
        sql """
            alter table ${mvName} MODIFY COMMENT "new table comment";
            """
    } catch (Exception e) {
        log.info(e.getMessage())
        Assert.fail();
    }

    // allow add index
    try {
        sql """
            CREATE INDEX index1 ON ${mvName} (num) USING INVERTED;
            """
    } catch (Exception e) {
        log.info(e.getMessage())
        Assert.fail();
    }
    wait_for_latest_op_on_table_finish(mvName, timeout)
    // allow drop index
    try {
        sql """
            DROP INDEX index1 ON ${mvName};
            """
    } catch (Exception e) {
        log.info(e.getMessage())
        Assert.fail();
    }

    // not allow dynamic_partition
    test {
        sql """ALTER TABLE ${mvName} set ("dynamic_partition.enable" = "true")"""
        exception "dynamic"
        }
    sql """drop materialized view if exists ${mvName};"""
    test {
          sql """
              CREATE MATERIALIZED VIEW ${mvName}
              BUILD DEFERRED REFRESH AUTO ON MANUAL
              partition by(`k3`)
              DISTRIBUTED BY RANDOM BUCKETS 2
              PROPERTIES ('replication_num' = '1','dynamic_partition.enable'='true')
              AS
              SELECT * FROM ${tableName};
          """
          exception "dynamic"
      }
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
