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

suite("test_alter_table_partition", "p0") {
    def tbName = "test_alter_example_range_tbl"
    def dropSql = """ DROP TABLE IF EXISTS ${tbName} """
    def initTable = "CREATE TABLE IF NOT EXISTS ${tbName}\n" +
            "(\n" +
            "    `user_id` LARGEINT NOT NULL COMMENT \"用户id\",\n" +
            "    `date` DATE NOT NULL COMMENT \"数据灌入日期时间\",\n" +
            "    `timestamp` DATETIME NOT NULL COMMENT \"数据灌入的时间戳\",\n" +
            "    `city` VARCHAR(20) COMMENT \"用户所在城市\",\n" +
            "    `age` SMALLINT COMMENT \"用户年龄\",\n" +
            "    `sex` TINYINT COMMENT \"用户性别\",\n" +
            "    `last_visit_date` DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户最后一次访问时间\",\n" +
            "    `cost` BIGINT SUM DEFAULT \"0\" COMMENT \"用户总消费\",\n" +
            "    `max_dwell_time` INT MAX DEFAULT \"0\" COMMENT \"用户最大停留时间\",\n" +
            "    `min_dwell_time` INT MIN DEFAULT \"99999\" COMMENT \"用户最小停留时间\"\n" +
            ")\n" +
            "ENGINE=OLAP\n" +
            "AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)\n" +
            "PARTITION BY RANGE(`date`)\n" +
            "(\n" +
            "    PARTITION `p201001` VALUES LESS THAN (\"2010-01-01\"),\n" +
            "    PARTITION `p201002` VALUES LESS THAN (\"2010-02-01\"),\n" +
            "    PARTITION `p2011` VALUES [(\"2011-01-01\"), (\"2012-01-01\"))\n" +
            ")\n" +
            "DISTRIBUTED BY HASH(`user_id`) BUCKETS 16\n" +
            "PROPERTIES\n" +
            "(\n" +
            "    \"replication_num\" = \"1\"\n" +
            ");"
    def result;
    def errorMessage;
    def sqlString;

    //Add partitions, existing partitions [MIN, 2013-01-01), add partitions [2013-01-01, 2014-01-01), using default bucket partitioning method
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "ADD PARTITION p1 VALUES LESS THAN (\"2010-03-01\");"

    //Add partitions and use new bucket numbers
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "ADD PARTITION p1 VALUES LESS THAN (\"2015-01-01\")\n" +
            "DISTRIBUTED BY HASH(`user_id`) BUCKETS 20;"

    //Add partitions and use new replicas
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "ADD PARTITION p1 VALUES LESS THAN (\"2015-01-01\")\n" +
            "(\"replication_num\"=\"1\");"

    //Modify the number of partition copies
    sql dropSql;
    sql initTable;
    sqlString = """ALTER TABLE ${tbName} MODIFY PARTITION p2011 SET("replication_num"="1");"""
    if (isCloudMode()){
        errorMessage = "errCode = 2, detailMessage = Cann't modify property 'replication_num' in cloud mode."
        expectException({
            sql sqlString
        }, errorMessage)
    }else {
        sql sqlString
    }

    //Batch modify specified partitions
    sql dropSql;
    sql initTable;
    sqlString = """ALTER TABLE ${tbName} MODIFY PARTITION (p201001, p201002, p2011) SET("replication_num"="1");"""
    if (isCloudMode()){
        errorMessage = "errCode = 2, detailMessage = Cann't modify property 'replication_num' in cloud mode."
        expectException({
            sql sqlString
        }, errorMessage)
    }else {
        sql sqlString
    }




    //Batch modify all partitions
    sql dropSql;
    sql initTable;
    sqlString = """ALTER TABLE ${tbName} MODIFY PARTITION (*) SET("storage_medium"="HDD");"""
    if (isCloudMode()){
        errorMessage = "errCode = 2, detailMessage = Cann't modify property 'storage_medium' in cloud mode."
        expectException({
            sql sqlString
        }, errorMessage)
    }else {
        sql sqlString
    }


    //delete a partition
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "DROP PARTITION p201001;"

    //Batch delete partitions
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "DROP PARTITION p201001,\n" +
            "DROP PARTITION p201002;"

    //Add a partition with specified upper and lower bounds
    sql dropSql;
    sql initTable;
    sql "ALTER TABLE ${tbName}\n" +
            "ADD PARTITION p1 VALUES [(\"2014-01-01\"), (\"2014-02-01\"));"

    //Batch add partitions for numeric and temporal types
    if (isCloudMode()) {
        sql dropSql;
        sql initTable;
        sql "ALTER TABLE ${tbName}\n" +
                "ADD PARTITIONS FROM (\"2012-01-01\") TO (\"2013-01-01\") INTERVAL 1 YEAR;"
    }


}
