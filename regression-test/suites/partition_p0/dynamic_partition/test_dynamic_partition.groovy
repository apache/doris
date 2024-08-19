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
suite("test_dynamic_partition") {
    // todo: test dynamic partition
    sql "drop table if exists dy_par"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
    def result  = sql "show tables like 'dy_par'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    result = sql_return_maparray "show partitions from dy_par"
    // XXX: buckets at pos(8), next maybe impl by sql meta
    assertEquals(result.get(0).Buckets.toInteger(), 10)
    sql "drop table dy_par"

    sql "drop table if exists dy_par"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="YEAR",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
    result  = sql "show tables like 'dy_par'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    result = sql_return_maparray "show partitions from dy_par"
    // XXX: buckets at pos(8), next maybe impl by sql meta
    assertEquals(result.get(0).Buckets.toInteger(), 10)
    sql "drop table dy_par"

    sql "drop table if exists dy_par_bucket_set_by_distribution"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par_bucket_set_by_distribution
        ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
    result  = sql "show tables like 'dy_par_bucket_set_by_distribution'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    result = sql_return_maparray "show partitions from dy_par_bucket_set_by_distribution"
    // XXX: buckets at pos(8), next maybe impl by sql meta
    assertEquals(result.get(0).Buckets.toInteger(), 3)
    sql "drop table dy_par_bucket_set_by_distribution"
    sql "drop table if exists dy_par_bad"
    def isCloudMode = {
        def ret = sql_return_maparray  """show backends"""
        ret.Tag[0].contains("cloud_cluster_name")
    }

    // not support tag in cloud mode
    if (!isCloudMode) {
    test {
        sql """
        CREATE TABLE IF NOT EXISTS dy_par_bad ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.not_exist_tag: 1")
        """
        // check exception message contains
        exception "errCode = 2,"
    }
    }

    sql "drop table if exists dy_par_bad"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par ( k1 datev2 NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
    result  = sql "show tables like 'dy_par'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    sql "drop table dy_par"
    //
    sql "drop table if exists dy_par_bad"
    // not support tag in cloud mode
    if (!isCloudMode) { 
    test {
        sql """
        CREATE TABLE IF NOT EXISTS dy_par_bad ( k1 datev2 NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.not_exist_tag: 1")
        """
        // check exception message contains
        exception "errCode = 2,"
    }
    }
    sql "drop table if exists dy_par_bad"
}
