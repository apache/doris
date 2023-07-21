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

suite("test_window_function_with_project") {
    sql """
        drop table if exists t_retention1;
    """

    sql """
        drop view if exists v_tes1;
    """

    sql """
        drop view if exists v_tes2;
    """
    
    sql """
         CREATE TABLE `t_retention1` (
        `dlsj` varchar(50)   NOT NULL,
        `zzjhsj` varchar(50)  DEFAULT NULL,
        `hzzt` varchar(50)  DEFAULT NULL,
        `kf` varchar(50)  DEFAULT NULL,
        `khssdq` varchar(50)  DEFAULT NULL,
        `tel` varchar(50)  DEFAULT NULL,
        `moshi` varchar(50) DEFAULT NULL,
        `tuigqd` varchar(50)  DEFAULT NULL,
        `jhdlsjc` int(11) DEFAULT NULL
        ) 
        UNIQUE KEY(`dlsj`)
        DISTRIBUTED BY HASH(`dlsj`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """
        CREATE VIEW v_tes1  AS
        SELECT
            `dlsj` AS `dlsj`,
            `zzjhsj` AS `zzjhsj`,
            `hzzt` AS `hzzt`,
            `kf` AS `kf`,
            `khssdq` AS `khssdq`,
            `tel` AS `tel`,
            `moshi` AS `moshi`,
            `tuigqd` AS `tuigqd`,
            `jhdlsjc` AS `jhdlsjc`
        FROM
            `t_retention1`;
    """

    sql """
        CREATE VIEW `v_tes2`   AS
        SELECT
            `dlsj` AS `dlsj`,
            str_to_date(`zzjhsj`,'%Y-%m-%d') AS `zzjhsj`,
            `hzzt` AS `hzzt`,
            `kf` AS `kf`,
            `khssdq` AS `khssdq`,
            `tel` AS `tel`,
            `moshi` AS `moshi`,
            `tuigqd` AS `tuigqd`,
            `jhdlsjc` AS `jhdlsjc`
        FROM
            `v_tes1` t_tmp_a;
    """

    sql """
        SELECT
            `dlsj` AS `dlsj`,
            `zzjhsj` AS `zzjhsj`,
            `hzzt` AS `hzzt`,
            `kf` AS `kf`,
            `khssdq` AS `khssdq`,
            `tel` AS `tel`,
            `moshi` AS `moshi`,
            `tuigqd` AS `tuigqd`,
            `jhdlsjc` AS `jhdlsjc`,
            multi_distinct_count(`t_tmp_a`.`tel`) OVER () AS `tel_distinct_count`
        FROM
            `v_tes2` t_tmp_a;
    """

    sql """
        drop table if exists t_retention1;
    """

    sql """
        drop view if exists v_tes1;
    """

    sql """
        drop view if exists v_tes2;
    """
}
