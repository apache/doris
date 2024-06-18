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

suite("show_constraint") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """ DROP TABLE IF EXISTS `t1` """
    sql """ DROP TABLE IF EXISTS `t2` """
    sql """
        CREATE TABLE `t1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """
        CREATE TABLE `t2` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
    alter table t1 add constraint pk primary key (id)
    """
    sql """
    alter table t2 add constraint pk primary key (id)
    """

    order_qt_add_primary """
    show constraints from t1;
    """

    sql """
    alter table t1 add constraint uk unique (id)
    """

    order_qt_add_unique """
    show constraints from t1;
    """

    sql """
    alter table t1 add constraint fk1 foreign key (id) references t2(id)
    """
    sql """
    alter table t2 add constraint fk2 foreign key (id) references t1(id)
    """

    order_qt_add_foreign """
    show constraints from t1;
    """

    sql """
    alter table t1 drop constraint uk
    """

    order_qt_drop_uk """
    show constraints from t1;
    """

    sql """
    alter table t1 drop constraint fk1
    """

    order_qt_drop_fk """
    show constraints from t1;
    """

    sql """
    alter table t1 drop constraint pk
    """

    order_qt_drop_pk """
    show constraints from t1;
    """

    order_qt_drop_fk_cascades """
    show constraints from t2;
    """

}
