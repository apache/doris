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

suite("bitmap_agg") {
    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"
    sql """
        CREATE TABLE `test_bitmap_agg` (
            `id` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        insert into `test_bitmap_agg`
        select number from numbers("number" = "20000");
    """

    qt_sql1 """
        select bitmap_count(bitmap_agg(id)) from `test_bitmap_agg`;
    """

    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"

    sql "DROP TABLE IF EXISTS test_bitmap_agg_nullable;"
    sql """
        CREATE TABLE `test_bitmap_agg_nullable` (
             `id` int(11) NULL
          ) ENGINE=OLAP
          DUPLICATE KEY(`id`)
          COMMENT 'OLAP'
          DISTRIBUTED BY HASH(`id`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "storage_format" = "V2",
          "light_schema_change" = "true",
          "disable_auto_compaction" = "false",
          "enable_single_replica_compaction" = "false"
          );
    """
    sql """
        insert into `test_bitmap_agg_nullable`
                select number from numbers("number" = "20000");
    """
    qt_sql2 """
        select bitmap_count(bitmap_agg(id)) from `test_bitmap_agg_nullable`;
    """
    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"

    sql "DROP TABLE IF EXISTS `test_bitmap_agg_nation`;"
    sql """
        CREATE TABLE `test_bitmap_agg_nation` (
          `N_NATIONKEY` int(11) NOT NULL,
          `N_NAME` char(25) NOT NULL,
          `N_REGIONKEY` int(11) NOT NULL,
          `N_COMMENT` varchar(152) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`N_NATIONKEY`, `N_NAME`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `test_bitmap_agg_nation` VALUES (0,'ALGERIA',0,' haggle. carefully final deposits detect slyly agai'),
        (1,'ARGENTINA',1,'al foxes promise slyly according to the regular accounts. bold requests alon'),
        (3,'CANADA',1,'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
        (8,'INDIA',2,'ss excuses cajole slyly across the packages. deposits print aroun'),
        (13,'JORDAN',4,'ic deposits are blithely about the carefully regular pa'),
        (17,'PERU',1,'platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun'),
        (22,'RUSSIA',3,' requests against the platelets use never according to the quickly regular pint'),
        (2,'BRAZIL',1,'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special '),
        (9,'INDONESIA',2,' slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'),
        (10,'IRAN',4,'efully alongside of the slyly final dependencies. '),
        (11,'IRAQ',4,'nic deposits boost atop the quickly final requests? quickly regula'),
        (12,'JAPAN',2,'ously. final, express gifts cajole a'),
        (15,'MOROCCO',0,'rns. blithely bold courts among the closely regular packages use furiously bold platelets?'),
        (18,'CHINA',2,'c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos'),
        (21,'VIETNAM',2,'hely enticingly express accounts. even, final '),
        (24,'UNITED STATES',1,'y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be'),
        (14,'KENYA',0,' pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t'),
        (16,'MOZAMBIQUE',0,'s. ironic, unusual asymptotes wake blithely r'),
        (19,'ROMANIA',3,'ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account'),
        (20,'SAUDI ARABIA',4,'ts. silent requests haggle. closely express packages sleep across the blithely'),
        (23,'UNITED KINGDOM',3,'eans boost carefully special requests. accounts are. carefull'),
        (4,'EGYPT',4,'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'),
        (5,'ETHIOPIA',0,'ven packages wake quickly. regu'),(6,'FRANCE',3,'refully final requests. regular, ironi'),
        (7,'GERMANY',3,'l platelets. regular accounts x-ray: unusual, regular acco');
    """

    qt_sql3 """
        select count(`n_nationkey`), count(distinct `n_nationkey`), bitmap_count(bitmap_agg(`n_nationkey`)) from `test_bitmap_agg_nation`;
    """
    qt_sql4 """
        select count(`n_nationkey`), bitmap_count(bitmap_agg(`n_nationkey`)) from `test_bitmap_agg_nation` group by `n_regionkey`;
    """
    qt_sql5 """
        select count(`n_nationkey`), bitmap_count(bitmap_agg(`n_nationkey`)) from `test_bitmap_agg_nation` group by `N_REGIONKEY` order by 1;
    """
    sql "DROP TABLE IF EXISTS `test_bitmap_agg_nation`;"
 }
