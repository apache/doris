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

suite("test_list_partition", "p0") {
    // test query list partition table
    sql "drop table if exists list_par"
    sql """
        CREATE TABLE IF NOT EXISTS list_par ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY LIST(k1) ( 
            PARTITION p1 VALUES IN ("1","2","3","4"), 
            PARTITION p2 ("5","6","7","8","9","10","11","12","13","14"), 
            PARTITION p3 VALUES IN ("15") ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    List<List<Object>> result1  = sql "show tables like 'list_par'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    List<List<Object>> result2  = sql "show partitions from list_par"
    logger.info("${result2}")
    assertEquals(result2.size(), 3)
    sql "drop table list_par"

    sql "drop table if exists test_list_partition_select_tb"
    sql """
    CREATE TABLE test_list_partition_select_tb (
        k1 tinyint NOT NULL, 
        k2 smallint NOT NULL, 
        k3 int NOT NULL, 
        k4 bigint NOT NULL, 
        k5 decimal(9, 3) NOT NULL, 
        k6 char(5) NOT NULL, 
        k10 date NOT NULL, 
        k11 datetime NOT NULL, 
        k7 varchar(20) NOT NULL, 
        k8 double max NOT NULL, 
        k9 float sum NOT NULL ) 
    AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7) 
    PARTITION BY LIST(k1) ( 
        PARTITION p1 VALUES IN ("-128","-127","-126","-125","-124","-123","-122","-121","-120","-119","-118","-117","-116","-115","-114","-113","-112","-111","-110","-109","-108","-107","-106","-105","-104","-103","-102","-101","-100","-99","-98","-97","-96","-95","-94","-93","-92","-91","-90","-89","-88","-87","-86","-85","-84","-83","-82","-81","-80","-79","-78","-77","-76","-75","-74","-73","-72","-71","-70","-69","-68","-67","-66","-65"), 
        PARTITION p2 ("-64","-63","-62","-61","-60","-59","-58","-57","-56","-55","-54","-53","-52","-51","-50","-49","-48","-47","-46","-45","-44","-43","-42","-41","-40","-39","-38","-37","-36","-35","-34","-33","-32","-31","-30","-29","-28","-27","-26","-25","-24","-23","-22","-21","-20","-19","-18","-17","-16","-15","-14","-13","-12","-11","-10","-9","-8","-7","-6","-5","-4","-3","-2","-1"), 
        PARTITION p3 VALUES IN ("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35","36","37","38","39","40","41","42","43","44","45","46","47","48","49","50","51","52","53","54","55","56","57","58","59","60","61","62","63"), 
        PARTITION p4 ("64","65","66","67","68","69","70","71","72","73","74","75","76","77","78","79","80","81","82","83","84","85","86","87","88","89","90","91","92","93","94","95","96","97","98","99","100","101","102","103","104","105","106","107","108","109","110","111","112","113","114","115","116","117","118","119","120","121","122","123","124","125","126","127") 
    ) 
    DISTRIBUTED BY HASH(k1) BUCKETS 5
    properties("replication_num" = "1")
    """

    sql "insert into test_list_partition_select_tb select k1, k2, k3, k4, k5, k6, k10, k11, k7, k8, k9 from test_query_db.baseall where k1 is not null;"

    qt_sql1 "select k1, k2, k3 from test_list_partition_select_tb where k1 between 3 and 13 order by 1, 2, 3"
    qt_sql2 "select * from test_list_partition_select_tb where k1 > 5 order by k1"
    qt_sql3 "select k1, k2, k3 from test_list_partition_select_tb where k1 in (5, 6, 1) order by k1"
    qt_sql4 "select * from test_list_partition_select_tb where k1 != 10 order by k1"
    qt_sql5 "select k1, k2, k3 from test_list_partition_select_tb where k1 not in (1, 2, 3, 100) order by k1"
    qt_sql6 "select * from test_list_partition_select_tb where k1 is null order by k1;"
}
