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
/*


    // Test DDL and Data:

    drop table `multi_partitions`;
    CREATE TABLE `multi_partitions` (
      `city` string,
      `gender` boolean,
      `mnt` smallint,
      `order_rate` float,
      `amount` decimal(24,9),
      `cut_date` date,
      `create_time` datetime,
      `finished_time` timestamp_ntz
    ) PARTITIONED BY (
      `yy` string,
      `mm` string,
      `dd` string,
      `pt` bigint
    );
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='01', pt=1) VALUES
    ('New York', TRUE, CAST(12 AS SMALLINT), CAST(0.75 AS FLOAT), CAST(1234.567890123 AS DECIMAL(24,9)),
     CAST('2023-08-01' AS DATE), CAST('2023-08-01 10:30:00' AS DATETIME), CAST('2023-08-01 11:00:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='01', pt=1) VALUES
    ('Los Angeles', FALSE, CAST(10 AS SMALLINT), CAST(1.15 AS FLOAT), CAST(9876.543210987 AS DECIMAL(24,9)),
     CAST('2023-08-01' AS DATE), CAST('2023-08-01 12:00:00' AS DATETIME), CAST('2023-08-01 12:30:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='02', pt=2) VALUES
    ('Chicago', TRUE, CAST(8 AS SMALLINT), CAST(0.90 AS FLOAT), CAST(5555.123456789 AS DECIMAL(24,9)),
     CAST('2023-08-02' AS DATE), CAST('2023-08-02 08:15:00' AS DATETIME), CAST('2023-08-02 08:45:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='02', pt=2) VALUES
    ('Houston', FALSE, CAST(15 AS SMALLINT), CAST(1.25 AS FLOAT), CAST(2222.987654321 AS DECIMAL(24,9)),
     CAST('2023-08-02' AS DATE), CAST('2023-08-02 14:45:00' AS DATETIME), CAST('2023-08-02 15:15:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='03', pt=3) VALUES
    ('Phoenix', TRUE, CAST(5 AS SMALLINT), CAST(0.50 AS FLOAT), CAST(7777.333333333 AS DECIMAL(24,9)),
     CAST('2023-08-03' AS DATE), CAST('2023-08-03 09:00:00' AS DATETIME), CAST('2023-08-03 09:30:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='03', pt=3) VALUES
    ('Philadelphia', FALSE, CAST(7 AS SMALLINT), CAST(0.85 AS FLOAT), CAST(8888.222222222 AS DECIMAL(24,9)),
     CAST('2023-08-03' AS DATE), CAST('2023-08-03 11:30:00' AS DATETIME), CAST('2023-08-03 12:00:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='04', pt=4) VALUES
    ('San Antonio', TRUE, CAST(9 AS SMALLINT), CAST(1.05 AS FLOAT), CAST(3333.666666666 AS DECIMAL(24,9)),
     CAST('2023-08-04' AS DATE), CAST('2023-08-04 07:00:00' AS DATETIME), CAST('2023-08-04 07:30:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='04', pt=4) VALUES
    ('San Diego', FALSE, CAST(11 AS SMALLINT), CAST(1.10 AS FLOAT), CAST(4444.555555555 AS DECIMAL(24,9)),
     CAST('2023-08-04' AS DATE), CAST('2023-08-04 10:00:00' AS DATETIME), CAST('2023-08-04 10:30:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='05', pt=5) VALUES
    ('Dallas', TRUE, CAST(6 AS SMALLINT), CAST(0.65 AS FLOAT), CAST(6666.444444444 AS DECIMAL(24,9)),
     CAST('2023-08-05' AS DATE), CAST('2023-08-05 13:00:00' AS DATETIME), CAST('2023-08-05 13:30:00' AS timestamp_ntz));
    INSERT INTO `multi_partitions` PARTITION (yy='2023', mm='08', dd='05', pt=5) VALUES
    ('San Jose', FALSE, CAST(14 AS SMALLINT), CAST(1.20 AS FLOAT), CAST(9999.111111111 AS DECIMAL(24,9)),
     CAST('2023-08-05' AS DATE), CAST('2023-08-05 15:30:00' AS DATETIME), CAST('2023-08-05 16:00:00' AS timestamp_ntz));
    drop table mc_parts;
    CREATE TABLE `mc_parts` (
      `mc_bigint` bigint,
      `mc_string` string
    )PARTITIONED BY (
    `dt` string
    );
    INSERT INTO `mc_parts` PARTITION (dt='2023-08-01') VALUES
    (1001, 'Sample data 1');
    INSERT INTO `mc_parts` PARTITION (dt='2023-08-02') VALUES
    (1002, 'Sample data 2');
    INSERT INTO `mc_parts` PARTITION (dt='2023-08-03') VALUES
    (1003, 'Sample data 3');
    INSERT INTO `mc_parts` PARTITION (dt='2023-08-04') VALUES
    (1004, 'Sample data 4');
    INSERT INTO `mc_parts` PARTITION (dt='2023-08-05') VALUES
    (1005, 'Sample data 5');
    CREATE TABLE int_types (
      mc_boolean BOOLEAN,
      mc_tinyint TINYINT,
      mc_int SMALLINT,
      mc_bigint BIGINT
    );
    INSERT INTO int_types VALUES (TRUE, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), CAST(-9223372036854775807 AS BIGINT));
    INSERT INTO int_types VALUES (FALSE, CAST(127 AS TINYINT), CAST(32767 AS SMALLINT), CAST(9223372036854775807 AS BIGINT));
    INSERT INTO int_types VALUES (TRUE, CAST(0 AS TINYINT), CAST(0 AS SMALLINT), CAST(0 AS BIGINT));
    INSERT INTO int_types VALUES (FALSE, CAST(0 AS TINYINT), CAST(0 AS SMALLINT), CAST(0 AS BIGINT));
    INSERT INTO int_types VALUES (TRUE, CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(1 AS BIGINT));
    INSERT INTO int_types VALUES (FALSE, CAST(-1 AS TINYINT), CAST(-1 AS SMALLINT), CAST(-1 AS BIGINT));
    INSERT INTO int_types VALUES
    (TRUE, CAST(1 AS TINYINT), CAST(100 AS SMALLINT), CAST(1000 AS BIGINT)),
    (FALSE, CAST(2 AS TINYINT), CAST(200 AS SMALLINT), CAST(2000 AS BIGINT)),
    (TRUE, CAST(3 AS TINYINT), CAST(300 AS SMALLINT), CAST(3000 AS BIGINT)),
    (FALSE, CAST(4 AS TINYINT), CAST(400 AS SMALLINT), CAST(4000 AS BIGINT)),
    (TRUE, CAST(5 AS TINYINT), CAST(500 AS SMALLINT), CAST(5000 AS BIGINT)),
    (FALSE, CAST(6 AS TINYINT), CAST(600 AS SMALLINT), CAST(6000 AS BIGINT)),
    (TRUE, CAST(7 AS TINYINT), CAST(700 AS SMALLINT), CAST(7000 AS BIGINT)),
    (FALSE, CAST(8 AS TINYINT), CAST(800 AS SMALLINT), CAST(8000 AS BIGINT)),
    (TRUE, CAST(9 AS TINYINT), CAST(900 AS SMALLINT), CAST(9000 AS BIGINT)),
    (FALSE, CAST(10 AS TINYINT), CAST(1000 AS SMALLINT), CAST(10000 AS BIGINT));
    CREATE TABLE web_site (
      web_site_sk BIGINT,
      web_site_id STRING,
      web_rec_start_date DATE,
      web_rec_end_date DATE,
      web_name STRING,
      web_open_date_sk BIGINT,
      web_close_date_sk BIGINT,
      web_class STRING,
      web_manager STRING,
      web_mkt_id INT,
      web_mkt_class STRING,
      web_mkt_desc STRING,
      web_market_manager STRING,
      web_company_id INT,
      web_company_name STRING,
      web_street_number STRING,
      web_street_name STRING,
      web_street_type STRING,
      web_suite_number STRING,
      web_city STRING,
      web_county STRING,
      web_state STRING,
      web_zip STRING,
      web_country STRING,
      web_gmt_offset DOUBLE,
      web_tax_percentage DECIMAL(5,2)
    );
    INSERT INTO web_site VALUES
    (
      CAST(1 AS BIGINT),
      'WS0001',
      CAST('2023-01-01' AS DATE),
      CAST('2023-12-31' AS DATE),
      'Example Web Site 1',
      CAST(20230101 AS BIGINT),
      CAST(20231231 AS BIGINT),
      'E-commerce',
      'John Doe',
      CAST(101 AS INT),
      'Retail',
      'Online retail website',
      'Jane Smith',
      CAST(201 AS INT),
      'Example Company',
      '1234',
      'Main Street',
      'Apt',
      'Unit 101',
      'Metropolis',
      'County',
      'NY',
      '12345',
      'USA',
      CAST(-5.0 AS DOUBLE),
      CAST(8.25 AS DECIMAL(5,2))
    );
    INSERT INTO web_site VALUES
    (
      CAST(2 AS BIGINT),
      'WS0002',
      CAST('2023-02-01' AS DATE),
      CAST('2023-11-30' AS DATE),
      'Example Web Site 2',
      CAST(20230201 AS BIGINT),
      CAST(20231130 AS BIGINT),
      'Technology',
      'Alice Johnson',
      CAST(102 AS INT),
      'Tech',
      'Tech news and reviews',
      'Bob Brown',
      CAST(202 AS INT),
      'Tech Innovations',
      '5678',
      'Tech Avenue',
      'Suite',
      'Suite 200',
      'Gotham',
      'County',
      'CA',
      '67890',
      'USA',
      CAST(-8.0 AS DOUBLE),
      CAST(7.50 AS DECIMAL(5,2))
    );
    INSERT INTO web_site VALUES
    (
      CAST(3 AS BIGINT),
      'WS0003',
      CAST('2023-03-01' AS DATE),
      CAST('2023-10-31' AS DATE),
      'Example Web Site 3',
      CAST(20230301 AS BIGINT),
      CAST(20231031 AS BIGINT),
      'Healthcare',
      'Robert White',
      CAST(103 AS INT),
      'Health',
      'Healthcare services and products',
      'Emily Green',
      CAST(203 AS INT),
      'Health Corp',
      '9101',
      'Health Drive',
      'Floor',
      'Floor 3',
      'Star City',
      'County',
      'TX',
      '23456',
      'USA',
      CAST(-6.0 AS DOUBLE),
      CAST(6.75 AS DECIMAL(5,2))
    );
    INSERT INTO web_site VALUES
    (
      CAST(4 AS BIGINT),
      'WS0004',
      CAST('2023-04-01' AS DATE),
      CAST('2023-09-30' AS DATE),
      'Example Web Site 4',
      CAST(20230401 AS BIGINT),
      CAST(20230930 AS BIGINT),
      'Education',
      'David Black',
      CAST(104 AS INT),
      'EdTech',
      'Educational technology platform',
      'Fiona Grey',
      CAST(204 AS INT),
      'Edu Tech',
      '1122',
      'Education Lane',
      'Building',
      'Building 1',
      'Smallville',
      'County',
      'FL',
      '34567',
      'USA',
      CAST(-4.0 AS DOUBLE),
      CAST(5.00 AS DECIMAL(5,2))
    );
    INSERT INTO web_site VALUES
    (
      CAST(5 AS BIGINT),
      'WS0005',
      CAST('2023-05-01' AS DATE),
      CAST('2023-08-31' AS DATE),
      'Example Web Site 5',
      CAST(20230501 AS BIGINT),
      CAST(20230831 AS BIGINT),
      'Travel',
      'Sophia Blue',
      CAST(105 AS INT),
      'Travel',
      'Travel and booking services',
      'Daniel Red',
      CAST(205 AS INT),
      'Travel Inc',
      '3344',
      'Tourist Street',
      'Unit',
      'Unit 5',
      'Metropolis',
      'County',
      'WA',
      '45678',
      'USA',
      CAST(-7.0 AS DOUBLE),
      CAST(8.00 AS DECIMAL(5,2))
    );
  
 */
suite("test_external_catalog_maxcompute", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk");
        String mc_db = "jz_datalake"
        String mc_catalog_name = "test_external_mc_catalog"

        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
            );
        """

        // query data test
        def q01 = {
            order_qt_q1 """ select count(*) from web_site """
        }
        // data type test
        def q02 = {
            order_qt_q2 """ select * from web_site where web_site_id>='WS0003' order by web_site_id; """ // test char,date,varchar,double,decimal
            order_qt_q3 """ select * from int_types  """ // test bool,tinyint,int,bigint
            order_qt_q3_1 """ select * from int_types order by mc_boolean  """
            order_qt_q3_2 """ select * from int_types order by mc_int"""
        }
        // test partition table filter
        def q03 = {
            order_qt_q4 """ select * from mc_parts where dt = '2023-08-03' order by mc_bigint """
            order_qt_q5 """ select * from mc_parts where dt > '2023-08-03' order by mc_bigint """
            order_qt_q6 """ select * from mc_parts where dt > '2023-08-03' and mc_bigint > 1002 """
            order_qt_q7 """ select * from mc_parts where dt < '2023-08-03' or (mc_bigint > 1003 and dt > '2023-08-04') order by mc_bigint, dt; """
        }

        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        q01()
        q02()
        q03()

        // replay test
        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
            );
        """
        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        order_qt_replay_q6 """ select * from mc_parts where dt >= '2023-08-03' and mc_bigint > 1001  order by mc_bigint """

        // test multi partitions prune
        sql """ refresh catalog ${mc_catalog_name} """
        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        order_qt_multi_partition_q1 """ show partitions from multi_partitions; """
        order_qt_multi_partition_q2 """ select pt, create_time, yy, mm, dd from multi_partitions where pt>-1 and yy > '' and mm > '' and dd >'' order by pt , dd; """
        order_qt_multi_partition_q3 """ select sum(pt), create_time, yy, mm, dd from multi_partitions where yy > '' and mm > '' and dd >'' group by create_time, yy, mm, dd order by create_time,dd ; """
        order_qt_multi_partition_q4 """ select count(*) from multi_partitions where pt>-1 and yy > '' and mm > '' and dd <= '30'; """
        order_qt_multi_partition_q5 """ select create_time, yy, mm, dd from multi_partitions where yy = '2023' and mm='08' and dd='04' order by pt ; """
        order_qt_multi_partition_q6 """ select max(pt), yy, mm from multi_partitions where yy = '2023' and mm='08' group by yy, mm order by yy, mm; """
        order_qt_multi_partition_q7 """ select count(*) from multi_partitions where yy < '2023' or dd < '03'; """
        order_qt_multi_partition_q8 """ select count(*) from multi_partitions where pt>=3; """
        order_qt_multi_partition_q9 """ select city,mnt,gender,finished_time,order_rate,cut_date,create_time,pt, yy, mm, dd from multi_partitions where pt >= 2 and pt < 4 and finished_time is not null; """
        order_qt_multi_partition_q10 """ select pt, yy, mm, dd from multi_partitions where pt >= 2 and create_time > '2023-08-03 03:11:00' order by pt, yy, mm, dd; """
    }
}
