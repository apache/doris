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

suite("test_case_when") {
    def tableName = "dws_scan_qrcode_user_ts"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            dt DATE NOT NULL ,
            hour_time INT NOT NULL ,
            merchant_id INT NOT NULL ,
            channel_id char(5) NOT NULL ,
            station_type char(5) NULL ,
            station_name varchar(55) NULL ,
            source char(5) NULL ,
            passenger_flow BIGINT SUM DEFAULT '1' ,
            user_id bitmap BITMAP_UNION ,
            price BIGINT SUM ,
            discount BIGINT SUM 
        )
        AGGREGATE KEY(dt,hour_time, merchant_id,channel_id,station_type,station_name,`source`)
        DISTRIBUTED BY HASH(dt,hour_time,merchant_id,channel_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 1, 45010002, '01', '00', 'xx站', '', 1, to_bitmap(0), 300, 300); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 1, 45010002, '01', '00', 'xxx站', '', 3, to_bitmap(0), 400, 400); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 2, 45010002, '00', '01', 'xx站', 'CHL', 1, to_bitmap(0), NULL, 23); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 3, 45010002, '00', '00', 'xx站', 'CHL', 1, to_bitmap(0), NULL, NULL); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 3, 45010002, '01', '00', 'xxxx站', '', 4, to_bitmap(0), 60, 60); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 3, 45010002, '01', '00', 'xxxx站', '', 2, to_bitmap(0), 200, 200); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 4, 45010002, '01', '00', 'xxxx站', '', 5, to_bitmap(0), 1000, 1000); "
    sql " INSERT INTO ${tableName} (`dt`, `hour_time`, `merchant_id`, `channel_id`, `station_type`, `station_name`, `source`, `passenger_flow`, `user_id`, `price`, `discount`) VALUES ('2019-01-01', 4, 45010002, '01', '00', 'xxx站', '', 1, to_bitmap(0), 20, 20); "

    try_sql """
    select
        CAST(
            CASE
            WHEN source is null THEN null
            ELSE null
            END AS bitmap
        ) as c0
    FROM
        dws_scan_qrcode_user_ts;
    """
}
