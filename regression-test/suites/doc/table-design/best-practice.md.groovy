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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/best-practice.md") {
    try {
        sql "drop table if exists session_data"
        sql """
        -- 例如 允许 KEY 重复仅追加新数据的日志数据分析
        CREATE TABLE session_data
        (
            visitorid   SMALLINT,
            sessionid   BIGINT,
            visittime   DATETIME,
            city        CHAR(20),
            province    CHAR(20),
            ip          varchar(32),
            brower      CHAR(20),
            url         VARCHAR(1024)
        )
        DUPLICATE KEY(visitorid, sessionid) -- 只用于指定排序列，相同的 KEY 行不会合并
        DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
        """

        sql "drop table if exists site_visit"
        sql """
        -- 例如 网站流量分析
        CREATE TABLE site_visit
        (
            siteid      INT,
            city        SMALLINT,
            username    VARCHAR(32),
            pv BIGINT   SUM DEFAULT '0' -- PV 浏览量计算
        )
        AGGREGATE KEY(siteid, city, username) -- 相同的 KEY 行会合并，非 KEY 列会根据指定的聚合函数进行聚合
        DISTRIBUTED BY HASH(siteid) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
        """

        sql "drop table if exists sales_order"
        sql """
        -- 例如 订单去重分析
        CREATE TABLE sales_order
        (
            orderid     BIGINT,
            status      TINYINT,
            username    VARCHAR(32),
            amount      BIGINT DEFAULT '0'
        )
        UNIQUE KEY(orderid) -- 相同的 KEY 行会合并
        DISTRIBUTED BY HASH(orderid) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
        """

        sql "drop table if exists sale_detail_bloom"
        sql """
        -- 创建示例：通过在建表语句的 PROPERTIES 里加上"bloom_filter_columns"="k1,k2,k3"
        -- 例如下面我们对表里的 saler_id,category_id 创建了 BloomFilter 索引。
        CREATE TABLE IF NOT EXISTS sale_detail_bloom  (
            sale_date date NOT NULL COMMENT "销售时间",
            customer_id int NOT NULL COMMENT "客户编号",
            saler_id int NOT NULL COMMENT "销售员",
            sku_id int NOT NULL COMMENT "商品编号",
            category_id int NOT NULL COMMENT "商品分类",
            sale_count int NOT NULL COMMENT "销售数量",
            sale_price DECIMAL(12,2) NOT NULL COMMENT "单价",
            sale_amt DECIMAL(20,2)  COMMENT "销售总金额"
        )
        Duplicate  KEY(sale_date, customer_id,saler_id,sku_id,category_id)
        DISTRIBUTED BY HASH(saler_id) BUCKETS 10
        PROPERTIES (
        "bloom_filter_columns"="saler_id,category_id",
        "replication_num" = "1"
        )
        """

        sql "drop table if exists nb_table"
        sql """
        -- 创建示例：表创建时指定
        CREATE TABLE `nb_table` (
          `siteid` int(11) NULL DEFAULT "10" COMMENT "",
          `citycode` smallint(6) NULL COMMENT "",
          `username` varchar(32) NULL DEFAULT "" COMMENT "",
          INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256") COMMENT 'username ngram_bf index'
        ) ENGINE=OLAP
        AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        )
        -- PROPERTIES("gram_size"="3", "bf_size"="256")，分别表示 gram 的个数和 bloom filter 的字节数。
        -- gram 的个数跟实际查询场景相关，通常设置为大部分查询字符串的长度，bloom filter 字节数，可以通过测试得出，通常越大过滤效果越好，可以从 256 开始进行验证测试看看效果。当然字节数越大也会带来索引存储、内存 cost 上升。
        -- 如果数据基数比较高，字节数可以不用设置过大，如果基数不是很高，可以通过增加字节数来提升过滤效果。
        """

        multi_sql """
        drop table if exists tbl_unique_merge_on_write;
        drop table if exists tbl_unique_merge_on_write_p;
        """
        multi_sql """
        -- 以 Unique 模型的 Merge-on-Write 表为例
        -- Unique 模型的写时合并实现，与聚合模型就是完全不同的两种模型了，查询性能更接近于 duplicate 模型，
        -- 在有主键约束需求的场景上相比聚合模型有较大的查询性能优势，尤其是在聚合查询以及需要用索引过滤大量数据的查询中。
        
        -- 非分区表
        CREATE TABLE IF NOT EXISTS tbl_unique_merge_on_write
        (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
            `register_time` DATE COMMENT "用户注册时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `phone` LARGEINT COMMENT "用户电话",
            `address` VARCHAR(500) COMMENT "用户地址"
        )
        UNIQUE KEY(`user_id`, `username`)
        -- 3-5G 的数据量
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10 
        PROPERTIES (
        -- 在 1.2.0 版本中，作为一个新的 feature，写时合并默认关闭，用户可以通过添加下面的 property 来开启
        "enable_unique_key_merge_on_write" = "true" ,
        "replication_num" = "1"
        );
        
        -- 分区表
        CREATE TABLE IF NOT EXISTS tbl_unique_merge_on_write_p
        (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
            `register_time` DATE COMMENT "用户注册时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `phone` LARGEINT COMMENT "用户电话",
            `address` VARCHAR(500) COMMENT "用户地址"
        )
        UNIQUE KEY(`user_id`, `username`, `register_time`)
        PARTITION BY RANGE(`register_time`) (
            PARTITION p00010101_1899 VALUES [('0001-01-01'), ('1900-01-01')), 
            PARTITION p19000101 VALUES [('1900-01-01'), ('1900-01-02')), 
            PARTITION p19000102 VALUES [('1900-01-02'), ('1900-01-03')),
            PARTITION p19000103 VALUES [('1900-01-03'), ('1900-01-04')),
            PARTITION p19000104_1999 VALUES [('1900-01-04'), ('2000-01-01')),
            FROM ("2000-01-01") TO ("2022-01-01") INTERVAL 1 YEAR, 
            PARTITION p30001231 VALUES [('3000-12-31'), ('3001-01-01')), 
            PARTITION p99991231 VALUES [('9999-12-31'), (MAXVALUE)) 
        ) 
        -- 默认 3-5G 的数据量
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10 
        PROPERTIES ( 
        -- 在 1.2.0 版本中，作为一个新的 feature，写时合并默认关闭，用户可以通过添加下面的 property 来开启
        "enable_unique_key_merge_on_write" = "true", 
        -- 动态分区调度的单位。可指定为 HOUR、DAY、WEEK、MONTH、YEAR。分别表示按小时、按天、按星期、按月、按年进行分区创建或删除。
        "dynamic_partition.time_unit" = "MONTH",
        -- 动态分区的起始偏移，为负数。根据 time_unit 属性的不同，以当天（星期/月）为基准，分区范围在此偏移之前的分区将会被删除（TTL）。如果不填写，则默认为 -2147483648，即不删除历史分区。
        "dynamic_partition.start" = "-3000",
        -- 动态分区的结束偏移，为正数。根据 time_unit 属性的不同，以当天（星期/月）为基准，提前创建对应范围的分区。
        "dynamic_partition.end" = "10",
        -- 动态创建的分区名前缀（必选）。
        "dynamic_partition.prefix" = "p",
        -- 动态创建的分区所对应的分桶数量。
        "dynamic_partition.buckets" = "10", 
        "dynamic_partition.enable" = "true", 
        -- 动态创建的分区所对应的副本数量，如果不填写，则默认为该表创建时指定的副本数量 3。
        "dynamic_partition.replication_num" = "1",
        "replication_num" = "1"
        );  
        
        -- 分区创建查看
        -- 实际创建的分区数需要结合 dynamic_partition.start、end 以及 PARTITION BY RANGE 的设置共同决定
        show partitions from tbl_unique_merge_on_write_p;
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/best-practice.md failed to exec, please fix it", t)
    }
}
