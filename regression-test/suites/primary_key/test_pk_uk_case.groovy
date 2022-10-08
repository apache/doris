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

import org.codehaus.groovy.runtime.IOGroovyMethods;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.time.format.DateTimeFormatter;

suite("test_pk_uk_case") {
    def tableNamePk = "primary_key_pk_uk"
    def tableNameUk = "unique_key_pk_uk"

    onFinish {
        try_sql("DROP TABLE IF EXISTS ${tableNamePk}")
        try_sql("DROP TABLE IF EXISTS ${tableNameUk}")
    }

    sql """ DROP TABLE IF EXISTS ${tableNamePk} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableNamePk} (
        L_ORDERKEY    INTEGER NOT NULL,
        L_PARTKEY     INTEGER NOT NULL,
        L_SUPPKEY     INTEGER NOT NULL,
        L_LINENUMBER  INTEGER NOT NULL,
        L_QUANTITY    DECIMAL(15,2) NOT NULL,
        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        L_TAX         DECIMAL(15,2) NOT NULL,
        L_RETURNFLAG  CHAR(1) NOT NULL,
        L_LINESTATUS  CHAR(1) NOT NULL,
        L_SHIPDATE    DATE NOT NULL,
        L_COMMITDATE  DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT CHAR(60) NOT NULL,
        L_SHIPMODE     CHAR(60) NOT NULL,
        L_COMMENT      VARCHAR(60) NOT NULL
        )
        UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """ DROP TABLE IF EXISTS ${tableNameUk} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameUk} (
        L_ORDERKEY    INTEGER NOT NULL,
        L_PARTKEY     INTEGER NOT NULL,
        L_SUPPKEY     INTEGER NOT NULL,
        L_LINENUMBER  INTEGER NOT NULL,
        L_QUANTITY    DECIMAL(15,2) NOT NULL,
        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        L_TAX         DECIMAL(15,2) NOT NULL,
        L_RETURNFLAG  CHAR(1) NOT NULL,
        L_LINESTATUS  CHAR(1) NOT NULL,
        L_SHIPDATE    DATE NOT NULL,
        L_COMMITDATE  DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT CHAR(60) NOT NULL,
        L_SHIPMODE     CHAR(60) NOT NULL,
        L_COMMENT      VARCHAR(60) NOT NULL
        )
        UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        )       
    """

    Random rd = new Random()
    def order_key = rd.nextInt(1000)
    def part_key = rd.nextInt(1000)
    def sub_key = 13
    def line_num = 29
    def decimal = rd.nextFloat()
    def city = RandomStringUtils.randomAlphabetic(10)
    def name = UUID.randomUUID().toString()
    def date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now())
    for (int idx = 0; idx < 50; idx++) {
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num,
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num,
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, 
            $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
            """

        // insert batch key 
        order_key = rd.nextInt(10)
        part_key = rd.nextInt(10)
        decimal = rd.nextFloat()
        city = RandomStringUtils.randomAlphabetic(10)
        name = UUID.randomUUID().toString()
        sql """ INSERT INTO ${tableNamePk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
        """
        sql """ INSERT INTO ${tableNameUk} VALUES
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city'),
            ($order_key, $part_key, $sub_key, $line_num, $decimal, $decimal, $decimal, $decimal, '1', '1', '$date', '$date', '$date', '$name', '$name', '$city')
        """

        // count(*)
        result0 = sql """ SELECT count(*) FROM ${tableNamePk}; """
        result1 = sql """ SELECT count(*) FROM ${tableNameUk}; """
        logger.info("result:" + result0[0][0] + "|" + result1[0][0])
        assertTrue(result0[0]==result1[0])
        if (result0[0][0]!=result1[0][0]) {
            logger.info("result:" + result0[0][0] + "|" + result1[0][0])
        }

        result0 = sql """ SELECT
                            l_returnflag,
                            l_linestatus,
                            sum(l_quantity)                                       AS sum_qty,
                            sum(l_extendedprice)                                  AS sum_base_price,
                            sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
                            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                            avg(l_quantity)                                       AS avg_qty,
                            avg(l_extendedprice)                                  AS avg_price,
                            avg(l_discount)                                       AS avg_disc,
                            count(*)                                              AS count_order
                            FROM
                            ${tableNamePk}
                            WHERE
                            l_shipdate <= DATE '2023-01-01' - INTERVAL '90' DAY
                            GROUP BY
                            l_returnflag,
                            l_linestatus
                            ORDER BY
                            l_returnflag,
                            l_linestatus
                        """
        result1 = sql """ SELECT
                            l_returnflag,
                            l_linestatus,
                            sum(l_quantity)                                       AS sum_qty,
                            sum(l_extendedprice)                                  AS sum_base_price,
                            sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
                            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                            avg(l_quantity)                                       AS avg_qty,
                            avg(l_extendedprice)                                  AS avg_price,
                            avg(l_discount)                                       AS avg_disc,
                            count(*)                                              AS count_order
                            FROM
                            ${tableNameUk}
                            WHERE
                            l_shipdate <= DATE '2023-01-01' - INTERVAL '90' DAY
                            GROUP BY
                            l_returnflag,
                            l_linestatus
                            ORDER BY
                            l_returnflag,
                            l_linestatus
                        """  
        assertTrue(result0.size()==result1.size())
        for (int i = 0; i < result0.size(); ++i) {
            for (j = 0; j < result0[0].size(); j++) {
                logger.info("result: " + result0[i][j] + "|" + result1[i][j])
                assertTrue(result0[0]==result1[0])
            }
        }       

        // delete
        if (idx % 10 == 0) {
            order_key = rd.nextInt(10)
            part_key = rd.nextInt(10)
            result0 = sql """ SELECT count(*) FROM ${tableNamePk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key; """
            result1 = sql """ SELECT count(*) FROM ${tableNameUk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"""
            logger.info("result:" + result0[0][0] + "|" + result1[0][0])
            sql "DELETE FROM ${tableNamePk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"
            sql "DELETE FROM ${tableNameUk} where L_ORDERKEY < $order_key and L_PARTKEY < $part_key"
        }
    }
}
