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

suite("test_trino_hive_parquet", "p0,external,hive,external_docker,external_docker_hive") {

    def q01 = {
        qt_q01 """
        select * from partition_table order by l_orderkey, l_partkey, l_suppkey;
    """
    }

    def q02 = {
        qt_q02 """
        select count(*) from partition_table;
    """
    }

    def q03 = {
        qt_q03 """
        select count(city) from partition_table;
    """
    }

    def q04 = {
        qt_q04 """
        select count(nation) from partition_table;
    """
    }

    def q05 = {
        qt_q05 """
        select distinct city from partition_table order by city;
    """
    }

    def q06 = {
        qt_q06 """
        select distinct nation from partition_table order by nation;
    """
    }

    def q07 = {
        qt_q07 """
        select city from partition_table order by city limit 10;
    """
    }

    def q08 = {
        qt_q08 """
        select nation from partition_table order by nation limit 10;
    """
    }

    def q09 = {
        qt_q09 """
        select city, nation from partition_table order by nation, city limit 10;
    """
    }

    def q10 = {
        qt_q10 """
        select nation, city, count(*) from partition_table group by nation, city order by nation, city;
    """
    }

    def q11 = {
        qt_q11 """
        select city, count(*) from partition_table group by city order by city;
    """
    }

    def q12 = {
        qt_q12 """
        select nation, count(*) from partition_table group by nation order by nation;
    """
    }

    def q13 = {
        qt_q13 """
        select city, l_orderkey, nation from partition_table order by l_orderkey limit 10;
    """
    }

    def q14 = {
        qt_q14 """
        select l_orderkey from partition_table order by l_orderkey limit 10;
    """
    }

    def q15 = {
        qt_q15 """
        select count(l_orderkey) from partition_table;
    """
    }

    def q16 = {
        qt_q16 """
        select count(*) from partition_table where city='beijing';
    """
    }

    def q17 = {
        qt_q17 """
        select count(*) from partition_table where nation='cn';
    """
    }

    def q18 = {
        qt_q18 """
        select count(l_orderkey) from partition_table where nation != 'cn' and l_quantity > 28;
    """
    }

    def q19 = {
        qt_q19 """
        select l_partkey from partition_table
        where (nation != 'cn' or city !='beijing') and (l_quantity > 28 or l_extendedprice > 30000)
        order by l_partkey limit 10;
    """
    }

    def q20 = {
        qt_q20 """
        select nation, city, count(l_linenumber) from partition_table
        where city != 'beijing' or l_quantity > 28 group by nation, city order by nation, city;
    """
    }

    def q21 = {
        qt_q21_max """
        select max(decimal_col) from parquet_decimal90_table;
        """
        qt_q21_min """
        select min(decimal_col) from parquet_decimal90_table;
        """
        qt_q21_sum """
        select sum(decimal_col) from parquet_decimal90_table;
        """
        qt_q21_avg """
        select avg(decimal_col) from parquet_decimal90_table;
        """
    }

    def q22 = {
        qt_q22_max """
        select max(decimal_col1), max(decimal_col2), max(decimal_col3), max(decimal_col4), max(decimal_col5) from fixed_length_byte_array_decimal_table;
        """
        qt_q22_min """
        select min(decimal_col1), min(decimal_col2), min(decimal_col3), min(decimal_col4), min(decimal_col5) from fixed_length_byte_array_decimal_table;
        """
        qt_q22_sum """
        select sum(decimal_col1), sum(decimal_col2), sum(decimal_col3), sum(decimal_col4), sum(decimal_col5) from fixed_length_byte_array_decimal_table;
        """
        qt_q22_avg """
        select avg(decimal_col1), avg(decimal_col2), avg(decimal_col3), avg(decimal_col4), avg(decimal_col5) from fixed_length_byte_array_decimal_table;
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def host_ips = new ArrayList()
        String[][] backends = sql """ show backends """
        for (def b in backends) {
            host_ips.add(b[1])
        }
        String [][] frontends = sql """ show frontends """
        for (def f in frontends) {
            host_ips.add(f[1])
        }
        dispatchTrinoConnectors(host_ips.unique())
        try {
            String hms_port = context.config.otherConfigs.get("hive2HmsPort")
            String catalog_name = "test_trino_hive_parquet"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """
                create catalog if not exists ${catalog_name} properties (
                    "type"="trino-connector",
                    "trino.connector.name"="hive",
                    'trino.hive.metastore.uri' = 'thrift://${externalEnvIp}:${hms_port}'
                );
            """

            sql """use `${catalog_name}`.`default`"""

            sql """set enable_fallback_to_original_planner=false;"""

            q01()
            q02()
            q03()
            q04()
            q05()
            q06()
            q07()
            q08()
            q09()
            q10()
            q11()
            q12()
            q13()
            q14()
            q15()
            q16()
            q17()
            q18()
            q19()
            q20()
            q21()
            q22()

            sql """explain physical plan select l_partkey from partition_table
                where (nation != 'cn' or city !='beijing') and (l_quantity > 28 or l_extendedprice > 30000)
                order by l_partkey limit 10;"""

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
