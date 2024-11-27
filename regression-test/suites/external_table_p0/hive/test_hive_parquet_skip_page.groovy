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

suite("test_hive_parquet_skip_page", "p0,external,hive,external_docker,external_docker_hive") {
    def q01 = {
        qt_q01 """
        select * from lineitem where l_orderkey  < 1000 order by l_orderkey,l_partkey limit 10;
    """
    }

    def q02 = {
        qt_q02 """
        select * from lineitem where l_orderkey > 5999000 order by l_orderkey,l_partkey limit 10;
    """
    }

    def q03 = {
        qt_q03 """
        select * from lineitem where l_orderkey > 2000000 and l_orderkey < 2001000  order by l_orderkey,l_partkey limit 10;
    """
    }

    def q04 = {
        qt_q04 """
        select * from customer where c_custkey < 10000 order by c_custkey limit 10;
    """
    }

    def q05 = {
        qt_q05 """
        select * from customer where c_custkey > 140000 order by c_custkey limit 10;
    """
    }

    def q06 = {
        qt_q06 """
        select * from customer where c_custkey > 100000 and c_custkey < 110000  order by c_custkey limit 10;
    """
    }

    def q07 = {
        qt_q07 """
        select * from orders where o_orderkey < 10000 order by o_orderkey limit 10;
    """
    }

    def q08 = {
        qt_q08 """
        select * from orders where o_orderkey > 5990000 order by o_orderkey limit 10;
    """
    }

    def q09 = {
        qt_q09 """
        select * from orders where o_orderkey > 2000000 and o_orderkey < 2010000 order by o_orderkey limit 10;
    """
    }

    def q10 = {
        qt_q10 """
        select * from part where p_partkey < 10000 order by p_partkey limit 10;
    """
    }

    def q11 = {
        qt_q08 """
        select * from part where p_partkey > 190000 order by p_partkey limit 10;
    """
    }

    def q12 = {
        qt_q12 """
        select * from part where p_partkey > 100000 and p_partkey < 110000 order by p_partkey limit 10;
    """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_parquet"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            sql """switch ${catalog_name}"""
            sql """use `tpch1_parquet`"""

            sql """set enable_profile=true;"""

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

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
