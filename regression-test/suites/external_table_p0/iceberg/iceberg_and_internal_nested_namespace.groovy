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

suite("iceberg_and_internal_nested_namespace", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_nested_namespace"

    sql """drop catalog if exists ${catalog_name}"""
    // 1.
    // iceberg.rest.nested-namespace-enabled = false
    // set global enable_nested_namespace = false
    sql """set global enable_nested_namespace=false"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1",
        "iceberg.rest.nested-namespace-enabled" = "false"
    );"""

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    
    // there is already a nested namespace "nested.db1", but can only see "nested"
    sql """show tables from `nested`;"""
    test {
        sql """show tables from `nested.db1`;"""
        exception """Unknown database 'nested.db1'"""
    }
    test {
        sql """drop database `nested.db1`;"""
        exception """Can't drop database 'nested.db1'; database doesn't exist"""
    }
    test {
        sql """select * from `nested.db1`.tbl1;"""
        exception """Database [nested.db1] does not exist"""
    }
    // can not create nested ns
    test {
        sql """create database `ns1.ns2`"""
        exception """Incorrect database name 'ns1.ns2'"""
    }

    // 2.
    // iceberg.rest.nested-namespace-enabled = true
    // set global enable_nested_namespace = false
    sql """set global enable_nested_namespace = false"""
    sql """alter catalog ${catalog_name} set properties("iceberg.rest.nested-namespace-enabled" = "true");"""
    sql """switch ${catalog_name}"""
    // can see the nested ns, with back quote
    sql """show tables from `nested`;"""
    sql """show tables from `nested.db1`;"""
    test {
        sql """show tables from nested.db1"""
        exception """Unknown catalog 'nested'"""
    }
    // for "use" stmt, back quote is not necessary
    // sql """use ${catalog_name}.nested.db1"""
    // sql """use ${catalog_name}.`nested.db1`"""
    // can not create nested ns
    test {
        sql """create database `ns1.ns2`"""
        exception """Incorrect database name 'ns1.ns2'"""
    }

    // 3.
    // iceberg.rest.nested-namespace-enabled = true
    // set global enable_nested_namespace = true
    sql """set global enable_nested_namespace = true"""
    sql """alter catalog ${catalog_name} set properties("iceberg.rest.nested-namespace-enabled" = "true");"""
    sql """switch ${catalog_name}"""
    // can see the nested ns, with back quote
    sql """show tables from `nested`;"""
    sql """show tables from `nested.db1`;"""
    test {
        sql """show tables from nested.db1"""
        exception """Unknown catalog 'nested'"""
    }
    // for "use" stmt, back quote is not necessary
    sql """use ${catalog_name}.nested;"""
    sql """use ${catalog_name}.`nested.db1`"""
    // drop and create nested db1
    sql """drop database if exists `ns1.ns2.ns3` force"""
    sql """drop database if exists `ns1.ns2` force"""
    sql """drop database if exists `ns1` force"""
    qt_sql01 """show databases like 'ns1.ns2.ns3'""" // empty
    sql """refresh catalog ${catalog_name}"""
    qt_sql02 """show databases like 'ns1.ns2.ns3'""" // empty

    sql """create database `ns1.ns2.ns3`"""
    // will see 3 ns, flat
    qt_sql03 """show databases like 'ns1'""" // 1
    qt_sql04 """show databases like 'ns1.ns2'""" // 1
    qt_sql05 """show databases like 'ns1.ns2.ns3'""" // 1
    // can create database in each ns
    sql """create table ns1.nested_tbl1 (k1 int)""";
    sql """insert into ns1.nested_tbl1 values(101)"""
    qt_sql06 """select * from ns1.nested_tbl1"""

    sql """create table `ns1.ns2`.nested_tbl2 (k1 int)""";
    sql """insert into `ns1.ns2`.nested_tbl2 values(102)"""
    qt_sql07 """select * from `ns1.ns2`.nested_tbl2"""

    sql """use ${catalog_name}.`ns1.ns2.ns3`"""
    sql """create table nested_tbl3 (k1 int)""";
    sql """insert into nested_tbl3 values(103)"""
    qt_sql08 """select * from nested_tbl3"""

    // test select column in diff qualified names
    qt_sql09 """select ${catalog_name}.ns1.nested_tbl1.k1 from ${catalog_name}.ns1.nested_tbl1"""
    qt_sql10 """select ${catalog_name}.`ns1.ns2`.nested_tbl2.k1 from ${catalog_name}.`ns1.ns2`.nested_tbl2"""
    sql """use ${catalog_name}.`ns1.ns2`"""
    order_qt_sql11 """select ${catalog_name}.ns1.nested_tbl1.k1 from ${catalog_name}.ns1.nested_tbl1
                      union all
                      select k1 from nested_tbl2
                      union all
                      select `ns1.ns2.ns3`.nested_tbl3.k1 from `ns1.ns2.ns3`.nested_tbl3;
                   """
    // test table exist in each ns
    qt_sql12 """show tables from ns1""";
    qt_sql13 """show tables from `ns1.ns2`""";
    qt_sql14 """show tables from `ns1.ns2.ns3`""";
    test {
        sql """drop database ns1"""
        exception """Namespace ns1 is not empty. 1 tables exist"""
    }
    test {
        sql """drop database `ns1.ns2`"""
        exception """Namespace ns1.ns2 is not empty. 1 tables exist"""
    }
    test {
        sql """drop database ${catalog_name}.`ns1.ns2.ns3`"""
        exception """Namespace ns1.ns2.ns3 is not empty. 1 tables exist"""
    }
    // test refresh database and table
    sql """refresh database ${catalog_name}.`ns1.ns2`"""
    sql """refresh database `ns1.ns2`"""
    sql """refresh table ${catalog_name}.`ns1.ns2`.nested_tbl2"""
    sql """refresh table `ns1.ns2`.nested_tbl2"""
    test {
        sql """refresh table `ns1.ns2`.nested_tbl2xxx"""
        exception """Table nested_tbl2xxx does not exist in db ns1.ns2"""
    }
    // drop ns1.ns2 first, we can still see it after refresh, because ns1.ns2.ns3 still exists
    sql """drop database `ns1.ns2` force"""
    qt_sql15 """show databases like "ns1.ns2"""" // empty
    sql """refresh catalog ${catalog_name}"""
    qt_sql16 """show databases like "ns1.ns2"""" // 1
    // then we drop ns1.ns2.ns3, after refresh, ns1.ns2 also disappear
    sql """drop database `ns1.ns2.ns3` force"""
    qt_sql17 """show databases like "ns1.ns2"""" // 1
    qt_sql18 """show databases like "ns1.ns2.ns3"""" // empty
    sql """refresh catalog ${catalog_name}"""
    qt_sql19 """show databases like "ns1.ns2"""" // empty
    qt_sql20 """show databases like "ns1.ns2.ns3"""" // empty

    // recreate ns1.ns2.ns3
    sql """create database `ns1.ns2.ns3`;"""
    qt_sql21 """show databases like "ns1.ns2"""" // 1
    qt_sql22 """show databases like "ns1.ns2.ns3"""" // 1
    // drop ns1.ns2.ns3, and ns1.ns2 will disappear too
    sql """drop database `ns1.ns2.ns3`"""
    sql """refresh catalog ${catalog_name}"""
    qt_sql23 """show databases like "ns1.ns2"""" // empty
    qt_sql24 """show databases like "ns1.ns2.ns3"""" // empty

    // recreate ns1.ns2.ns3, and create table in ns1.ns2
    sql """create database `ns1.ns2.ns3`;"""
    qt_sql25 """show databases like "ns1.ns2"""" // 1
    qt_sql26 """show databases like "ns1.ns2.ns3"""" // 1
    sql """create table `ns1.ns2`.test_table2(k1 int);"""
    sql """insert into `ns1.ns2`.test_table2 values(104)"""
    qt_sql261 """select * from `ns1.ns2`.test_table2"""
    // drop ns1.ns2.ns3, ns1.ns2 will still exist
    sql """drop database `ns1.ns2.ns3`"""
    sql """refresh catalog ${catalog_name}"""
    qt_sql27 """show databases like "ns1.ns2"""" // 1
    qt_sql28 """show databases like "ns1.ns2.ns3"""" // empty
    qt_sql29 """select * from `ns1.ns2`.test_table2"""
    // drop `ns1.ns2`.test_table2, and then ns1.ns2 will disappeal
    sql """drop table `ns1.ns2`.test_table2"""
    sql """refresh catalog ${catalog_name}"""
    qt_sql30 """show databases like "ns1.ns2"""" // empty

    // test dropping and creating table in nested ns spark created
    sql """drop table if exists `nested.db1`.spark_table"""
    sql """create table `nested.db1`.spark_table (k1 int)"""
    sql """insert into `nested.db1`.spark_table values(105)"""
    qt_sql31 """select * from `nested.db1`.spark_table"""


    // 4.
    // iceberg.rest.nested-namespace-enabled = false
    // set global enable_nested_namespace = true
    sql """set global enable_nested_namespace = true"""
    sql """alter catalog ${catalog_name} set properties("iceberg.rest.nested-namespace-enabled" = "false");"""
    sql """switch ${catalog_name}"""
    // can not see the nested ns
    qt_sql32 """show databases like "nested.db1";""" // empty
    test {
        sql """use ${catalog_name}.`nested.db1`"""
        exception """Unknown database 'nested.db1'"""
    }

    // can create nested ns, but can not drop because nested ns can not be seen
    test {
        sql """drop database `nested.db1`"""
        exception """Can't drop database 'nested.db1'; database doesn't exist"""
    }
    sql """create database if not exists `nsa.nsb`"""
    sql """create database if not exists `nsa.nsb.nsc`"""
    // can only see nsa
    qt_sql33 """show databases like "nsa"""" // 1
    qt_sql34 """show databases like "nsa.nsb"""" // empty
    // can create and drop table in nsa
    sql """drop table if exists nsa.nsa_tbl1"""
    sql """create table nsa.nsa_tbl1 (k1 int);"""
    sql """insert into nsa.nsa_tbl1 values(106)"""
    qt_sql35 """select * from nsa.nsa_tbl1"""
    sql """drop table nsa.nsa_tbl1"""
    test {
        sql """select * from nsa.nsa_tbl1"""
        exception """Table [nsa_tbl1] does not exist in database [nsa]"""
    }

    // 5. test internal
    sql """switch internal"""
    sql """set global enable_nested_namespace = true"""
    // create nested ns
    sql """drop database if exists `idb1.idb2.idb3`"""
    sql """drop database if exists `idb1.idb2`"""
    sql """drop database if exists `idb1`"""
    sql """create database `idb1`"""
    sql """create database `idb1.idb2`"""
    sql """create database `idb1.idb2.idb3`"""
    qt_sql1001 """show databases like "idb1""""
    qt_sql1002 """show databases like "idb1.idb2""""
    qt_sql1003 """show databases like "idb1.idb2.idb3""""

    // create table
    sql """create table idb1.itbl1 (k1 int) properties("replication_num" = "1")"""
    sql """create table `idb1.idb2`.itbl2 (k1 int) properties("replication_num" = "1")"""
    sql """use internal.`idb1.idb2.idb3`"""
    sql """create table itbl3 (k1 int) properties("replication_num" = "1")"""

    // insert
    sql """insert into idb1.itbl1 values(201)"""
    sql """insert into `idb1.idb2`.itbl2 values(202)"""
    sql """use internal.`idb1.idb2.idb3`"""
    sql """insert into itbl3 values(203)"""

    // query
    qt_sql101 """select * from idb1.itbl1"""
    qt_sql103 """select `idb1.idb2`.itbl2.k1 from `idb1.idb2`.itbl2"""
    sql """use internal.`idb1.idb2.idb3`"""
    order_qt_sql104 """select `idb1.idb2`.itbl2.k1 from `idb1.idb2`.itbl2
                       union all
                       select idb1.itbl1.k1 from idb1.itbl1
                       union all
                       select itbl3.k1 from itbl3
                    """
    // disable
    sql """set global enable_nested_namespace = false"""
    // still can see nested ns
    qt_sql2001 """show databases like "idb1""""
    qt_sql2002 """show databases like "idb1.idb2""""
    qt_sql2003 """show databases like "idb1.idb2.idb3""""

    sql """ unset global variable enable_nested_namespace;"""
}
