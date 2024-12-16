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

suite("test_external_partition") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled Hive test.")
        return;
    }

    def enabled_partition = sql """show variables like "%enable_partition_analyze%" """
    if (enabled_partition[0][1].equalsIgnoreCase("false")) {
        logger.info("partition analyze disabled. " + enabled_partition)
        return;
    }

    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String catalog_name = "${hivePrefix}_test_partitions"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""
    sql """use `${catalog_name}`.`default`"""
    sql """analyze table partition_table with sync"""
    def result = sql """show column stats partition_table;"""
    assertEquals(18, result.size())
    result = sql """show column cached stats partition_table;"""
    assertEquals(18, result.size())
    result = sql """show column stats partition_table partition(*);"""
    assertEquals(108, result.size())
    result = sql """show column cached stats partition_table partition(*);"""
    assertEquals(108, result.size())

    result = sql """show column stats partition_table(l_orderkey)"""
    assertEquals(1, result.size())
    assertEquals("9995.0", result[0][2])
    assertEquals("2506.0", result[0][3])
    assertEquals("0.0", result[0][4])
    assertEquals("1", result[0][7])
    assertEquals("10050", result[0][8])

    result = sql """show column stats partition_table(l_orderkey) partition(`nation=cn/city=beijing`);"""
    assertEquals(1, result.size())
    assertEquals("l_orderkey", result[0][0])
    assertEquals("nation=cn/city=beijing", result[0][1])
    assertEquals("partition_table", result[0][2])
    assertEquals("2003", result[0][3])
    assertEquals("501", result[0][4])
    assertEquals("0", result[0][5])
    assertEquals("1", result[0][6])
    assertEquals("1991", result[0][7])

    result = sql """show column cached stats partition_table(l_orderkey) partition(`nation=cn/city=beijing`);"""
    assertEquals(1, result.size())
    assertEquals("l_orderkey", result[0][0])
    assertEquals("nation=cn/city=beijing", result[0][1])
    assertEquals("partition_table", result[0][2])
    assertEquals("2003.0", result[0][3])
    assertEquals("534", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("1", result[0][6])
    assertEquals("1991", result[0][7])

    sql """drop stats partition_table partition(`nation=cn/city=beijing`)"""
    result = sql """show column stats partition_table  partition(`nation=cn/city=beijing`)"""
    assertEquals(0, result.size())
    result = sql """show column cached stats partition_table  partition(`nation=cn/city=beijing`)"""
    assertEquals(0, result.size())

    sql """drop stats partition_table"""
    sql """analyze table partition_table partition(`nation=cn/city=beijing`) with sync"""
    result = sql """show column stats partition_table  partition(`nation=cn/city=beijing`)"""
    assertEquals(18, result.size())
    result = sql """show column cached stats partition_table  partition(`nation=cn/city=beijing`)"""
    assertEquals(18, result.size())
    result = sql """show column stats partition_table  partition(*)"""
    assertEquals(18, result.size())
    result = sql """show column cached stats partition_table  partition(*)"""
    assertEquals(18, result.size())

    sql """drop catalog if exists ${catalog_name}"""
}

