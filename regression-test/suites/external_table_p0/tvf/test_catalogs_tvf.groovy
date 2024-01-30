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

suite("test_catalogs_tvf","p0,external,tvf,external_docker") {
    List<List<Object>> table =  sql """ select * from catalogs(); """
    assertTrue(table.size() > 0)
    assertEquals(5, table[0].size)

    
    table = sql """ select CatalogId,CatalogName from catalogs();"""
    assertTrue(table.size() > 0)
    assertTrue(table[0].size == 2)


    table = sql """ select * from catalogs() where CatalogId=0;"""
    assertTrue(table.size() > 0)
    assertEquals("NULL", table[0][3])
    assertEquals("NULL", table[0][4])


    def res = sql """ select count(*) from catalogs(); """
    assertTrue(res[0][0] > 0)

    res = sql """ select * from catalogs() order by CatalogId; """
    assertTrue(res[0][0] == 0)
    assertEquals(res[0][1],"internal") 
    assertEquals(res[0][2],"internal") 

    sql """ drop catalog if exists catalog_test_hive00 """ 
    sql """ drop catalog if exists catalog_test_es00 """
    
    sql """ CREATE CATALOG catalog_test_hive00 PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://127.0.0.1:7004',
    'dfs.nameservices'='HANN',
    'dfs.ha.namenodes.HANN'='nn1,nn2',
    'dfs.namenode.rpc-address.HANN.nn1'='nn1_host:rpc_port',
    'dfs.namenode.rpc-address.HANN.nn2'='nn2_host:rpc_port',
    'dfs.client.failover.proxy.provider.HANN'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
    ) """

    sql """ CREATE CATALOG catalog_test_es00 PROPERTIES (
    "type"="es",
    "hosts"="http://127.0.0.1:9200"
    ) """ 
    
    
    qt_create """ select CatalogName,CatalogType,Property,Value from catalogs() where CatalogName in ("catalog_test_es00","catalog_test_hive00") and Property="type" order by Value"""

    sql """ drop catalog catalog_test_hive00 """ 

    qt_delete """ select CatalogName,CatalogType,Property,Value from catalogs() where CatalogName="catalog_test_hive00" """

    qt_create """ select CatalogName,CatalogType,Property,Value from catalogs() where CatalogName in ("catalog_test_es00","catalog_test_hive00") and Property="type" order by Value"""
    
    sql """ drop catalog catalog_test_es00 """

    // test exception
    test {
        sql """ select * from catalogs("Host" = "127.0.0.1"); """

        // check exception
        exception "catalogs table-valued-function does not support any params"
    }
}
