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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite("test_hive_ha_catalog_validation", "p0,external") {
    sql "drop catalog if exists test_hive_ha_catalog_validation"

    test {
        sql """create catalog test_hive_ha_catalog_validation properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
            'test_connection' = 'false',
            'dfs.nameservices' = 'ns1'
        )"""
        exception "Missing property: dfs.ha.namenodes.ns1"
    }

    test {
        sql """create catalog test_hive_ha_catalog_validation properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
            'test_connection' = 'false',
            'dfs.nameservices' = ','
        )"""
        exception "dfs.nameservices must contain a nameservice"
    }

    test {
        sql """create catalog test_hive_ha_catalog_validation properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
            'test_connection' = 'false',
            'dfs.nameservices' = 'ns1,'
        )"""
        exception "dfs.nameservices must not contain empty nameservice"
    }

    test {
        sql """create catalog test_hive_ha_catalog_validation properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
            'test_connection' = 'false',
            'dfs.nameservices' = 'ns1',
            'dfs.ha.namenodes.ns1' = 'nn1,nn2'
        )"""
        exception "Missing property: dfs.namenode.rpc-address.ns1.nn1"
    }

    test {
        sql """create catalog test_hive_ha_catalog_validation properties (
            'type' = 'hms',
            'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
            'test_connection' = 'false',
            'dfs.nameservices' = 'ns1',
            'dfs.ha.namenodes.ns1' = 'nn1,nn2',
            'dfs.namenode.rpc-address.ns1.nn1' = '127.0.0.1:8020',
            'dfs.namenode.rpc-address.ns1.nn2' = '127.0.0.1:8021'
        )"""
        exception "Missing property: dfs.client.failover.proxy.provider.ns1"
    }

    sql """create catalog test_hive_ha_catalog_validation properties (
        'type' = 'hms',
        'hive.metastore.uris' = 'thrift://127.0.0.1:9083',
        'test_connection' = 'false',
        'dfs.nameservices' = 'ns1',
        'dfs.ha.namenodes.ns1' = 'nn1,nn2',
        'dfs.namenode.rpc-address.ns1.nn1' = '127.0.0.1:8020',
        'dfs.namenode.rpc-address.ns1.nn2' = '127.0.0.1:8021',
        'dfs.client.failover.proxy.provider.ns1' = 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
    )"""

    test {
        sql """alter catalog test_hive_ha_catalog_validation set properties (
            'dfs.nameservices' = 'ns2'
        )"""
        exception "Missing property: dfs.ha.namenodes.ns2"
    }
}
