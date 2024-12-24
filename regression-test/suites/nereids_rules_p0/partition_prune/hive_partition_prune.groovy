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

suite("hive_partition_prune") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
    String extHiveHmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String catalog_name = "test_external_catalog_hive_partition_prune"

    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
        );
    """
    sql """switch ${catalog_name};"""
    sql "drop database if exists test_hive_partition_prune"
    sql "create database test_hive_partition_prune"
    sql "use test_hive_partition_prune"
    sql "drop table if exists test_hive_partition "
    sql """create table test_hive_partition (a int, b int, p int)
    engine=hive partition by list(p)()
    properties("file_format"="parquet","orc.compress"="zstd");"""

    sql "insert into test_hive_partition values(1,3,1),(1,2,2),(1,2,3),(1,2,4),(1,2,5),(1,6,null)"

    explain {
        sql "select * from test_hive_partition where p=1 or p=2"
        contains("partition=2/6")
    }

    explain {
        sql "SELECT * FROM test_hive_partition WHERE p in (1,2) and p in (2,3,4)"
        contains("partition=1/6")
    }

    explain {
        sql "SELECT * FROM test_hive_partition WHERE (p=1 or p=2) and p in (3,5,2)"
        contains("partition=1/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p in (1, 5,6)"
        contains("partition=2/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p not in (15,6,1, '2021-01-02 00:00:00')"
        contains("partition=4/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p not in (1, 5,6,null)"
        contains("partition=0/6")
    }
    explain {
        sql "select * from test_hive_partition where !(p is not null)"
        contains("partition=1/6")
    }
    explain {
        sql "select * from test_hive_partition where p is null"
        contains("partition=1/6")
    }
    explain {
        sql "select * from test_hive_partition where p is not null"
        contains("partition=5/6")
    }
    explain {
        sql "select * from test_hive_partition where not p is null"
        contains("partition=5/6")
    }
    explain {
        sql "select * from test_hive_partition where !(p is null)"
        contains("partition=5/6")
    }

    explain {
        sql "select * from test_hive_partition where p <=> null"
        contains("partition=1/6")
    }
    explain {
        sql "select * from test_hive_partition where !(p <=> null)"
        contains("partition=5/6")
    }

    explain {
        sql "SELECT * FROM test_hive_partition WHERE p<1"
        contains("partition=0/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p<=2"
        contains("partition=2/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p>2"
        contains("partition=3/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p>=3"
        contains("partition=3/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p=4"
        contains("partition=1/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p<=>5"
        contains("partition=1/6")
    }
    explain {
        sql "SELECT * FROM test_hive_partition WHERE p!=5 and p!=6"
        contains("partition=4/6")
    }
    sql "drop table if exists test_hive_partition "
}