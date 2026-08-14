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

suite("test_paimon_write_boundary",
        "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_write_boundary"
    String dbName = "paimon_write_boundary_db"
    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.write_boundary;
            create table paimon.${dbName}.write_boundary (
                id int,
                score int,
                note string
            ) using paimon tblproperties (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet'
            );
            insert into paimon.${dbName}.write_boundary values
                (1, 10, 'base-1'),
                (2, 20, 'base-2');
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        qt_before_rows """select id, score, note from write_boundary order by id"""
        qt_before_snapshots """select count(*) from write_boundary\$snapshots"""

        sql """insert into write_boundary values (3, 30, 'insert-values')"""
        order_qt_after_insert_values """select id, score, note from write_boundary"""

        sql """insert into write_boundary select 4, 40, 'insert-select'"""
        order_qt_after_insert_select """select id, score, note from write_boundary"""

        sql """insert overwrite table write_boundary values (5, 50, 'overwrite')"""
        order_qt_after_overwrite """select id, score, note from write_boundary"""

        // Row-level mutation remains an OLAP-table-only command. Paimon upserts are performed
        // through INSERT statements against primary-key tables.
        test {
            sql """update write_boundary set score = score + 1 where id = 1"""
            exception "target table in update command should be an olapTable"
        }
        test {
            sql """delete from write_boundary where id = 1"""
            exception "delete command could be only used on olap table"
        }
        test {
            sql """
                merge into write_boundary target
                using (select 1 as id, 99 as score, 'merge' as note) source
                on target.id = source.id
                when matched then update set score = source.score, note = source.note
                when not matched then insert (id, score, note)
                    values (source.id, source.score, source.note)
            """
            exception "merge into command only support MOW unique key olapTable"
        }

        sql """refresh table write_boundary"""
        qt_after_rows """select id, score, note from write_boundary order by id"""
        qt_after_snapshots """select count(*) from write_boundary\$snapshots"""
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
