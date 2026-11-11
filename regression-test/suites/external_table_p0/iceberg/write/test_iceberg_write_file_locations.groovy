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

// Every file Doris writes into an Iceberg table must be recorded under the table's own location, not
// under the address BE wrote it at. The REST fixture's warehouse is s3a://warehouse/wh/, which FE
// normalizes to s3:// for BE, so a writer that records the s3:// address is caught here: delete files
// and deletion vectors were once committed that way. Each write operation Doris supports runs below;
// a new one belongs here too.
suite("test_iceberg_write_file_locations", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_file_locations"
    String dbName = "iceberg_write_file_locations_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    // The table's data location, taken from its metadata rather than from any file Doris wrote.
    def dataLocationOf = { String table ->
        String ddl = sql("""show create table ${table}""")[0][1]
        def matcher = ddl =~ /LOCATION '([^']+)'/
        assertTrue(matcher.find(), "no LOCATION in ${ddl}")
        return matcher.group(1) + "/data/"
    }

    // Every file of every snapshot, branches included: data files, position deletes and deletion vectors.
    def assertFilesUnderDataLocation = { String table, String operation ->
        String dataLocation = dataLocationOf(table)
        def files = sql """select content, file_format, file_path from ${table}\$all_files"""
        assertTrue(files.size() > 0, "no files in ${table} after ${operation}")
        for (def file : files) {
            String path = file[2].toString()
            assertTrue(path.startsWith(dataLocation),
                    "${operation} recorded ${file[1]} file ${path} (content ${file[0]}) outside ${dataLocation}")
        }
    }

    def countFiles = { String table, String predicate ->
        return (sql """select count(*) from ${table}\$all_files where ${predicate}""")[0][0] as long
    }

    // Format v2: data files and position delete files.
    sql """drop table if exists file_locations_v2"""
    sql """
        create table file_locations_v2 (
            id int,
            region string,
            payload string
        )
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """insert into file_locations_v2 values (1, 'A', 'a1'), (2, 'B', 'b1'), (3, 'C', 'c1')"""
    assertFilesUnderDataLocation("file_locations_v2", "INSERT")
    sql """insert into file_locations_v2 values (4, 'A', 'a2'), (5, 'B', 'b2')"""
    assertFilesUnderDataLocation("file_locations_v2", "second INSERT")
    sql """insert overwrite table file_locations_v2 partition (region = 'C') values (6, 'c2')"""
    assertFilesUnderDataLocation("file_locations_v2", "INSERT OVERWRITE PARTITION")
    sql """alter table file_locations_v2 create branch location_branch"""
    sql """insert into file_locations_v2@branch(location_branch) values (7, 'A', 'branch')"""
    assertFilesUnderDataLocation("file_locations_v2", "branch INSERT")
    sql """delete from file_locations_v2 where id = 1"""
    assertFilesUnderDataLocation("file_locations_v2", "DELETE")
    sql """update file_locations_v2 set payload = 'b1-updated' where id = 2"""
    assertFilesUnderDataLocation("file_locations_v2", "UPDATE")
    sql """
        merge into file_locations_v2 t
        using (
            select 4 as id, 'A' as region, 'a2-merged' as payload
            union all
            select 8, 'D', 'd1'
        ) s
        on t.id = s.id
        when matched then update set payload = s.payload
        when not matched then insert (id, region, payload) values (s.id, s.region, s.payload)
    """
    assertFilesUnderDataLocation("file_locations_v2", "MERGE")
    sql """
        alter table ${catalogName}.${dbName}.file_locations_v2
        execute rewrite_data_files("target-file-size-bytes" = "10485760", "min-input-files" = "2")
    """
    assertFilesUnderDataLocation("file_locations_v2", "rewrite_data_files")
    assertTrue(countFiles("file_locations_v2", "content = 1") > 0, "DELETE / UPDATE / MERGE wrote no delete files")

    // Format v3: row-level operations write deletion vectors into Puffin files.
    sql """drop table if exists file_locations_v3"""
    sql """
        create table file_locations_v3 (
            id int,
            region string,
            payload string
        )
        partition by list (region) ()
        properties (
            "format-version" = "3",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """insert into file_locations_v3 values (1, 'A', 'a1'), (2, 'B', 'b1'), (3, 'C', 'c1')"""
    assertFilesUnderDataLocation("file_locations_v3", "INSERT")
    sql """delete from file_locations_v3 where id = 1"""
    assertFilesUnderDataLocation("file_locations_v3", "DELETE")
    sql """update file_locations_v3 set payload = 'b1-updated' where id = 2"""
    assertFilesUnderDataLocation("file_locations_v3", "UPDATE")
    sql """
        merge into file_locations_v3 t
        using (
            select 3 as id, 'C' as region, 'c1-merged' as payload
            union all
            select 4, 'D', 'd1'
        ) s
        on t.id = s.id
        when matched then update set payload = s.payload
        when not matched then insert (id, region, payload) values (s.id, s.region, s.payload)
    """
    assertFilesUnderDataLocation("file_locations_v3", "MERGE")
    assertTrue(countFiles("file_locations_v3", "upper(file_format) = 'PUFFIN'") > 0,
            "DELETE / UPDATE / MERGE wrote no deletion vectors")
}
